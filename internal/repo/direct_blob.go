package repo

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	storagev1 "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/spill"
	"gyit/internal/store"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Direct blob pages group each object's size and payload descriptors so reads
// do not need to search the main object index for every content chunk.
const directBlobPageBytes = 64 << 10

type directBlobStage struct{ records *spill.Sorter }

func newDirectBlobStage(tmp string) (*directBlobStage, error) {
	s, err := spill.New(tmp, 8<<20)
	return &directBlobStage{records: s}, err
}

// add is called under stage.mu. Blob identities remain in the main index;
// payload descriptors leave it and join their sizes in this separate sort.
func (d *directBlobStage) add(key string, value any) (bool, error) {
	var oid string
	var part uint64
	r := &storagev1.DirectBlobStaged{}
	switch v := value.(type) {
	case object:
		if !strings.HasPrefix(key, "o/") || v.Kind != "blob" {
			return false, nil
		}
		if v.Size < 0 {
			return false, fmt.Errorf("negative staged blob size")
		}
		oid, r.SizeRecord, r.Size = key[2:], true, uint64(v.Size)
	case chunk:
		if !strings.HasPrefix(key, "b/") {
			return false, nil
		}
		fields := strings.Split(key, "/")
		if len(fields) != 3 || len(fields[2]) != 16 {
			return false, fmt.Errorf("invalid staged blob chunk key")
		}
		var err error
		part, err = strconv.ParseUint(fields[2], 16, 64)
		if err != nil {
			return false, err
		}
		oid = fields[1]
		b, err := marshal(v)
		if err != nil {
			return false, err
		}
		r.Chunk = &storagev1.ChunkRecord{}
		if err := proto.Unmarshal(b, r.Chunk); err != nil {
			return false, err
		}
	default:
		return false, nil
	}
	id, err := hex.DecodeString(oid)
	if err != nil || (len(id) != 20 && len(id) != 32) {
		return false, fmt.Errorf("invalid staged blob OID")
	}
	k := append(id, 0)
	if !r.SizeRecord {
		k[len(k)-1] = 1
		k = binary.BigEndian.AppendUint64(k, part)
	}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(r)
	if err == nil {
		err = d.records.Add(k, b)
	}
	return !r.SizeRecord, err
}

func directKeyCompare(a []byte, ap uint64, b []byte, bp uint64) int {
	if c := bytes.Compare(a, b); c != 0 {
		return c
	}
	if ap < bp {
		return -1
	}
	if ap > bp {
		return 1
	}
	return 0
}

func directWireSize(v proto.Message) int {
	n := proto.Size(v)
	return 1 + protowire.SizeBytes(n)
}

type directBlobBuilder struct {
	w          *indexWriter
	leaf       []*storagev1.DirectBlobPart
	leafBytes  int
	levels     [][]*storagev1.DirectBlobChild
	levelBytes []int
}

func (b *directBlobBuilder) save(p *storagev1.DirectBlobPage) (*storagev1.DirectBlobChild, error) {
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(p)
	if err != nil {
		return nil, err
	}
	if len(raw) > directBlobPageBytes {
		return nil, fmt.Errorf("direct blob page exceeds byte limit")
	}
	ref, err := b.w.saveBytes(raw)
	if err != nil {
		return nil, err
	}
	c := &storagev1.DirectBlobChild{Page: encodePageRef(ref)}
	if len(p.Items) > 0 {
		c.MinOid, c.MinPart = bytes.Clone(p.Items[0].Oid), p.Items[0].Part
	} else if len(p.Children) > 0 {
		c.MinOid, c.MinPart = bytes.Clone(p.Children[0].MinOid), p.Children[0].MinPart
	} else {
		return nil, fmt.Errorf("empty direct blob page")
	}
	return c, nil
}

func (b *directBlobBuilder) addChild(level int, c *storagev1.DirectBlobChild) error {
	if level >= 16 {
		return fmt.Errorf("direct blob index exceeds depth limit")
	}
	for len(b.levels) <= level {
		b.levels = append(b.levels, nil)
		b.levelBytes = append(b.levelBytes, 0)
	}
	n := directWireSize(c)
	if n > directBlobPageBytes {
		return fmt.Errorf("direct blob child exceeds byte limit")
	}
	if len(b.levels[level]) == fanout || b.levelBytes[level]+n > directBlobPageBytes {
		p, err := b.save(&storagev1.DirectBlobPage{Children: b.levels[level]})
		if err != nil {
			return err
		}
		b.levels[level], b.levelBytes[level] = nil, 0
		if err := b.addChild(level+1, p); err != nil {
			return err
		}
	}
	b.levels[level] = append(b.levels[level], c)
	b.levelBytes[level] += n
	return nil
}

func (b *directBlobBuilder) flushLeaf() error {
	if len(b.leaf) == 0 {
		return nil
	}
	p, err := b.save(&storagev1.DirectBlobPage{Items: b.leaf})
	if err != nil {
		return err
	}
	b.leaf, b.leafBytes = nil, 0
	return b.addChild(0, p)
}

func (b *directBlobBuilder) add(r *storagev1.DirectBlobPart) error {
	n := directWireSize(r)
	if n > directBlobPageBytes {
		return fmt.Errorf("direct blob record exceeds byte limit")
	}
	if len(b.leaf) == fanout || b.leafBytes+n > directBlobPageBytes {
		if err := b.flushLeaf(); err != nil {
			return err
		}
	}
	b.leaf = append(b.leaf, r)
	b.leafBytes += n
	return nil
}

func (b *directBlobBuilder) finish() (pageRef, error) {
	if err := b.flushLeaf(); err != nil {
		return pageRef{}, err
	}
	for level := 0; level < len(b.levels); level++ {
		if len(b.levels[level]) == 0 {
			continue
		}
		if level == len(b.levels)-1 && len(b.levels[level]) == 1 {
			if err := b.w.flush(); err != nil {
				return pageRef{}, err
			}
			return decodePageRef(b.levels[level][0].Page), nil
		}
		c, err := b.save(&storagev1.DirectBlobPage{Children: b.levels[level]})
		if err != nil {
			return pageRef{}, err
		}
		b.levels[level], b.levelBytes[level] = nil, 0
		if err := b.addChild(level+1, c); err != nil {
			return pageRef{}, err
		}
	}
	return pageRef{}, nil
}

func (d *directBlobStage) build(ctx context.Context, backend store.Store) (pageRef, error) {
	b := &directBlobBuilder{w: &indexWriter{ctx: ctx, store: backend, prefix: "blobs-" + rand.Text()}}
	if err := d.walkParts(ctx, b.add); err != nil {
		return pageRef{}, err
	}
	return b.finish()
}

func (d *directBlobStage) walkParts(ctx context.Context, emit func(*storagev1.DirectBlobPart) error) error {
	var oid []byte
	var size, next uint64
	finish := func() error {
		if oid == nil {
			return nil
		}
		if size == 0 {
			if next != 0 {
				return fmt.Errorf("empty blob has payload")
			}
			return emit(&storagev1.DirectBlobPart{Oid: bytes.Clone(oid)})
		}
		if next != (size-1)/ChunkSize+1 {
			return fmt.Errorf("incomplete staged blob")
		}
		return nil
	}
	err := d.records.Walk(ctx, func(key, value []byte) error {
		r := &storagev1.DirectBlobStaged{}
		if err := proto.Unmarshal(value, r); err != nil {
			return err
		}
		if r.SizeRecord {
			if (len(key) != 21 && len(key) != 33) || key[len(key)-1] != 0 || r.Size > math.MaxInt64 || r.Chunk != nil {
				return fmt.Errorf("invalid staged blob size record")
			}
			if err := finish(); err != nil {
				return err
			}
			oid, size, next = bytes.Clone(key[:len(key)-1]), r.Size, 0
			return nil
		}
		if (len(key) != 29 && len(key) != 41) || key[len(key)-9] != 1 || !bytes.Equal(oid, key[:len(key)-9]) || r.Chunk == nil || size == 0 {
			return fmt.Errorf("chunk has no preceding blob size")
		}
		part := binary.BigEndian.Uint64(key[len(key)-8:])
		if part != next || part > (size-1)/ChunkSize {
			return fmt.Errorf("invalid staged blob part order")
		}
		next++
		return emit(&storagev1.DirectBlobPart{Oid: bytes.Clone(oid), Part: part, Size: size, Chunk: r.Chunk})
	})
	if err == nil {
		err = finish()
	}
	return err
}

func (idx *index) directBlobPage(ctx context.Context, ref pageRef) (*storagev1.DirectBlobPage, error) {
	if ref.Length <= 0 || ref.Length > directBlobPageBytes {
		return nil, fmt.Errorf("invalid direct blob page length")
	}
	raw, err := idx.pageBytes(ctx, ref)
	if err != nil {
		return nil, err
	}
	// Bound repeated outer message allocation before protobuf decoding.
	count := 0
	for rest := raw; len(rest) > 0; {
		num, typ, n := protowire.ConsumeTag(rest)
		if n < 0 || (num != 1 && num != 2) || typ != protowire.BytesType {
			return nil, fmt.Errorf("invalid direct blob wire field")
		}
		_, m := protowire.ConsumeBytes(rest[n:])
		if m < 0 {
			return nil, fmt.Errorf("invalid direct blob wire length")
		}
		count++
		if count > fanout {
			return nil, fmt.Errorf("direct blob page exceeds fanout")
		}
		rest = rest[n+m:]
	}
	p := &storagev1.DirectBlobPage{}
	if err := (proto.UnmarshalOptions{RecursionLimit: MaxDeltaDepth + 4}).Unmarshal(raw, p); err != nil {
		return nil, err
	}
	if (len(p.Items) == 0) == (len(p.Children) == 0) {
		return nil, fmt.Errorf("invalid direct blob page shape")
	}
	var prior []byte
	var priorPart uint64
	for _, r := range p.Items {
		if (len(r.Oid) != 20 && len(r.Oid) != 32) || r.Size > math.MaxInt64 || (prior != nil && directKeyCompare(prior, priorPart, r.Oid, r.Part) >= 0) {
			return nil, fmt.Errorf("invalid direct blob leaf key")
		}
		if r.Size == 0 {
			if r.Part != 0 || r.Chunk != nil {
				return nil, fmt.Errorf("invalid empty blob record")
			}
		} else if r.Part > (r.Size-1)/ChunkSize || r.Chunk == nil {
			return nil, fmt.Errorf("invalid direct blob part")
		}
		if r.Chunk != nil {
			if len(r.Chunk.ArchiveRecipe) > 8192 {
				return nil, fmt.Errorf("archive recipe limit")
			}
			depth := 0
			for base := r.Chunk.Base; base != nil; base = base.Base {
				depth++
				if depth > MaxDeltaDepth {
					return nil, fmt.Errorf("direct blob dependency depth exceeds limit")
				}
			}
		}
		prior, priorPart = r.Oid, r.Part
	}
	for _, c := range p.Children {
		if (len(c.MinOid) != 20 && len(c.MinOid) != 32) || c.Page == nil || (prior != nil && directKeyCompare(prior, priorPart, c.MinOid, c.MinPart) >= 0) {
			return nil, fmt.Errorf("invalid direct blob child key")
		}
		prior, priorPart = c.MinOid, c.MinPart
	}
	return p, nil
}

func (s *Snapshot) readBlobPart(ctx context.Context, oid string, part int64) (int64, chunk, error) {
	if part < 0 {
		return 0, chunk{}, fmt.Errorf("negative blob part")
	}
	if s.idx.blobRoot == (pageRef{}) {
		var o object
		if err := s.idx.get(ctx, "o/"+oid, &o); err != nil {
			return 0, chunk{}, err
		}
		if o.Kind != "blob" {
			return 0, chunk{}, fmt.Errorf("%s is not a blob", oid)
		}
		if o.Size == 0 || part > (o.Size-1)/ChunkSize {
			return o.Size, chunk{}, nil
		}
		var c chunk
		err := s.idx.get(ctx, chunkKey(oid, part), &c)
		if err == nil && c.ArchiveRecipe != "" {
			_, err = checkedArchiveBlob(c, oid, o.Size, part)
		}
		return o.Size, c, err
	}
	id, err := hex.DecodeString(oid)
	if err != nil || (len(id) != 20 && len(id) != 32) {
		return 0, chunk{}, fmt.Errorf("invalid blob OID")
	}
	ref := s.idx.blobRoot
	for depth := 0; depth < 16; depth++ {
		p, err := s.idx.directBlobPage(ctx, ref)
		if err != nil {
			return 0, chunk{}, err
		}
		if len(p.Children) > 0 {
			i := sort.Search(len(p.Children), func(i int) bool {
				c := p.Children[i]
				return directKeyCompare(c.MinOid, c.MinPart, id, uint64(part)) > 0
			}) - 1
			if i < 0 {
				return 0, chunk{}, store.ErrNotFound
			}
			ref = decodePageRef(p.Children[i].Page)
			continue
		}
		i := sort.Search(len(p.Items), func(i int) bool { r := p.Items[i]; return directKeyCompare(r.Oid, r.Part, id, uint64(part)) > 0 }) - 1
		if i < 0 || !bytes.Equal(p.Items[i].Oid, id) {
			return 0, chunk{}, store.ErrNotFound
		}
		r := p.Items[i]
		if r.Size == 0 || uint64(part) > (r.Size-1)/ChunkSize {
			return int64(r.Size), chunk{}, nil
		}
		if r.Part != uint64(part) {
			return 0, chunk{}, fmt.Errorf("missing in-range direct blob part")
		}
		c := chunk{Pack: r.Chunk.Pack, Offset: r.Chunk.Offset, Length: r.Chunk.Length, Hash: r.Chunk.Hash, ArchiveRecipe: string(r.Chunk.ArchiveRecipe)}
		if r.Chunk.Base != nil {
			c.Base, err = decodeChunkBase(r.Chunk.Base, 0)
			if err != nil {
				return 0, chunk{}, err
			}
		}
		if c.ArchiveRecipe != "" {
			if _, err := checkedArchiveBlob(c, oid, int64(r.Size), int64(r.Part)); err != nil {
				return 0, chunk{}, err
			}
		}
		return int64(r.Size), c, nil
	}
	return 0, chunk{}, fmt.Errorf("direct blob index exceeds depth limit")
}
