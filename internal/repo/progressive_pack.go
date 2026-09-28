package repo

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

const progressiveSegment = 8 << 20
const progressiveObjectLimit = 64 << 20

// ImportPacks copies only new packs and incrementally updates the object index.
// Metadata comes from pack headers (delta result lengths from their prefixes),
// not a full reconstruction/verification pass over historical file bodies.
func (p *Progressive) ImportPacks(ctx context.Context, gitdir string) error {
	files, err := filepath.Glob(filepath.Join(gitdir, "objects", "pack", "pack-*.idx"))
	if err != nil {
		return err
	}
	uploads := newPublicationUploads(ctx, p.store)
	defer uploads.close()
	return p.stage(func(changes *bolt.Bucket) error {
		for _, file := range files {
			if err := ctx.Err(); err != nil {
				return err
			}
			id := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(file), "pack-"), ".idx")
			if !validProgressiveOID(id) {
				return fmt.Errorf("invalid pack identity")
			}
			var known pb.ProgressiveObject
			if err := p.get(ctx, "pack/"+id, &known); err == nil {
				continue
			} else if !errors.Is(err, store.ErrNotFound) {
				return err
			}
			if err := p.importPack(ctx, uploads, file, id, changes); err != nil {
				return err
			}
		}
		if key, _ := changes.Cursor().First(); key == nil {
			return nil
		}
		p.writer.Lock()
		defer p.writer.Unlock()
		return p.publishUploads(ctx, changes, uploads)
	})
}
func (p *Progressive) importPack(ctx context.Context, backend store.Store, indexPath, id string, b *bolt.Bucket) error {
	idx, err := os.ReadFile(indexPath)
	if err != nil {
		return err
	}
	if len(idx) < 1072 || !bytes.Equal(idx[:8], []byte{255, 't', 'O', 'c', 0, 0, 0, 2}) {
		return fmt.Errorf("unsupported pack index")
	}
	count := int(binary.BigEndian.Uint32(idx[1028:1032]))
	if count > (len(idx)-1072)/28 {
		return fmt.Errorf("pack index bounds")
	}
	file, err := os.Open(strings.TrimSuffix(indexPath, ".idx") + ".pack")
	if err != nil {
		return err
	}
	defer file.Close()
	st, err := file.Stat()
	if err != nil {
		return err
	}
	if st.Size() < 32 || st.Size() > int64(^uint(0)>>1) {
		return fmt.Errorf("pack size bounds")
	}
	data, err := unix.Mmap(int(file.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_PRIVATE)
	if err != nil {
		return err
	}
	defer unix.Munmap(data)
	if string(data[:4]) != "PACK" || binary.BigEndian.Uint32(data[4:8]) != 2 || int(binary.BigEndian.Uint32(data[8:12])) != count || hex.EncodeToString(data[len(data)-20:]) != id {
		return fmt.Errorf("pack header mismatch")
	}
	for off := int64(0); off < st.Size(); off += progressiveSegment {
		if err = backend.Put(ctx, progressivePackKey(id, off/progressiveSegment), data[off:min(st.Size(), off+progressiveSegment)], ""); err != nil {
			return err
		}
	}
	var inflater io.ReadCloser
	defer func() {
		if inflater != nil {
			inflater.Close()
		}
	}()
	for i := 0; i < count; i++ {
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		off := uint64(binary.BigEndian.Uint32(idx[1032+24*count+4*i:]))
		if off&0x80000000 != 0 {
			pos := uint64(1032+28*count) + 8*(off&0x7fffffff)
			if pos+8 > uint64(len(idx)-40) {
				return fmt.Errorf("large offset bounds")
			}
			off = binary.BigEndian.Uint64(idx[pos:])
		}
		if off < 12 || off >= uint64(len(data)-20) {
			return fmt.Errorf("pack offset bounds")
		}
		in := bytes.NewReader(data[off : len(data)-20])
		kind, size, _, _, err := progressiveHeader(in, int64(off))
		if err != nil {
			return err
		}
		if kind == 6 || kind == 7 {
			if inflater == nil {
				inflater, err = zlib.NewReader(in)
			} else {
				err = inflater.(zlib.Resetter).Reset(in, nil)
			}
			if err != nil {
				return err
			}
			br := bufio.NewReaderSize(inflater, 32)
			if _, err = binary.ReadUvarint(br); err != nil {
				return err
			}
			n, e := binary.ReadUvarint(br)
			if e != nil || n > 1<<62 {
				return fmt.Errorf("delta result size")
			}
			size = int64(n)
		}
		oid := hex.EncodeToString(idx[1032+20*i : 1032+20*(i+1)])
		// Small foreground history packs are already mapped here. Cache only
		// independently encoded, bounded commits; never retain compressed data.
		// Cache admission is optional and cannot turn an import into verification.
		if st.Size() <= 1<<20 && kind == 1 && size <= 64<<10 {
			p.cacheCommit(ctx, oid, in, size)
		}
		if err = progressivePut(b, "g/"+oid, &pb.ProgressiveObject{Pack: id, PackSize: st.Size(), Offset: int64(off), Size: size}); err != nil {
			return err
		}
	}
	return progressivePut(b, "pack/"+id, &pb.ProgressiveObject{Pack: id, PackSize: st.Size()})
}

func (p *Progressive) cacheCommit(ctx context.Context, oid string, in io.Reader, size int64) {
	if ctx.Err() != nil || (p.cache.disk == nil && p.cache.max == 0) {
		return
	}
	z, err := zlib.NewReader(in)
	if err != nil {
		return
	}
	raw, err := io.ReadAll(io.LimitReader(z, size+1))
	z.Close()
	if err != nil || int64(len(raw)) != size {
		return
	}
	h := sha1.New()
	fmt.Fprintf(h, "commit %d%c", len(raw), 0)
	h.Write(raw)
	if hex.EncodeToString(h.Sum(nil)) != oid {
		return
	}
	p.cache.put("progressive-object/"+oid, append([]byte{1}, raw...))
}

func progressiveHeader(r io.ByteReader, off int64) (kind byte, size int64, baseOffset int64, baseOID string, err error) {
	b, e := r.ReadByte()
	if e != nil {
		err = e
		return
	}
	kind = (b >> 4) & 7
	size = int64(b & 15)
	for shift := uint(4); b&128 != 0; shift += 7 {
		if shift > 56 {
			err = fmt.Errorf("pack size overflow")
			return
		}
		b, err = r.ReadByte()
		if err != nil {
			return
		}
		size |= int64(b&127) << shift
	}
	switch kind {
	case 1, 2, 3, 4:
	case 6:
		b, err = r.ReadByte()
		if err != nil {
			return
		}
		distance := int64(b & 127)
		for n := 0; b&128 != 0; n++ {
			if n >= 8 {
				err = fmt.Errorf("delta offset overflow")
				return
			}
			b, err = r.ReadByte()
			if err != nil {
				return
			}
			distance = ((distance + 1) << 7) | int64(b&127)
		}
		if distance <= 0 || distance > off-12 {
			err = fmt.Errorf("delta base offset")
			return
		}
		baseOffset = off - distance
	case 7:
		id := make([]byte, 20)
		for i := range id {
			id[i], err = r.ReadByte()
			if err != nil {
				return
			}
		}
		baseOID = hex.EncodeToString(id)
	default:
		err = fmt.Errorf("invalid pack object type")
	}
	return
}

// A bounded range reader spans immutable pack segments, without downloading the
// pack or placing compressed data in a second persistent cache.
type progressiveRange struct {
	ctx      context.Context
	p        *Progressive
	pack     string
	pos, end int64
	buf      []byte
}

func (r *progressiveRange) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(b) == 0 {
		return 0, nil
	}
	if len(r.buf) == 0 {
		if r.pos >= r.end {
			return 0, io.EOF
		}
		n := min(int64(32<<10), r.end-r.pos, progressiveSegment-r.pos%progressiveSegment)
		var raw []byte
		var err error
		if source, ok := r.ctx.Value(historySourceKey{}).(*historySource); ok {
			for _, pack := range source.packs {
				if pack.id == r.pack && r.pos+n <= int64(len(pack.data)) {
					raw = pack.data[r.pos : r.pos+n]
					break
				}
			}
		}
		if raw == nil {
			raw, _, err = r.p.store.Get(r.ctx, progressivePackKey(r.pack, r.pos/progressiveSegment), r.pos%progressiveSegment, n)
		}
		if err != nil {
			return 0, err
		}
		r.buf = raw
	}
	n := copy(b, r.buf)
	r.buf = r.buf[n:]
	r.pos += int64(n)
	return n, nil
}
func (r *progressiveRange) ReadByte() (byte, error) {
	// Check cancellation at each bounded range refill, not each deflate bit byte.
	if len(r.buf) > 0 {
		b := r.buf[0]
		r.buf = r.buf[1:]
		r.pos++
		return b, nil
	}
	var b [1]byte
	_, e := r.Read(b[:])
	return b[0], e
}
func (p *Progressive) decodeObject(ctx context.Context, o *pb.ProgressiveObject, depth int, budget *int64) ([]byte, byte, error) {
	if source, ok := ctx.Value(historySourceKey{}).(*historySource); ok {
		key := fmt.Sprintf("offset/%s/%d", o.Pack, o.Offset)
		if raw, ok := source.cache.get(key); ok {
			if int64(len(raw)-1) > *budget {
				return nil, 0, fmt.Errorf("object decoding exceeds bounded working memory")
			}
			*budget -= int64(len(raw) - 1)
			return raw[1:], raw[0], nil
		}
		raw, kind, err := p.decodeObjectUncached(ctx, o, depth, budget)
		if err == nil {
			source.cache.put(key, append([]byte{kind}, raw...))
		}
		return raw, kind, err
	}
	return p.decodeObjectUncached(ctx, o, depth, budget)
}
func (p *Progressive) decodeObjectUncached(ctx context.Context, o *pb.ProgressiveObject, depth int, budget *int64) ([]byte, byte, error) {
	if depth > 128 || !validProgressiveOID(o.Pack) || o.Offset < 12 || o.Offset >= o.PackSize-20 {
		return nil, 0, fmt.Errorf("pack recipe bounds")
	}
	r := &progressiveRange{ctx: ctx, p: p, pack: o.Pack, pos: o.Offset, end: o.PackSize - 20}
	kind, size, baseOff, baseOID, err := progressiveHeader(r, o.Offset)
	if err != nil {
		return nil, 0, err
	}
	if size < 0 || size > progressiveObjectLimit || *budget < size {
		return nil, 0, fmt.Errorf("object decoding exceeds bounded working memory")
	}
	*budget -= size
	z, err := zlib.NewReader(r)
	if err != nil {
		return nil, 0, err
	}
	data, err := io.ReadAll(io.LimitReader(z, size+1))
	z.Close()
	if err != nil {
		return nil, 0, err
	}
	if int64(len(data)) != size {
		return nil, 0, fmt.Errorf("pack object length mismatch")
	}
	if kind != 6 && kind != 7 {
		return data, kind, nil
	}
	base := &pb.ProgressiveObject{Pack: o.Pack, PackSize: o.PackSize, Offset: baseOff}
	if baseOID != "" {
		if err = p.get(ctx, "g/"+baseOID, base); err != nil {
			return nil, 0, err
		}
	}
	raw, kind, err := p.decodeObject(ctx, base, depth+1, budget)
	if err != nil {
		return nil, 0, err
	}
	data, err = applyLimit(raw, data, progressiveObjectLimit)
	return data, kind, err
}
func (p *Progressive) borrowObject(ctx context.Context, oid string) ([]byte, func(), error) {
	if !validProgressiveOID(oid) {
		return nil, func() {}, fmt.Errorf("invalid object identity")
	}
	cache := p.cache
	if source, ok := ctx.Value(historySourceKey{}).(*historySource); ok {
		cache = source.cache
	}
	load := func() ([]byte, error) {
		select {
		case p.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		defer func() { <-p.slots }()
		var o pb.ProgressiveObject
		if err := p.get(ctx, "g/"+oid, &o); err != nil {
			return nil, err
		}
		budget := int64(256 << 20)
		data, kind, err := p.decodeObject(ctx, &o, 0, &budget)
		if err != nil {
			return nil, err
		}
		if int64(len(data)) != o.Size {
			return nil, fmt.Errorf("object size mismatch")
		}
		names := map[byte]string{1: "commit", 2: "tree", 3: "blob", 4: "tag"}
		name := names[kind]
		if name == "" {
			return nil, fmt.Errorf("invalid object kind")
		}
		h := sha1.New()
		fmt.Fprintf(h, "%s %d%c", name, len(data), 0)
		h.Write(data)
		if hex.EncodeToString(h.Sum(nil)) != oid {
			return nil, fmt.Errorf("object identity mismatch")
		}
		return append([]byte{kind}, data...), nil
	}
	if cache != p.cache {
		// Query-owned mmap input must not outlive its owner on cancellation. Do not
		// run this loader in the asynchronous shared-cache singleflight.
		key := "progressive-object/" + oid
		if b, ok := cache.get(key); ok {
			return b, func() {}, nil
		}
		b, err := load()
		if err == nil {
			cache.put(key, b)
		}
		return b, func() {}, err
	}
	return cache.borrow(ctx, "progressive-object/"+oid, load)
}
func (p *Progressive) object(ctx context.Context, oid string) ([]byte, byte, error) {
	data, release, err := p.borrowObject(ctx, oid)
	defer release()
	if err != nil {
		return nil, 0, err
	}
	return append([]byte(nil), data[1:]...), data[0], nil
}
func (p *Progressive) readAt(ctx context.Context, oid string, b []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	if len(b) == 0 {
		return 0, nil
	}
	if err := p.Ensure(ctx, []string{oid}); err != nil {
		return 0, err
	}
	raw, release, err := p.borrowObject(ctx, oid)
	defer release()
	if err != nil {
		return 0, err
	}
	if raw[0] != 3 {
		return 0, fmt.Errorf("not a blob")
	}
	raw = raw[1:]
	if off >= int64(len(raw)) {
		return 0, io.EOF
	}
	n := copy(b, raw[off:])
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}
