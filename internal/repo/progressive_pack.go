package repo

import (
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
	"io"
)

const progressiveSegment = 8 << 20
const progressiveObjectLimit = 64 << 20

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
	ctx       context.Context
	p         *Progressive
	pack      string
	pos, end  int64
	buf       []byte
	localOnly bool
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
			r.localOnly = false
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

// Return one owned buffer in decoded-cache format: object kind followed by the
// payload. Recursive delta reconstruction and cache admission share this buffer.
func (p *Progressive) decodeObject(ctx context.Context, o *pb.ProgressiveObject, depth int, budget *int64) ([]byte, bool, error) {
	if source, ok := ctx.Value(historySourceKey{}).(*historySource); ok {
		key := fmt.Sprintf("offset/%s/%d", o.Pack, o.Offset)
		if raw, ok := source.cache.get(key); ok {
			if int64(len(raw)-1) > *budget {
				return nil, false, fmt.Errorf("object decoding exceeds bounded working memory")
			}
			*budget -= int64(len(raw) - 1)
			return raw, true, nil
		}
		raw, localOnly, err := p.decodeObjectWithOrigin(ctx, o, depth, budget)
		// An offset cache hit must prove that its entire delta chain came
		// from this acquisition, including REF_DELTA recipe lookups.
		if err == nil && localOnly {
			source.cache.put(key, raw)
		}
		return raw, localOnly, err
	}
	// Shared delta bases are addressed by immutable pack identity and offset.
	// Keep them in the same bounded, uncompressed cache as verified objects;
	// never treat this offset entry as proof of an object's Git identity.
	// Top-level results already have their verified OID cache entry.
	if depth == 0 || p.cache == nil {
		return p.decodeObjectWithOrigin(ctx, o, depth, budget)
	}
	if depth > 128 || !validProgressiveOID(o.Pack) || o.Offset < 12 || o.Offset >= o.PackSize-20 {
		return nil, false, fmt.Errorf("pack recipe bounds")
	}
	key := fmt.Sprintf("progressive-delta/%s/%d/%d", o.Pack, o.PackSize, o.Offset)
	data, release, err := p.cache.borrowCached(ctx, key)
	if err == nil {
		defer release()
		if len(data) < 1 || int64(len(data)-1) > *budget {
			return nil, false, fmt.Errorf("object decoding exceeds bounded working memory")
		}
		*budget -= int64(len(data) - 1)
		// Recursive decoding returns owned bytes; no mapped lease may escape.
		return append([]byte(nil), data...), false, nil
	}
	release()
	if !errors.Is(err, store.ErrNotFound) {
		return nil, false, err
	}
	data, _, err = p.decodeObjectWithOrigin(ctx, o, depth, budget)
	if err != nil {
		return nil, false, err
	}
	// Admit only after decoding, without a load slot or recursive singleflight:
	// malformed delta cycles must reach the depth bound rather than deadlock.
	_, release, err = p.cache.borrowLimited(ctx, key, nil, func() ([]byte, error) { return data, nil })
	release()
	return data, false, err
}
func (p *Progressive) decodeObjectWithOrigin(ctx context.Context, o *pb.ProgressiveObject, depth int, budget *int64) ([]byte, bool, error) {
	if depth > 128 || !validProgressiveOID(o.Pack) || o.Offset < 12 || o.Offset >= o.PackSize-20 {
		return nil, false, fmt.Errorf("pack recipe bounds")
	}
	r := &progressiveRange{ctx: ctx, p: p, pack: o.Pack, pos: o.Offset, end: o.PackSize - 20, localOnly: true}
	kind, size, baseOff, baseOID, err := progressiveHeader(r, o.Offset)
	if err != nil {
		return nil, false, err
	}
	if size < 0 || size > progressiveObjectLimit || *budget < size {
		return nil, false, fmt.Errorf("object decoding exceeds bounded working memory")
	}
	*budget -= size
	var z io.ReadCloser
	source, local := ctx.Value(historySourceKey{}).(*historySource)
	if local {
		z, err = source.inflater(r)
	} else {
		z, err = zlib.NewReader(r)
	}
	if err != nil {
		return nil, false, err
	}
	// The header length has already passed the object and working-memory
	// bounds. Allocate it once rather than repeatedly growing a ReadAll buffer.
	// Read through the end marker as well: filling the declared length alone
	// would accept an extra byte or miss a trailing zlib checksum failure.
	storage := make([]byte, int(size)+1)
	data := storage[1:]
	_, err = io.ReadFull(z, data)
	if err == nil {
		var extra [1]byte
		if _, endErr := io.ReadFull(z, extra[:]); endErr == nil {
			err = fmt.Errorf("pack object length mismatch")
		} else if endErr != io.EOF {
			err = endErr
		}
	}
	if local {
		source.releaseInflater(z)
	} else {
		z.Close()
	}
	if err != nil {
		return nil, false, err
	}
	if kind != 6 && kind != 7 {
		storage[0] = kind
		return storage, r.localOnly, nil
	}
	base := &pb.ProgressiveObject{Pack: o.Pack, PackSize: o.PackSize, Offset: baseOff}
	if baseOID != "" {
		var storedRecipe bool
		base, storedRecipe, err = p.objectRecipe(ctx, baseOID)
		r.localOnly = r.localOnly && !storedRecipe
		if err != nil {
			return nil, false, err
		}
	}
	raw, baseLocal, err := p.decodeObject(ctx, base, depth+1, budget)
	r.localOnly = r.localOnly && baseLocal
	if err != nil {
		return nil, false, err
	}
	storage, err = applyLimitPrefix(raw[1:], data, progressiveObjectLimit, 1)
	if err != nil {
		return nil, false, err
	}
	storage[0] = raw[0]
	return storage, r.localOnly, nil
}

// Acquisition indexes already locate the packed bytes. The decoder validates
// their encoded length and delta result size; remote reads also check identity;
// computing the reconstructed size here would inflate every delta twice.
func (p *Progressive) objectRecipe(ctx context.Context, oid string) (*pb.ProgressiveObject, bool, error) {
	if source, ok := ctx.Value(historySourceKey{}).(*historySource); ok {
		o, _, err := source.locate(oid)
		if err != nil || o != nil {
			return o, false, err
		}
	}
	var o pb.ProgressiveObject
	idx := p.index()
	idx.pageRanges = true
	err := idx.get(ctx, "g/"+oid, &o)
	return &o, true, err
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
		if cache == p.cache && p.readSlots != nil {
			// Foreground decoding retains its two-object bound. Acquisition
			// can use up to four global slots without raising that read limit.
			select {
			case p.readSlots <- struct{}{}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			defer func() { <-p.readSlots }()
		}
		select {
		case p.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		defer func() { <-p.slots }()
		o, knownSize, err := p.objectRecipe(ctx, oid)
		if err != nil {
			return nil, err
		}
		budget := int64(256 << 20)
		storage, localOnly, err := p.decodeObject(ctx, o, 0, &budget)
		if err != nil {
			return nil, err
		}
		data, kind := storage[1:], storage[0]
		if knownSize && int64(len(data)) != o.Size {
			return nil, fmt.Errorf("object size mismatch")
		}
		names := map[byte]string{1: "commit", 2: "tree", 3: "blob", 4: "tag"}
		name := names[kind]
		if name == "" {
			return nil, fmt.Errorf("invalid object kind")
		}
		source, acquisition := ctx.Value(historySourceKey{}).(*historySource)
		if !acquisition || !source.indexedByGit || knownSize || !localOnly {
			h := sha1.New()
			fmt.Fprintf(h, "%s %d%c", name, len(data), 0)
			h.Write(data)
			if hex.EncodeToString(h.Sum(nil)) != oid {
				return nil, fmt.Errorf("object identity mismatch")
			}
		}
		if cache != p.cache {
			// The decoder's owned buffer already uses the cache format. Add
			// its OID name without copying the payload a second time.
			key := fmt.Sprintf("offset/%s/%d", o.Pack, o.Offset)
			cache.alias(key, "progressive-object/"+oid)
		}
		return storage, nil
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
	// The loader already owns a separate decoder budget. Holding page-cache
	// slots here can deadlock all object readers waiting for nested index reads.
	return cache.borrowLimited(ctx, "progressive-object/"+oid, nil, load)
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
