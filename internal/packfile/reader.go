// Package packrecipe is a bounded, single-pack import experiment, not a mounted reader.
package packrecipe

import (
	"container/list"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	probev1 "gyit/internal/gen/gyit/probe/v1"
	wirecodec "gyit/internal/packcodec"
	"google.golang.org/protobuf/proto"

	"gyit/internal/gitdelta"
	"github.com/klauspost/compress/zstd"
)

type cached[T any] struct {
	key, size int
	value     T
}
type bounded[T any] struct {
	byKey       map[int]*list.Element
	lru         list.List
	used, limit int
}

func (c *bounded[T]) get(k int) (v T, ok bool) {
	if e := c.byKey[k]; e != nil {
		c.lru.MoveToFront(e)
		return e.Value.(cached[T]).value, true
	}
	return
}
func (c *bounded[T]) put(k int, v T, size int) {
	if size > c.limit {
		return
	}
	if c.byKey == nil {
		c.byKey = make(map[int]*list.Element)
	}
	if _, ok := c.byKey[k]; ok {
		return
	}
	for c.used+size > c.limit || len(c.byKey) >= 32768 {
		e := c.lru.Back()
		v := e.Value.(cached[T])
		delete(c.byKey, v.key)
		c.used -= v.size
		c.lru.Remove(e)
	}
	c.byKey[k] = c.lru.PushFront(cached[T]{k, size, v})
	c.used += size
}

type recipe struct {
	root    int
	program *gitdelta.Program
}
type fullRoot struct {
	raw, packed []byte
	hash        string
}
type Base struct {
	Packed []byte
	Hash   string
}
type Output struct {
	Data []byte
	Hash string
	Base *Base
	Full bool
	Size int
}
type Counters struct{ Objects, Recipes, FullTargets, Fallbacks, Inflated, Reconstructed, CompressedFull, CompressedRecipe int64 }
type Reader struct {
	deferred              *deferredSource
	conversion            ConversionCounters
	p                     *packReader
	source                string
	ctx                   context.Context
	encoder               *zstd.Encoder
	recipes               bounded[recipe]
	roots                 bounded[*fullRoot]
	raw, wire, compressed []byte
	Stats                 Counters
}

func Open(ctx context.Context, pack, source string) (*Reader, error) {
	return openReader(ctx, pack, source, false)
}

// OpenDeferred retains bounded native delta recipes and authenticates reconstructed
// content when it is read, instead of inflating every source object during import.
func OpenDeferred(ctx context.Context, pack, source string) (*Reader, error) {
	return openReader(ctx, pack, source, true)
}

func openReader(ctx context.Context, pack, source string, deferred bool) (*Reader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, e := openPack(pack, 64<<20)
	if e != nil {
		return nil, e
	}
	enc, e := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithZeroFrames(true))
	if e != nil {
		p.close()
		return nil, e
	}
	r := &Reader{p: p, source: source, ctx: ctx, encoder: enc, recipes: bounded[recipe]{limit: 16 << 20}, roots: bounded[*fullRoot]{limit: 16 << 20}}
	if deferred {
		r.deferred, e = openDeferred(p, pack)
		if e != nil {
			r.Close()
			return nil, e
		}
	}
	return r, nil
}
func (r *Reader) Close() {
	if r.deferred != nil {
		r.deferred.close()
	}
	r.encoder.Close()
	r.p.close()
}
func (r *Reader) Has(oid string) bool {
	id, e := hex.DecodeString(oid)
	if e != nil {
		return false
	}
	_, e = r.p.offset(id)
	return e == nil
}
func (r *Reader) compose(offset int) (recipe, error) {
	if value, ok := r.recipes.get(offset); ok {
		return value, nil
	}
	var chain [64]frame
	n, cur := 0, offset
	var result recipe
	for {
		if e := r.ctx.Err(); e != nil {
			return result, e
		}
		if value, ok := r.recipes.get(cur); ok {
			result = value
			break
		}
		f, base, e := r.p.header(cur)
		if e != nil {
			return result, e
		}
		if base == 0 {
			if f.kind != 3 || f.size > gitdelta.MaxSize {
				return result, gitdelta.ErrLimit
			}
			p, e := gitdelta.From(uint32(f.size))
			if e != nil {
				return result, e
			}
			result = recipe{cur, p}
			r.recipes.put(cur, result, p.MemoryBytes()+64)
			break
		}
		if n == len(chain) {
			return result, gitdelta.ErrLimit
		}
		chain[n] = f
		n++
		cur = base
	}
	for i := n - 1; i >= 0; i-- {
		body, e := r.p.inflate(chain[i])
		if e != nil {
			return result, e
		}
		r.Stats.Inflated += int64(len(body))
		p, e := result.program.Compose(body)
		if e != nil {
			return result, e
		}
		result.program = p
		r.recipes.put(chain[i].offset, result, p.MemoryBytes()+64)
	}
	return result, nil
}
func (r *Reader) root(offset int) (*fullRoot, error) {
	if root, ok := r.roots.get(offset); ok {
		return root, nil
	}
	f, base, e := r.p.header(offset)
	if e != nil {
		return nil, e
	}
	if base != 0 || f.kind != 3 {
		return nil, fmt.Errorf("invalid native root")
	}
	body, packed, e := r.p.rawFrame(f)
	if e != nil {
		return nil, e
	}
	r.Stats.Inflated += int64(len(body))

	root := &fullRoot{raw: body, packed: packed, hash: fmt.Sprintf("%x", sha256.Sum256(body))}
	r.roots.put(offset, root, len(root.raw)+len(root.packed)+128)
	return root, nil
}
func verify(oid string, raw []byte) error {
	h := sha1.New()
	fmt.Fprintf(h, "blob %d\x00", len(raw))
	h.Write(raw)
	if hex.EncodeToString(h.Sum(nil)) != oid {
		return fmt.Errorf("native target checksum mismatch: %s", oid)
	}
	return nil
}
func (r *Reader) convertEager(oid string, dst []byte) (Output, error) {
	id, e := hex.DecodeString(oid)
	if e != nil {
		return Output{}, e
	}
	off, e := r.p.offset(id)
	if e != nil {
		return Output{}, e
	}
	var frames []frame
	cur := off
	for {
		if e = r.ctx.Err(); e != nil {
			return Output{}, e
		}
		if len(frames) >= 64 {
			return Output{}, gitdelta.ErrLimit
		}
		f, base, e := r.p.header(cur)
		if e != nil {
			return Output{}, e
		}
		frames = append(frames, f)
		d, e := r.p.frameData(f)
		if e != nil {
			return Output{}, e
		}
		// The published reader has per-frame bounds in addition to the bundle
		// limit. Valid larger source programs must retain the ordinary fallback.
		if len(d.raw) > 2<<20 || len(d.packed) > 2<<20 {
			return Output{}, gitdelta.ErrLimit
		}
		if base == 0 {
			if f.kind != 3 || len(d.raw) > 1<<20 {
				return Output{}, gitdelta.ErrLimit
			}
			break
		}
		_, n := binary.Uvarint(d.raw)
		if n <= 0 {
			return Output{}, fmt.Errorf("delta size")
		}
		size, n := binary.Uvarint(d.raw[n:])
		if n <= 0 {
			return Output{}, fmt.Errorf("delta target")
		}
		if size > 1<<20 {
			return Output{}, gitdelta.ErrLimit
		}
		cur = base
	}
	root, e := r.root(cur)
	if e != nil {
		return Output{}, e
	}
	target, e := r.p.get(oid)
	if e != nil {
		return Output{}, e
	}
	if e = verify(oid, target.data); e != nil {
		return Output{}, e
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(target.data))
	if len(frames) > 1 && (len(target.data) < 4096 || len(root.raw) > len(target.data)*5/4) {
		packed, err := wirecodec.Deflate(target.data)
		if err != nil {
			return Output{}, err
		}
		return Output{Data: packed, Hash: hash, Full: true, Size: len(target.data)}, nil
	}

	if len(frames) == 1 {
		return Output{Data: root.packed, Hash: hash, Full: true, Size: len(target.data)}, nil
	}
	b := &probev1.NativeProgramBundle{}
	for i := len(frames) - 2; i >= 0; i-- {
		d, e := r.p.frameData(frames[i])
		if e != nil {
			return Output{}, e
		}
		b.Frames = append(b.Frames, &probev1.NativeFrame{RawSize: uint32(len(d.raw)), Zlib: d.packed})
	}
	wire, e := proto.MarshalOptions{Deterministic: true}.MarshalAppend(dst[:0], b)
	if e != nil {
		return Output{}, e
	}
	if len(wire) > 2<<20 {
		return Output{}, gitdelta.ErrLimit
	}
	return Output{Data: wire, Hash: hash, Base: &Base{Packed: root.packed, Hash: root.hash}, Size: len(target.data)}, nil
}
