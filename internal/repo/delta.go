package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	storagev1 "gat/internal/gen/gat/storage/v1"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// A base is immutable once queued. Only the ordered sink writes location, and
// workers never read it. Thus a dependent chunk cannot publish before its base.
const MaxDeltaDepth = 8
const maxDeltaCandidates = 8
const baseCacheBytes = 8 << 20

type deltaBase struct {
	depth    int
	raw      []byte
	table    []int32
	location chunkBase
}

func deltaHash(b []byte) uint64 { return binary.LittleEndian.Uint64(b) * 0x9e3779b185ebca87 }

func newDeltaBase(raw []byte) *deltaBase {
	size := 16
	for size < len(raw)/8 {
		size *= 2
	}
	b := &deltaBase{raw: bytes.Clone(raw), table: make([]int32, size)}
	for i := 0; i+16 <= len(raw); i += 16 {
		b.table[deltaHash(raw[i:])>>32&uint64(size-1)] = int32(i + 1)
	}
	return b
}

// A fixed-probe hash table and forward-only scan bound matching work. Copy
// operations refer only to the full base; output can never reference itself.
func (b *deltaBase) delta(raw []byte) []byte {
	return b.deltaInto(nil, raw)
}

// deltaInto retains the matching decisions and canonical protobuf encoding,
// while reusing worker scratch space instead of allocating per-operation messages.
func (b *deltaBase) deltaInto(dst, raw []byte) []byte {
	if len(raw) > 0 {
		dst = protowire.AppendTag(dst, 1, protowire.VarintType)
		dst = protowire.AppendVarint(dst, uint64(len(raw)))
	}
	appendLiteral := func(literal []byte) {
		dst = protowire.AppendTag(dst, 2, protowire.BytesType)
		dst = protowire.AppendVarint(dst, uint64(1+protowire.SizeBytes(len(literal))))
		dst = protowire.AppendTag(dst, 3, protowire.BytesType)
		dst = protowire.AppendBytes(dst, literal)
	}
	literal := 0
	for i := 0; i+16 <= len(raw); {
		pos := int(b.table[deltaHash(raw[i:])>>32&uint64(len(b.table)-1)]) - 1
		if pos < 0 || !bytes.Equal(raw[i:i+16], b.raw[pos:pos+16]) {
			i++
			continue
		}
		if literal < i {
			appendLiteral(raw[literal:i])
		}
		n := 16
		limit := min(len(raw)-i, len(b.raw)-pos)
		// Most version-to-version copies contain long unchanged ranges. Compare
		// these in blocks using the runtime's vectorized equality routine; the
		// final byte scan still stops at exactly the same first mismatch.
		for n+64 <= limit && bytes.Equal(raw[i+n:i+n+64], b.raw[pos+n:pos+n+64]) {
			n += 64
		}
		for n+8 <= limit && binary.LittleEndian.Uint64(raw[i+n:]) == binary.LittleEndian.Uint64(b.raw[pos+n:]) {
			n += 8
		}
		for n < limit && raw[i+n] == b.raw[pos+n] {
			n++
		}
		size := 1 + protowire.SizeVarint(uint64(n))
		if pos != 0 {
			size += 1 + protowire.SizeVarint(uint64(pos))
		}
		dst = protowire.AppendTag(dst, 2, protowire.BytesType)
		dst = protowire.AppendVarint(dst, uint64(size))
		if pos != 0 {
			dst = protowire.AppendTag(dst, 1, protowire.VarintType)
			dst = protowire.AppendVarint(dst, uint64(pos))
		}
		dst = protowire.AppendTag(dst, 2, protowire.VarintType)
		dst = protowire.AppendVarint(dst, uint64(n))
		i += n
		literal = i
	}
	if literal < len(raw) {
		appendLiteral(raw[literal:])
	}
	return dst
}

func applyDelta(base, data []byte) ([]byte, error) {
	if len(base) > ChunkSize || len(data) > 2*ChunkSize {
		return nil, fmt.Errorf("delta exceeds size limit")
	}
	var d storagev1.ChunkDelta
	if err := proto.Unmarshal(data, &d); err != nil {
		return nil, err
	}
	if d.Size == 0 || d.Size > ChunkSize || len(d.Operations) > ChunkSize/8+1 {
		return nil, fmt.Errorf("invalid delta size or operation count")
	}
	out := make([]byte, 0, int(d.Size))
	for _, op := range d.Operations {
		if len(op.Literal) > 0 {
			if op.Length != 0 || op.Offset != 0 || len(op.Literal) > int(d.Size)-len(out) {
				return nil, fmt.Errorf("invalid delta literal")
			}
			out = append(out, op.Literal...)
		} else {
			off, n := uint64(op.Offset), uint64(op.Length)
			if n == 0 || off+n > uint64(len(base)) || n > uint64(int(d.Size)-len(out)) {
				return nil, fmt.Errorf("invalid delta copy")
			}
			out = append(out, base[off:off+n]...)
		}
	}
	if len(out) != int(d.Size) {
		return nil, fmt.Errorf("delta output size mismatch")
	}
	return out, nil
}

func validChunkRange(c chunkBase) bool {
	return strings.HasPrefix(c.Pack, "packs/") && c.Offset >= 0 && c.Length > 0 && c.Length <= 2*ChunkSize && c.Offset <= PackSize-c.Length && len(c.Hash) == 64
}

func decodeFrame(data []byte, limit int) ([]byte, error) {
	var header zstd.Header
	if err := header.Decode(data); err != nil {
		return nil, err
	}
	capacity := limit
	if header.HasFCS {
		if header.FrameContentSize > uint64(limit) {
			return nil, fmt.Errorf("frame exceeds decoded size limit")
		}
		capacity = int(header.FrameContentSize)
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(8<<20), zstd.WithDecodeAllCapLimit(true))
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	decoded, err := dec.DecodeAll(data, make([]byte, 0, capacity))
	if err != nil {
		return nil, err
	}
	// The LRU accounts for length; do not retain oversized decode buffers.
	if cap(decoded) != len(decoded) {
		decoded = bytes.Clone(decoded)
	}
	return decoded, nil
}

func checkedChunk(data []byte, hash string) ([]byte, error) {
	if len(data) > ChunkSize || fmt.Sprintf("%x", sha256.Sum256(data)) != hash {
		return nil, fmt.Errorf("chunk checksum mismatch")
	}
	return data, nil
}

// chunkLocation embeds the complete bounded dependency chain. Readers need no
// history traversal or further index lookups to discover dependency ranges.
func chunkLocation(c chunk) chunkBase {
	return chunkBase{Pack: c.Pack, Offset: c.Offset, Length: c.Length, Hash: c.Hash, Base: c.Base}
}
func (c chunkBase) chunk() chunk {
	return chunk{Pack: c.Pack, Offset: c.Offset, Length: c.Length, Hash: c.Hash, Base: c.Base}
}
func (c chunkBase) depth() (int, error) {
	n := 0
	for p := c.Base; p != nil; p = p.Base {
		n++
		if n > MaxDeltaDepth {
			return 0, fmt.Errorf("delta chain exceeds supported depth")
		}
	}
	return n, nil
}

func (s *Snapshot) readChunk(ctx context.Context, c chunk) ([]byte, error) {
	if c.ArchiveRecipe != "" {
		return s.readArchiveBlob(ctx, c)
	}
	if strings.HasPrefix(c.Pack, "packs/native-") || strings.HasPrefix(c.Pack, "packs/nativechain-") {
		return s.readNativeFlat(ctx, c)
	}
	loc := chunkLocation(c)
	if _, err := loc.depth(); err != nil {
		return nil, err
	}
	for p := &loc; p != nil; p = p.Base {
		if !validChunkRange(*p) {
			return nil, fmt.Errorf("invalid chunk range")
		}
	}
	return s.idx.cache.load(ctx, "data/"+c.Hash, func() ([]byte, error) {
		var chain []chunkBase
		var raw []byte
		for p := &loc; p != nil; p = p.Base {
			if cached, ok := s.idx.cache.get("data/" + p.Hash); ok {
				raw = cached
				break
			}
			chain = append(chain, *p)
		}
		// All missing payloads are known up front. Each flight owns at most depth+1
		// parallel GETs, and never recursively takes another bounded cache slot.
		packed := make([][]byte, len(chain))
		errs := make([]error, len(chain))
		fetchCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		var wg sync.WaitGroup
		for i, part := range chain {
			wg.Go(func() {
				packed[i], _, errs[i] = s.idx.store.Get(fetchCtx, part.Pack, part.Offset, part.Length)
				if errs[i] != nil {
					cancel()
				}
			})
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				return nil, err
			}
		}
		for i := len(chain) - 1; i >= 0; i-- {
			part := chain[i]
			limit := ChunkSize
			if part.Base != nil {
				limit = 2 * ChunkSize
			}
			decoded, err := decodeFrame(packed[i], limit)
			if err != nil {
				return nil, err
			}
			packed[i] = nil
			if part.Base != nil {
				decoded, err = applyDelta(raw, decoded)
				if err != nil {
					return nil, err
				}
			}
			raw, err = checkedChunk(decoded, part.Hash)
			if err != nil {
				return nil, err
			}
			s.idx.cache.put("data/"+part.Hash, raw)
		}
		return raw, nil
	})
}

func anchorKey(hint string) string       { return fmt.Sprintf("a5/%x", sha256.Sum256([]byte(hint))) }
func legacyAnchorKey(hint string) string { return fmt.Sprintf("a/%x", sha256.Sum256([]byte(hint))) }

type baseCandidate struct {
	hint string
	base *deltaBase
}
type baseCache struct {
	entries []baseCandidate
	used    int
	limit   int
}

func newBaseCache(limit int) *baseCache { return &baseCache{limit: limit} }
func (c *baseCache) candidates(hint string) []*deltaBase {
	var out []*deltaBase
	for i := len(c.entries) - 1; i >= 0; i-- {
		if c.entries[i].hint == hint {
			out = append(out, c.entries[i].base)
		}
	}
	return out
}
func (c *baseCache) remove(i int) {
	b := c.entries[i].base
	c.used -= len(b.raw) + 4*len(b.table)
	copy(c.entries[i:], c.entries[i+1:])
	c.entries[len(c.entries)-1] = baseCandidate{}
	c.entries = c.entries[:len(c.entries)-1]
}
func (c *baseCache) put(hint string, b *deltaBase) {
	count, oldest := 0, -1
	for i, entry := range c.entries {
		if entry.hint == hint {
			count++
			if oldest < 0 {
				oldest = i
			}
		}
	}
	if count >= c.limit {
		c.remove(oldest)
	}
	cost := len(b.raw) + 4*len(b.table)
	for len(c.entries) > 0 && (c.used+cost > baseCacheBytes || len(c.entries) >= 64) {
		c.remove(0)
	}
	c.entries = append(c.entries, baseCandidate{hint, b})
	c.used += cost
}
