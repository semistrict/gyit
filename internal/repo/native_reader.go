package repo

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	probev1 "gat/internal/gen/gat/probe/v1"
	wirecodec "gat/internal/packcodec"
	"gat/internal/store"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"strings"
	"sync"
)

func (s *Snapshot) readNativeFlat(ctx context.Context, c chunk) ([]byte, error) {
	loc := chunkLocation(c)
	if _, err := loc.depth(); err != nil {
		return nil, err
	}
	if loc.Base != nil && loc.Base.Base != nil {
		return nil, fmt.Errorf("native payload must be a root plus one bundle")
	}
	for p := &loc; p != nil; p = p.Base {
		if !deferredNativeRange(*p) {
			return nil, fmt.Errorf("invalid chunk range")
		}
	}
	return s.idx.cache.load(ctx, "data/"+c.Hash, func() ([]byte, error) {
		reads := store.NewReadScope(s.idx.store)
		defer reads.Close()
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
		if len(chain) == 2 && chain[0].Pack == chain[1].Pack && chain[1].Offset+chain[1].Length == chain[0].Offset {
			p, _, err := reads.Get(ctx, chain[1].Pack, chain[1].Offset, chain[1].Length+chain[0].Length)
			if err != nil {
				return nil, err
			}
			if int64(len(p)) != chain[1].Length+chain[0].Length {
				return nil, fmt.Errorf("short capsule")
			}
			packed[1], packed[0] = p[:chain[1].Length], p[chain[1].Length:]
		} else {
			fetchCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			var wg sync.WaitGroup
			for i, part := range chain {
				wg.Go(func() {
					packed[i], _, errs[i] = reads.Get(fetchCtx, part.Pack, part.Offset, part.Length)
					if errs[i] != nil {
						cancel()
					}
				})
			}
			wg.Wait()
		}
		for i, err := range errs {
			if err != nil {
				return nil, err
			}
			if int64(len(packed[i])) != chain[i].Length {
				return nil, fmt.Errorf("short native payload")
			}
		}
		for i := len(chain) - 1; i >= 0; i-- {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			part := chain[i]
			limit := ChunkSize
			if part.Base != nil {
				limit = 2 * ChunkSize
			}
			var decoded []byte
			var err error
			if part.Base != nil && strings.HasPrefix(part.Pack, "packs/nativechain-") {
				bundle, e := deferredNativeBundle(packed[i])
				if e != nil {
					return nil, e
				}
				decoded = raw
				for _, f := range bundle.Frames {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					if f.RawSize > 2*ChunkSize || len(f.Zlib) > 2*ChunkSize {
						return nil, fmt.Errorf("program bound")
					}
					program := make([]byte, int(f.RawSize))
					if err = wirecodec.Inflate(program, f.Zlib); err != nil {
						return nil, err
					}
					decoded, err = apply(decoded, program)
					if err != nil {
						return nil, err
					}
				}
			} else {
				decoded, err = wirecodec.InflateBounded(packed[i], limit)
				if err != nil {
					return nil, err
				}
				if part.Base != nil {
					decoded, err = apply(raw, decoded)
					if err != nil {
						return nil, err
					}
				}
			}
			packed[i] = nil

			raw, err = checkedDeferredNative(decoded, part.Hash)
			if err != nil {
				return nil, err
			}
			s.idx.cache.put("data/"+part.Hash, raw)
		}
		return raw, nil
	})
}

// Hash tags make verified Git objects distinct from legacy raw SHA256 cache keys.
// Pack dispatch and the publication format remain the caller's responsibility.
func deferredNativeRange(c chunkBase) bool {
	if (!strings.HasPrefix(c.Pack, "packs/native-") && !strings.HasPrefix(c.Pack, "packs/nativechain-")) || c.Offset < 0 || c.Length <= 0 || c.Length > 2*ChunkSize || c.Offset > PackSize-c.Length {
		return false
	}
	text, width := c.Hash, 64
	if strings.HasPrefix(text, "git-sha1:") {
		text, width = strings.TrimPrefix(text, "git-sha1:"), 40
	}
	if len(text) != width || strings.ToLower(text) != text {
		return false
	}
	_, err := hex.DecodeString(text)
	return err == nil
}

func checkedDeferredNative(data []byte, hash string) ([]byte, error) {
	if !strings.HasPrefix(hash, "git-sha1:") {
		return checkedChunk(data, hash)
	}
	if len(data) > ChunkSize {
		return nil, fmt.Errorf("native blob size limit")
	}
	expected, err := hex.DecodeString(strings.TrimPrefix(hash, "git-sha1:"))
	if err != nil || len(expected) != sha1.Size {
		return nil, fmt.Errorf("invalid native Git identity")
	}
	h := sha1.New()
	fmt.Fprintf(h, "blob %d\x00", len(data))
	h.Write(data)
	var actual [sha1.Size]byte
	copy(actual[:], h.Sum(nil))
	for i := range actual {
		if actual[i] != expected[i] {
			return nil, fmt.Errorf("native Git blob checksum mismatch")
		}
	}
	return data, nil
}

// Count frame records before protobuf allocates messages: a 2MiB input could
// otherwise contain a million empty frames. The existing bundle has one field.
func deferredNativeBundle(data []byte) (*probev1.NativeProgramBundle, error) {
	if len(data) == 0 || len(data) > 2*ChunkSize {
		return nil, fmt.Errorf("native bundle size limit")
	}
	frames := 0
	for remaining := data; len(remaining) != 0; {
		field, typ, n := protowire.ConsumeTag(remaining)
		if n < 0 || field != 1 || typ != protowire.BytesType {
			return nil, fmt.Errorf("invalid native bundle field")
		}
		remaining = remaining[n:]
		_, n = protowire.ConsumeBytes(remaining)
		if n < 0 {
			return nil, fmt.Errorf("truncated native bundle frame")
		}
		remaining = remaining[n:]
		frames++
		if frames > 63 {
			return nil, fmt.Errorf("program count")
		}
	}
	var bundle probev1.NativeProgramBundle
	if err := (proto.UnmarshalOptions{RecursionLimit: 4, DiscardUnknown: true}).Unmarshal(data, &bundle); err != nil {
		return nil, err
	}
	if len(bundle.Frames) != frames {
		return nil, fmt.Errorf("program count")
	}
	return &bundle, nil
}
