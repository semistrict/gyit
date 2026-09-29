package repo

import (
	"bytes"
	"context"
	"google.golang.org/protobuf/proto"
	"strings"
	"testing"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
)

type recipeByteStore struct {
	store.Store
	bytes int64
	reads int
}

func (s *recipeByteStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	b, v, e := s.Store.Get(ctx, key, off, n)
	s.bytes += int64(len(b))
	s.reads++
	return b, v, e
}

func TestObjectRecipeReadsOnlyNeededIndexPages(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(map[bool]string{false: "cold", true: "container-cached"}[warm], func(t *testing.T) {
			backend, e := store.NewLocal(t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			w := &indexWriter{ctx: t.Context(), store: backend, prefix: "recipe-ranges"}
			oid := strings.Repeat("1", 40)
			want := &pb.ProgressiveObject{Pack: strings.Repeat("a", 40), PackSize: 1000, Offset: 12, Size: 42}
			value, e := marshal(want)
			if e != nil {
				t.Fatal(e)
			}
			leaf, e := w.save(page{Items: []item{{Key: "g/" + oid, Value: value}}})
			if e != nil {
				t.Fatal(e)
			}
			unused, e := w.save(page{Items: []item{{Key: "g/" + strings.Repeat("f", 40), Value: bytes.Repeat([]byte{'x'}, 64<<10)}}})
			if e != nil {
				t.Fatal(e)
			}
			root, e := w.save(page{Children: []edge{leaf, unused}})
			if e != nil {
				t.Fatal(e)
			}
			if e = w.flush(); e != nil {
				t.Fatal(e)
			}
			trace := &recipeByteStore{Store: backend}
			p := &Progressive{store: trace, cache: newCache(1 << 20), root: root.ID}
			if warm {
				raw, _, e := backend.Get(t.Context(), root.ID.Pack, 0, -1)
				if e != nil {
					t.Fatal(e)
				}
				p.cache.put("index-container/"+root.ID.Pack, raw)
			}
			for range 2 {
				size, e := p.ObjectSize(t.Context(), oid)
				if e != nil || size != want.Size {
					t.Fatalf("object size %d: %v", size, e)
				}
				got, known, e := p.objectRecipe(t.Context(), oid)
				if e != nil || !known || !proto.Equal(got, want) {
					t.Fatalf("recipe differs: %+v, %v", got, e)
				}
			}
			maxBytes := root.ID.Length + leaf.ID.Length
			if warm {
				maxBytes = 0
			}
			if trace.bytes > maxBytes {
				t.Fatalf("read %d bytes; need only %d bytes of index pages", trace.bytes, maxBytes)
			}
			// A cold narrow read must validate its own hash, just like a page
			// extracted from a whole container. Do not reuse the valid cache.
			raw, _, e := backend.Get(t.Context(), leaf.ID.Pack, 0, -1)
			if e != nil {
				t.Fatal(e)
			}
			raw[leaf.ID.Offset] ^= 1
			if e = backend.Put(t.Context(), leaf.ID.Pack, raw, ""); e != nil {
				t.Fatal(e)
			}
			p.cache = newCache(1 << 20)
			if _, _, e = p.objectRecipe(t.Context(), oid); e == nil || !strings.Contains(e.Error(), "checksum mismatch") {
				t.Fatalf("corrupt narrow page accepted: %v", e)
			}
		})
	}
}
