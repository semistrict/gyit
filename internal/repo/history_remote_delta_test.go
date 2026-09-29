//go:build !js

package repo

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
)

type deltaReadStore struct {
	store.Store
	reads atomic.Int64
}

func (s *deltaReadStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if strings.HasPrefix(key, "packs/") {
		s.reads.Add(1)
	}
	return s.Store.Get(ctx, key, off, n)
}

func remoteDeltaFixture(t *testing.T) (*Progressive, *deltaReadStore, *pb.ProgressiveObject, *pb.ProgressiveObject, string, []byte) {
	t.Helper()
	p, local, oid, want, _ := historyRecipeFixture(t)
	source := local.Value(historySourceKey{}).(*historySource)
	original, _, err := source.locate(oid)
	if err != nil {
		t.Fatal(err)
	}
	raw := source.packs[0].data
	_, _, _, baseID, err := progressiveHeader(bytes.NewReader(raw[original.Offset:]), original.Offset)
	if err != nil {
		t.Fatal(err)
	}
	base, _, err := source.locate(baseID)
	if err != nil {
		t.Fatal(err)
	}
	// Two independently located deltas share the same remote base. Duplicating
	// the tiny encoded delta makes this test independent of Git's pack choices.
	pack := bytes.Clone(raw[:len(raw)-20])
	second := &pb.ProgressiveObject{Pack: original.Pack, Offset: int64(len(pack)), Size: int64(len(want))}
	pack = append(pack, raw[original.Offset:len(raw)-20]...)
	pack = append(pack, make([]byte, 20)...)
	original.PackSize, base.PackSize, second.PackSize = int64(len(pack)), int64(len(pack)), int64(len(pack))
	original.Size, base.Size = int64(len(want)), int64(len(want)-1)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	trace := &deltaReadStore{Store: backend}
	p.store = trace
	p.cache = newCache(1 << 20)
	if err := trace.Put(t.Context(), progressivePackKey(original.Pack, 0), pack, ""); err != nil {
		t.Fatal(err)
	}
	w := &indexWriter{ctx: t.Context(), store: trace, prefix: "remote-delta"}
	var items []item
	for id, recipe := range map[string]*pb.ProgressiveObject{oid: original, baseID: base} {
		value, err := marshal(recipe)
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, item{Key: "g/" + id, Value: value})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	e, err := w.save(page{Items: items})
	if err != nil {
		t.Fatal(err)
	}
	if err = w.flush(); err != nil {
		t.Fatal(err)
	}
	p.root = e.ID
	return p, trace, original, second, oid, want
}

func TestRemoteDeltaBaseReuse(t *testing.T) {
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprint(disk), func(t *testing.T) {
			p, trace, first, second, _, want := remoteDeltaFixture(t)
			if disk {
				c, e := store.NewDiskCache(nil, t.TempDir(), "delta-test", 32<<20)
				if e != nil {
					t.Fatal(e)
				}
				defer c.Close()
				p.cache.disk = c
			}
			for i, recipe := range []*pb.ProgressiveObject{first, second} {
				budget := int64(256 << 20)
				got, local, err := p.decodeObject(t.Context(), recipe, 0, &budget)
				if err != nil || local || len(got) == 0 || !bytes.Equal(got[1:], want) {
					t.Fatalf("decode %d: local=%v err=%v", i, local, err)
				}
				if reads := trace.reads.Load(); reads != int64(i+2) {
					t.Fatalf("decode %d fetched %d packed members; want %d (shared base read once)", i, reads, i+2)
				}
			}
			// A cached base still consumes the caller's decoding budget.
			budget := int64(len(want) - 2)
			if _, _, err := p.decodeObject(t.Context(), second, 0, &budget); err == nil || !strings.Contains(err.Error(), "bounded working memory") {
				t.Fatalf("cached base bypassed memory budget: %v", err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			budget = 256 << 20
			before := trace.reads.Load()
			if _, _, err := p.decodeObject(ctx, second, 0, &budget); err != context.Canceled {
				t.Fatalf("canceled decoder: %v", err)
			}
			if trace.reads.Load() != before {
				t.Fatal("canceled decoder fetched data")
			}
		})
	}
}

func TestRemoteDeltaCacheStillChecksIdentity(t *testing.T) {
	p, _, recipe, _, oid, want := remoteDeltaFixture(t)
	budget := int64(256 << 20)
	if _, _, err := p.decodeObject(t.Context(), recipe, 0, &budget); err != nil {
		t.Fatal(err)
	}
	// Corrupt the already decoded base while preserving length and type. The
	// final object's OID check must reject it, even on an offset-cache hit.
	p.cache.mu.Lock()
	changed := false
	for key, e := range p.cache.items {
		if strings.HasPrefix(key, "progressive-delta/") {
			raw := e.Value.(cached).data
			if len(raw) == len(want) {
				raw[1] ^= 1
				changed = true
			}
		}
	}
	p.cache.mu.Unlock()
	if !changed {
		t.Fatal("shared base was not cached")
	}
	_, release, err := p.borrowObject(t.Context(), oid)
	release()
	if err == nil || !strings.Contains(err.Error(), "object identity mismatch") {
		t.Fatalf("corrupt decoded base accepted: %v", err)
	}
}
