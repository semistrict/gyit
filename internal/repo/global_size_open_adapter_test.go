package repo

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"testing"

	"gyit/internal/store"
)

func globalOpenIndex(t *testing.T, backend store.Store, prefix string, records map[string]any) pageRef {
	t.Helper()
	var keys []string
	for key := range records {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var items []item
	for _, key := range keys {
		b, err := marshal(records[key])
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, item{Key: key, Value: b})
	}
	writer := &indexWriter{ctx: t.Context(), store: backend, prefix: prefix}
	edge, err := writer.save(page{Items: items})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	return edge.ID
}

func globalOpenFixture(t *testing.T, backend store.Store, ordinal int, size uint32) (manifest, string, Entry) {
	t.Helper()
	ctx := t.Context()
	var id [20]byte
	id[19] = byte(ordinal + 1)
	ref := globalTestRef(t, backend, fmt.Sprintf("index/global-sizes-open-%d", ordinal), globalTestData(t, id, size))
	raw := append([]byte("100644 file\x00"), id[:]...)
	tree, directory := lazyTreeFixtureDescriptor(t, ctx, backend, raw)
	commit := fmt.Sprintf("%040x", ordinal+1)
	records := map[string]any{"o/" + commit: object{Kind: "commit", Tree: tree}, "o/" + tree: object{Kind: "tree", Size: int64(len(raw)), Directory: directory}, "x/global-sizes": chunk{Pack: ref.Key, Hash: ref.Hash, Length: ref.Length}}
	root := globalOpenIndex(t, backend, fmt.Sprintf("global-open-main-%d", ordinal), records)
	m := manifest{Version: globalReaderFormat, Format: "sha1", Root: root}
	return m, commit, Entry{Name: "file", OID: hex.EncodeToString(id[:]), Mode: 0100644, Size: int64(size)}
}
func globalPublishManifest(t *testing.T, backend store.Store, m manifest) {
	t.Helper()
	b, err := marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Put(t.Context(), "HEAD", b, ""); err != nil {
		t.Fatal(err)
	}
}

func TestGlobalOpenPinsTableAcrossPublications(t *testing.T) {
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first, sha1, want1 := globalOpenFixture(t, backend, 0, 11)
	second, sha2, want2 := globalOpenFixture(t, backend, 1, 22)
	r, err := New(backend, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	globalPublishManifest(t, backend, first)
	a, err := r.Open(t.Context(), sha1)
	if err != nil {
		t.Fatal(err)
	}
	globalPublishManifest(t, backend, second)
	b, err := r.Open(t.Context(), sha2)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range []struct {
		s *Snapshot
		e Entry
	}{{a, want1}, {b, want2}, {a, want1}} {
		got, err := pair.s.Lookup(t.Context(), pair.s.Tree, "file")
		if err != nil || got != pair.e {
			t.Fatal("pinned generation mismatch", got, pair.e, err)
		}
		derived := &Snapshot{idx: pair.s.idx, Tree: pair.s.Tree}
		got, err = derived.Resolve(t.Context(), "file")
		if err != nil || got != pair.e {
			t.Fatal("derived snapshot lost table binding", got, err)
		}
		at, err := pair.s.atCommit(t.Context(), pair.s.SHA)
		if err != nil {
			t.Fatal(err)
		}
		got, err = at.Resolve(t.Context(), "file")
		if err != nil || got != pair.e {
			t.Fatal("history snapshot lost binding", got, err)
		}
	}
	if a.idx.cache != b.idx.cache || a.idx.cache != r.cache || r.cache.max != globalOrdinaryBudget || r.globalSizes.peak.Load() > globalTableBudget {
		t.Fatal("split cache generations")
	}
	if a.idx.globalSizeRef == b.idx.globalSizeRef || a.idx.globalSizes != b.idx.globalSizes {
		t.Fatal("immutable reference/shared slot mismatch")
	}
	// Revision construction must bind the root it received, regardless of HEAD.
	old, err := r.openRevision(t.Context(), first, sha1, "")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := old.Resolve(t.Context(), "file"); err != nil || got != want1 {
		t.Fatal("revision binding", got, err)
	}
}

func TestGlobalOpenHonorsCacheBudgetAndLegacy(t *testing.T) {
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m, sha, _ := globalOpenFixture(t, backend, 2, 33)
	globalPublishManifest(t, backend, m)
	small, err := New(backend, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := small.Open(t.Context(), sha); err == nil || !strings.Contains(err.Error(), "configured cache") {
		t.Fatal("small cache silently enlarged", err)
	}
	if small.cache.max != 8<<20 || small.globalSizes != nil {
		t.Fatal("small cache mutated")
	}
	m.Version = 9012
	globalPublishManifest(t, backend, m)
	legacy, err := New(backend, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	s, err := legacy.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	slot, _ := s.globalBinding()
	if slot != nil || legacy.globalSizes != nil || legacy.cache.max != DefaultCacheBytes {
		t.Fatal("legacy path activated table")
	}
}

func TestGlobalOpenPointerValidation(t *testing.T) {
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r, err := New(backend, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range []chunk{{Pack: "packs/wrong", Hash: strings.Repeat("0", 64)}, {Pack: "index/global-sizes-bad", Hash: strings.Repeat("0", 64), Base: &chunkBase{}}, {Pack: "index/global-sizes-bad", Hash: strings.Repeat("A", 64)}, {Pack: "index/global-sizes-bad", Hash: strings.Repeat("0", 64), Length: globalTableBudget}} {
		root := globalOpenIndex(t, backend, fmt.Sprintf("global-open-pointer-%d", i), map[string]any{"x/global-sizes": c})
		idx := &index{store: backend, cache: r.cache, root: root}
		if err := r.bindGlobalIndex(t.Context(), manifest{Version: globalReaderFormat, Format: "sha1", Root: root}, idx); err == nil {
			t.Fatalf("bad pointer%d accepted", i)
		}
	}
}
