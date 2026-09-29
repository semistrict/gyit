package repo

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"gyit/internal/store"
)

type historyCacheReadStore struct {
	store.Store
	reads atomic.Int64
}

func TestHistoryWriterCacheExpansionFallback(t *testing.T) {
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	backend := &historyCacheReadStore{Store: local}
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p.cache = newCache(32 << 20)
	enc, err := newCompressor()
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	w := p.historyWriter(t.Context(), backend, "progressive-history-v2-expansion")
	raw := bytes.Repeat([]byte("x"), 64<<10)
	var first pageRef
	for i := range 70 {
		ref, err := w.saveBytes(enc.EncodeAll(raw, nil))
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = ref
		}
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.cache.get("history-container/" + first.Pack); ok {
		t.Fatal("writer cached a container exceeding the decoded expansion bound")
	}
	got, err := p.historyBytes(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatal("bounded fallback changed frame bytes")
	}
	if backend.reads.Load() == 0 {
		t.Fatal("fallback did not read durable data")
	}
}

func (s *historyCacheReadStore) Get(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	if strings.HasPrefix(key, "index/progressive-history-") {
		s.reads.Add(1)
	}
	return s.Store.Get(ctx, key, off, length)
}

func TestHistoryWriterSeedsBoundedCache(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for i := range 12 {
		write(t, dir, "file", []byte(fmt.Sprint(i)))
		commit(t, dir)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	want := command(t, dir, "log", "--format=%H", "--", "file")
	command(t, dir, "repack", "-ad")
	for _, mode := range []string{"memory", "disk", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			local, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			backend := &historyCacheReadStore{Store: local}
			p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if mode == "disk" {
				p = directoryReader(t, backend, t.TempDir(), 32<<20)
			} else if mode == "memory" {
				p.cache = newCache(32 << 20)
			}
			if err := p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
				t.Fatal(err)
			}
			if err := p.IngestHistory(t.Context(), sha, filepath.Join(dir, ".git")); err != nil {
				t.Fatal(err)
			}
			read := func(reader *Progressive) {
				t.Helper()
				snapshot, err := reader.Open(t.Context(), sha)
				if err != nil {
					t.Fatal(err)
				}
				var got []string
				err = snapshot.LogWithOptions(t.Context(), LogOptions{Count: 100, Paths: []string{"file"}, FullCommitIDs: true}, func(e LogEntry) error { got = append(got, e.SHA); return nil })
				if err != nil {
					t.Fatal(err)
				}
				if strings.Join(got, "\n") != want {
					t.Fatalf("history mismatch: %v", got)
				}
			}
			backend.reads.Store(0)
			read(p)
			if got := backend.reads.Load(); mode != "disabled" && got != 0 {
				t.Fatalf("first query downloaded %d newly written history containers", got)
			}
			// The cache is disposable: another reader must recover the same
			// history solely from the published durable objects.
			cold, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			backend.reads.Store(0)
			read(cold)
			if backend.reads.Load() == 0 {
				t.Fatal("uncached reader did not read durable history")
			}
		})
	}
}
