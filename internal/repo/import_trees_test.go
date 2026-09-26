package repo

import (
	"bytes"
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gyit/internal/store"
)

type parallelDirectoryStore struct {
	store.Store
	mu              sync.Mutex
	active, arrived int
	second          chan struct{}
	failure         error
}

func (s *parallelDirectoryStore) Put(ctx context.Context, key string, data []byte, condition string) error {
	if !strings.HasPrefix(key, "index/dirs-") {
		return s.Store.Put(ctx, key, data, condition)
	}
	s.mu.Lock()
	s.arrived++
	ticket := s.arrived
	s.active++
	if ticket == 2 {
		close(s.second)
	}
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.active--; s.mu.Unlock() }()
	select {
	case <-s.second:
	case <-ctx.Done():
		return ctx.Err()
	}
	if s.failure != nil {
		if ticket == 1 {
			return s.failure
		}
		<-ctx.Done()
		return ctx.Err()
	}
	return s.Store.Put(ctx, key, data, condition)
}

func TestImportParallelDirectoryPacks(t *testing.T) {
	for _, preload := range []bool{false, true} {
		name := "normal"
		if preload {
			name = "preloaded"
		}
		t.Run(name, func(t *testing.T) {
			importer := Import
			if preload {
				importer = func(ctx context.Context, backend store.Store, opt ImportOptions) (Stats, error) {
					return importWithMetadataThreshold(ctx, backend, opt, 0)
				}
			}

			source, sha := directoryHistory(t, 512, 512)
			local, e := store.NewLocal(t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			backend := &parallelDirectoryStore{Store: local, second: make(chan struct{})}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			tmp := t.TempDir()
			if _, e = importer(ctx, backend, ImportOptions{Repo: source, DeltaDepth: 1, TempDir: tmp, CompressionWorkers: 4}); e != nil {
				t.Fatal(e)
			}
			backend.mu.Lock()
			active, arrived := backend.active, backend.arrived
			backend.mu.Unlock()
			if active != 0 || arrived < 2 {
				t.Fatalf("directory uploads not joined: active=%d arrived=%d", active, arrived)
			}
			r, _ := New(local, 64<<10)
			for _, rev := range []string{sha, "HEAD~255", "HEAD~511"} {
				oid := command(t, source, "rev-parse", rev)
				snap, e := r.Open(t.Context(), oid)
				if e != nil {
					t.Fatal(e)
				}
				checkObjectViewParity(t, t.Context(), r, snap, source, "", []string{"cat-file", "tree", snap.Tree})
				checkObjectViewParity(t, t.Context(), r, snap, source, "", []string{"ls-tree", "-l", oid})
			}
			if files, e := os.ReadDir(tmp); e != nil || len(files) != 0 {
				t.Fatalf("scratch remains: %v %v", files, e)
			}
		})
	}
}

func TestImportDirectoryFailureJoinsWriters(t *testing.T) {
	for _, preload := range []bool{false, true} {
		name := "normal"
		if preload {
			name = "preloaded"
		}
		t.Run(name, func(t *testing.T) {
			importer := Import
			if preload {
				importer = func(ctx context.Context, backend store.Store, opt ImportOptions) (Stats, error) {
					return importWithMetadataThreshold(ctx, backend, opt, 0)
				}
			}

			source, _ := directoryHistory(t, 512, 512)
			local, e := store.NewLocal(t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			failure := errors.New("injected directory upload failure")
			backend := &parallelDirectoryStore{Store: local, second: make(chan struct{}), failure: failure}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			tmp := t.TempDir()
			_, e = importer(ctx, backend, ImportOptions{Repo: source, DeltaDepth: 1, TempDir: tmp, CompressionWorkers: 4})
			if !errors.Is(e, failure) {
				t.Fatalf("directory error lost: %v", e)
			}
			backend.mu.Lock()
			active := backend.active
			backend.mu.Unlock()
			if active != 0 {
				t.Fatalf("writers remain active: %d", active)
			}
			if _, _, e = local.Get(t.Context(), "HEAD", 0, -1); !errors.Is(e, store.ErrNotFound) {
				t.Fatalf("failed import published: %v", e)
			}
			if files, e := os.ReadDir(tmp); e != nil || len(files) != 0 {
				t.Fatalf("scratch remains: %v %v", files, e)
			}
		})
	}
}

func TestImportParallelTreesVerifySource(t *testing.T) {
	for _, preload := range []bool{false, true} {
		name := "normal"
		if preload {
			name = "preloaded"
		}
		t.Run(name, func(t *testing.T) {
			importer := Import
			if preload {
				importer = func(ctx context.Context, backend store.Store, opt ImportOptions) (Stats, error) {
					return importWithMetadataThreshold(ctx, backend, opt, 0)
				}
			}

			source := t.TempDir()
			command(t, source, "init", "-q")
			write(t, source, "a", []byte("data\n"))
			commit(t, source)
			oid := command(t, source, "rev-parse", "HEAD^{tree}")
			raw, e := git(t.Context(), source, "cat-file", "tree", oid).Output()
			if e != nil {
				t.Fatal(e)
			}
			changed := bytes.Replace(raw, []byte(" a\x00"), []byte(" b\x00"), 1)
			if bytes.Equal(changed, raw) {
				t.Fatal("fixture tree name missing")
			}
			var corrupted bytes.Buffer
			z := zlib.NewWriter(&corrupted)
			fmt.Fprintf(z, "tree %d\x00", len(changed))
			z.Write(changed)
			if e = z.Close(); e != nil {
				t.Fatal(e)
			}
			objects := command(t, source, "rev-parse", "--path-format=absolute", "--git-path", "objects")
			local, e := store.NewLocal(t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			var once sync.Once
			var mutationErr error
			_, e = importer(t.Context(), local, ImportOptions{Repo: source, DeltaDepth: 1, CompressionWorkers: 4, Progress: func(stats Stats) {
				if stats.Phase == "objects" {
					once.Do(func() {
						path := filepath.Join(objects, oid[:2], oid[2:])
						mutationErr = os.Chmod(path, 0600)
						if mutationErr == nil {
							mutationErr = os.WriteFile(path, corrupted.Bytes(), 0600)
						}
					})
				}
			}})
			if mutationErr != nil {
				t.Fatal(mutationErr)
			}
			if e == nil || !strings.Contains(e.Error(), "checksum") {
				t.Fatalf("corrupt tree accepted or checksum error lost: %v", e)
			}
			if _, _, e = local.Get(t.Context(), "HEAD", 0, -1); !errors.Is(e, store.ErrNotFound) {
				t.Fatalf("corrupt tree published: %v", e)
			}
		})
	}
}

func BenchmarkImportDirectoryWorkers(b *testing.B) {
	source, _ := directoryHistory(b, 1024, 2048)
	for _, workers := range []int{1, 8} {
		b.Run(fmt.Sprintf("workers-%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				root, e := os.MkdirTemp(b.TempDir(), "store-*")
				if e != nil {
					b.Fatal(e)
				}
				local, e := store.NewLocal(root)
				if e != nil {
					b.Fatal(e)
				}
				b.StartTimer()
				start := time.Now()
				_, e = Import(b.Context(), local, ImportOptions{Repo: source, DeltaDepth: 1, CompressionWorkers: workers})
				elapsed := time.Since(start)
				b.StopTimer()
				if e != nil {
					b.Fatal(e)
				}
				if elapsed > time.Second {
					b.Logf("SLOW >1s: %.6fs", elapsed.Seconds())
				}
				b.ReportMetric(float64(directoryStoreBytes(b, root)), "store-bytes")
				if e = os.RemoveAll(root); e != nil {
					b.Fatal(e)
				}
				b.StartTimer()
			}
		})
	}
}

// Writers sharing a directory prefix must retain every earlier range when
// their packs roll over, including the foreground writer's reserved sequence.
func TestDirectoryPackRolloverPreservesRanges(t *testing.T) {
	local, e := store.NewLocal(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	writers := []*indexWriter{
		{ctx: t.Context(), store: local, prefix: "dirs-shared", number: 0, step: 3},
		{ctx: t.Context(), store: local, prefix: "dirs-shared", number: 1, step: 3},
		{ctx: t.Context(), store: local, prefix: "dirs-shared", number: 2, step: 3},
	}
	type saved struct {
		ref   pageRef
		value byte
	}
	var ranges []saved
	for round := 0; round < 2; round++ {
		for i, w := range writers {
			value := byte(1 + round*len(writers) + i)
			ref, e := w.saveBytes(bytes.Repeat([]byte{value}, indexPackSize))
			if e != nil {
				t.Fatal(e)
			}
			ranges = append(ranges, saved{ref, value})
		}
	}
	for _, w := range writers {
		if e = w.flush(); e != nil {
			t.Fatal(e)
		}
	}
	for _, saved := range ranges {
		data, _, e := local.Get(t.Context(), saved.ref.Pack, saved.ref.Offset, saved.ref.Length)
		if e != nil || !bytes.Equal(data, bytes.Repeat([]byte{saved.value}, indexPackSize)) {
			t.Fatalf("earlier directory range overwritten: %+v %v", saved.ref, e)
		}
	}
}
