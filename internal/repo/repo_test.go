package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"gat/internal/store"
	bolt "go.etcd.io/bbolt"
)

func command(t *testing.T, dir string, args ...string) string {
	t.Helper()
	c := exec.Command("git", append([]string{"-C", dir}, args...)...)
	c.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
	b, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, b)
	}
	return strings.TrimSpace(string(b))
}
func write(t *testing.T, dir, name string, b []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), b, 0644); err != nil {
		t.Fatal(err)
	}
}
func commit(t *testing.T, dir string) string {
	t.Helper()
	command(t, dir, "add", ".")
	command(t, dir, "commit", "-qm", "fixture")
	return command(t, dir, "rev-parse", "HEAD")
}

type countedStore struct {
	store.Store
	mu                    sync.Mutex
	gets, packGets, bytes int
	failHead              bool
}

func (s *countedStore) Get(ctx context.Context, k string, o, n int64) ([]byte, string, error) {
	b, v, e := s.Store.Get(ctx, k, o, n)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	s.bytes += len(b)
	if strings.HasPrefix(k, "packs/") {
		s.packGets++
	}
	return b, v, e
}
func (s *countedStore) Put(ctx context.Context, k string, b []byte, c string) error {
	if k == "HEAD" && s.failHead {
		return errors.New("injected publication failure")
	}
	return s.Store.Put(ctx, k, b, c)
}
func (s *countedStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets = 0
	s.packGets = 0
	s.bytes = 0
}

func TestImportLazySnapshotsAndUpdates(t *testing.T) {
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			command(t, dir, "init", "-q", "--object-format="+format)
			large := make([]byte, 3*ChunkSize+123)
			rand.New(rand.NewSource(4)).Read(large)
			write(t, dir, "large", large)
			write(t, dir, "small", []byte("old"))
			write(t, dir, "empty", nil)
			write(t, dir, "sub/script", []byte("#!/bin/sh\n"))
			os.Chmod(filepath.Join(dir, "sub/script"), 0755)
			if err := os.Symlink("small", filepath.Join(dir, "link")); err != nil {
				t.Fatal(err)
			}
			weird := "raw-\xff"
			if runtime.GOOS == "darwin" {
				weird = "raw-ü"
			}
			write(t, dir, weird, []byte("non-UTF8 name"))
			first := commit(t, dir)
			local, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			s := &countedStore{Store: local}
			stats, err := Import(ctx, s, ImportOptions{Repo: dir})
			if err != nil {
				t.Fatal(err)
			}
			if stats.Blobs < 6 {
				t.Fatal(stats)
			}
			r, _ := New(s, 2*ChunkSize)
			s.reset()
			old, err := r.Open(ctx, first)
			if err != nil {
				t.Fatal(err)
			}
			entries, err := old.ReadDir(ctx, old.Tree, "", 128)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 6 || s.packGets != 0 || s.gets > 8 {
				t.Fatalf("root listing should fetch only a few metadata pages: entries=%d gets=%d packs=%d", len(entries), s.gets, s.packGets)
			}
			if _, err := old.Resolve(ctx, weird); err != nil {
				t.Fatal(err)
			}
			script, err := old.Resolve(ctx, "sub/script")
			if err != nil || script.Mode != 0100755 {
				t.Fatalf("executable mode: %v %v", script, err)
			}
			e, err := old.Resolve(ctx, "large")
			if err != nil {
				t.Fatal(err)
			}
			s.reset()
			b := make([]byte, 50)
			n, err := old.ReadAt(ctx, e.OID, b, ChunkSize-20)
			if err != nil || n != len(b) || !bytes.Equal(b, large[ChunkSize-20:ChunkSize+30]) {
				t.Fatalf("cross-chunk range: %d %v", n, err)
			}
			if s.packGets != 2 || s.bytes > 2*ChunkSize+(128<<10) {
				t.Fatalf("range read downloaded too much: packs=%d bytes=%d", s.packGets, s.bytes)
			}
			b = make([]byte, 200)
			n, err = old.ReadAt(ctx, e.OID, b, int64(len(large)-10))
			if n != 10 || err != io.EOF || !bytes.Equal(b[:n], large[len(large)-10:]) {
				t.Fatalf("EOF: %d %v", n, err)
			}
			write(t, dir, "small", []byte("new data"))
			write(t, dir, "new", []byte("added"))
			second := commit(t, dir)
			var wg sync.WaitGroup
			wg.Go(func() {
				for i := 0; i < 40; i++ {
					e, err := old.Resolve(ctx, "small")
					if err != nil {
						t.Error(err)
						return
					}
					b := make([]byte, 3)
					_, err = old.ReadAt(ctx, e.OID, b, 0)
					if err != nil || string(b) != "old" {
						t.Errorf("old reader changed: %q %v", b, err)
						return
					}
				}
			})
			stats, err = Import(ctx, s, ImportOptions{Repo: dir})
			wg.Wait()
			if err != nil {
				t.Fatal(err)
			}
			if stats.Blobs != 2 {
				t.Fatalf("incremental import should upload only new blobs: %+v", stats)
			}
			current, err := r.Open(ctx, second)
			if err != nil {
				t.Fatal(err)
			}
			e, err = current.Resolve(ctx, "small")
			if err != nil {
				t.Fatal(err)
			}
			b = make([]byte, 8)
			_, err = current.ReadAt(ctx, e.OID, b, 0)
			if err != nil || string(b) != "new data" {
				t.Fatalf("new snapshot: %q %v", b, err)
			}
			if _, err := old.Resolve(ctx, "new"); !IsNotFound(err) {
				t.Fatalf("old tree changed: %v", err)
			}
			if _, err := r.Open(ctx, first); err != nil {
				t.Fatal("old SHA lost", err)
			}
			stats, err = Import(ctx, s, ImportOptions{Repo: dir})
			if err != nil || stats.Objects != 0 {
				t.Fatalf("repeat import: %+v %v", stats, err)
			}
			write(t, dir, "small", []byte("unpublished"))
			third := commit(t, dir)
			s.failHead = true
			if _, err := Import(ctx, s, ImportOptions{Repo: dir}); err == nil {
				t.Fatal("expected publication failure")
			}
			s.failHead = false
			if _, err := r.Open(ctx, third); !IsNotFound(err) {
				t.Fatalf("failed publication leaked: %v", err)
			}
			if _, err := r.Open(ctx, second); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPersistentIndexAndPagedDirectory(t *testing.T) {
	ctx := context.Background()
	local, _ := store.NewLocal(t.TempDir())
	s := &countedStore{Store: local}
	idx := &index{store: s, cache: newCache(64 << 10)}
	db, err := bolt.Open(filepath.Join(t.TempDir(), "test.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	update := func(values map[string]int) {
		t.Helper()
		err := db.Update(func(tx *bolt.Tx) error {
			_ = tx.DeleteBucket([]byte("delta"))
			b, err := tx.CreateBucket([]byte("delta"))
			if err != nil {
				return err
			}
			for k, v := range values {
				raw, err := marshal(object{Kind: "blob", Size: int64(v)})
				if err != nil {
					return err
				}
				if err := b.Put([]byte(k), raw); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		err = db.View(func(tx *bolt.Tx) error {
			var err error
			idx.root, err = idx.update(ctx, tx.Bucket([]byte("delta")))
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	want := map[string]int{}
	for i := 0; i < 20000; i++ {
		want[fmt.Sprintf("t/tree/%08d", i*2)] = i
	}
	update(want)
	original := *idx
	delta := map[string]int{}
	for i := 0; i < 300; i++ {
		delta[fmt.Sprintf("t/tree/%08d", i*131)] = -i
	}
	delta["a/before"] = 1
	delta["z/after"] = 2
	update(delta)
	for k, v := range delta {
		want[k] = v
	}
	for k, v := range want {
		var got object
		if err := idx.get(ctx, k, &got); err != nil || got.Size != int64(v) {
			t.Fatalf("%s: %d want %d: %v", k, got.Size, v, err)
		}
	}
	var old object
	if err := original.get(ctx, "t/tree/00000000", &old); err != nil || old.Size != 0 {
		t.Fatal(old, err)
	}
	if err := original.get(ctx, "z/after", &old); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("old root modified", err)
	}
	after := ""
	count := 0
	for {
		batch, err := idx.scan(ctx, "t/tree/", after, 73)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range batch {
			if item.Key <= after {
				t.Fatal("unordered scan")
			}
			after = item.Key
			count++
		}
		if len(batch) < 73 {
			break
		}
	}
	if count != len(want)-2 {
		t.Fatalf("paged scan got %d want %d", count, len(want)-2)
	}
	if idx.cache.used > idx.cache.max {
		t.Fatal("cache exceeded budget")
	}
}

func TestCacheBudgetAndChunkCorruption(t *testing.T) {
	c := newCache(1024)
	for i := 0; i < 100; i++ {
		c.put(fmt.Sprint(i), make([]byte, 100))
	}
	if c.used > 1024 {
		t.Fatal(c.used)
	}
	c.put("huge", make([]byte, 2048))
	if _, ok := c.get("huge"); ok {
		t.Fatal("oversized cached entry")
	}
	ctx := context.Background()
	dir := t.TempDir()
	command(t, dir, "init", "-q")
	write(t, dir, "a", []byte("payload"))
	sha := commit(t, dir)
	root := t.TempDir()
	s, _ := store.NewLocal(root)
	if _, err := Import(ctx, s, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	r, _ := New(s, 0)
	snapshot, err := r.Open(ctx, sha)
	if err != nil {
		t.Fatal(err)
	}
	e, _ := snapshot.Resolve(ctx, "a")
	var ch chunk
	if err := snapshot.idx.get(ctx, chunkKey(e.OID, 0), &ch); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(root, ch.Pack), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte{0, 0, 0, 0}, ch.Offset)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.ReadAt(ctx, e.OID, make([]byte, 7), 0); err == nil {
		t.Fatal("corrupt content accepted")
	}
}
