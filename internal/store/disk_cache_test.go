package store

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestDiskCachePersistsRanges(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	origin, err := NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := origin.Put(ctx, "packs/one", []byte("0123456789"), ""); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	c, err := testDiskCache(origin, dir, "repository-one", 8192)
	if err != nil {
		t.Fatal(err)
	}
	b, release, err := Acquire(ctx, c, "packs/one", 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, []byte("2345")) {
		t.Fatalf("read %q", b)
	}
	release()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "packs/one")); err != nil {
		t.Fatal(err)
	}
	c, err = testDiskCache(origin, dir, "repository-one", 8192)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	b, release, err = Acquire(ctx, c, "packs/one", 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if string(b) != "2345" {
		t.Fatalf("cached read %q", b)
	}
}

func TestDiskCacheEvictionPinsAndPublication(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	origin, _ := NewLocal(root)
	for _, key := range []string{"one", "two", "three", "HEAD"} {
		if err := origin.Put(ctx, key, []byte(key+"-payload"), ""); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	c, err := testDiskCache(origin, dir, "repo", 8192)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	first, release, err := Acquire(ctx, c, "one", 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for _, key := range []string{"two", "three"} {
		b, done, err := Acquire(ctx, c, key, 0, 3)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != key[:3] {
			t.Fatalf("read %q", b)
		}
		done()
	}
	if string(first) != "one" {
		t.Fatalf("eviction changed pinned bytes: %q", first)
	}
	if err := os.Remove(filepath.Join(root, "two")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Get(ctx, "two", 0, 3); !errors.Is(err, ErrNotFound) {
		t.Fatalf("evicted range unexpectedly available: %v", err)
	}
	head, token, err := c.Get(ctx, "HEAD", 0, -1)
	if err != nil || string(head) != "HEAD-payload" || token == "" {
		t.Fatalf("HEAD: %q %q %v", head, token, err)
	}
	if _, _, err := c.Get(ctx, "HEAD", 0, 3); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "HEAD", []byte("new-payload"), token); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "HEAD", []byte("wrong"), token); !errors.Is(err, ErrConflict) {
		t.Fatalf("CAS: %v", err)
	}
	head, _, err = c.Get(ctx, "HEAD", 0, 3)
	if err != nil || string(head) != "new" {
		t.Fatalf("stale publication: %q %v", head, err)
	}
	var allocated int64
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || d.Name() == "lock" {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		allocated += (info.Size() + 4095) / 4096 * 4096
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if allocated > 8192 {
		t.Fatalf("disk budget exceeded: %d", allocated)
	}
	c.Close()
	if string(first) != "one" {
		t.Fatal("Close invalidated live lease")
	}
	if other, err := testDiskCache(origin, dir, "repo", 8192); err == nil {
		other.Close()
		t.Fatal("owner lock released before live lease")
	}
	release()
	other, err := testDiskCache(origin, dir, "repo", 4096)
	if err != nil {
		t.Fatal(err)
	}
	other.Close()
}

func TestDiskCacheIsolationConcurrencyAndGetOwnership(t *testing.T) {
	ctx := t.Context()
	origin, _ := NewLocal(t.TempDir())
	if err := origin.Put(ctx, "pack", []byte("original"), ""); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	c, err := testDiskCache(origin, dir, "first", 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for range 10 {
				b, done, err := Acquire(ctx, c, "pack", 0, 8)
				if err != nil {
					t.Error(err)
					return
				}
				if string(b) != "original" {
					t.Errorf("bad range %q", b)
				}
				done()
			}
		})
	}
	wg.Wait()
	b, _, err := c.Get(ctx, "pack", 0, 8)
	if err != nil {
		t.Fatal(err)
	}
	b[0] = 'X'
	b, _, err = c.Get(ctx, "pack", 0, 8)
	if err != nil || string(b) != "original" {
		t.Fatalf("Get leaked mutable cache bytes: %q %v", b, err)
	}
	second, _ := NewLocal(t.TempDir())
	if err := second.Put(ctx, "pack", []byte("distinct"), ""); err != nil {
		t.Fatal(err)
	}
	isolated, err := testDiskCache(second, dir, "second", 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer isolated.Close()
	b, _, err = isolated.Get(ctx, "pack", 0, 8)
	if err != nil || string(b) != "distinct" {
		t.Fatalf("cross-repository cache collision: %q %v", b, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := c.Get(cancelled, "pack", 0, 8); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
}

func BenchmarkDiskCacheDecodedHit(b *testing.B) {
	origin, _ := NewLocal(b.TempDir())
	data := bytes.Repeat([]byte("compressed bytes"), 4096)
	if err := origin.Put(b.Context(), "pack", data, ""); err != nil {
		b.Fatal(err)
	}
	c, err := testDiskCache(origin, b.TempDir(), "benchmark", 1<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	_, done, err := c.Load(b.Context(), "decoded", func() ([]byte, error) { return data, nil })
	if err != nil {
		b.Fatal(err)
	}
	done()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		p, release, err := c.Load(b.Context(), "decoded", func() ([]byte, error) { return data, nil })
		if err != nil {
			b.Fatal(err)
		}
		if p[0] != 'c' {
			b.Fatal("wrong bytes")
		}
		release()
	}
}

func testDiskCache(s Store, dir, identity string, max int64) (*DiskCache, error) {
	return newDiskCache(s, dir, identity, max, func() (int64, int64, error) { return 100 << 30, 50 << 30, nil })
}

func TestDecodedDiskCacheStoresUncompressedBytes(t *testing.T) {
	dir := t.TempDir()
	c, err := testDiskCache(nil, dir, "decoded", 8192)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("uncompressed reconstructed file contents")
	b, done, err := c.Load(t.Context(), "data/hash", func() ([]byte, error) { return want, nil })
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, want) {
		t.Fatalf("decoded %q", b)
	}
	done()
	c.Close()
	c, err = testDiskCache(nil, dir, "decoded", 8192)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	b, done, err = c.Load(t.Context(), "data/hash", func() ([]byte, error) { t.Fatal("decoded hit invoked fetch/decode"); return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	if !bytes.Equal(b, want) {
		t.Fatalf("persistent bytes %q", b)
	}
	var found bool
	filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || d.Name() == "lock" {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Equal(content, want) {
			found = true
		}
		return nil
	})
	if !found {
		t.Fatal("decoded bytes not stored verbatim")
	}
}
