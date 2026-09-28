package githubfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"gyit/internal/store"
)

type preparedReadOnlyStore struct {
	store.Store
	t *testing.T
}

func (s preparedReadOnlyStore) Put(context.Context, string, []byte, string) error {
	s.t.Error("prepared mount wrote durable storage")
	return syscall.EROFS
}
func TestPreparedProgressiveReadsWithoutAcquisition(t *testing.T) {
	opts, first, _ := fixture(t)

	original, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { original.Close() })
	waitReady(t, original, "acme/project")
	pool, err := original.progressiveRepository(t.Context(), Target{Owner: "acme", Repository: "project"})
	if err != nil {
		t.Fatal(err)
	}
	if err = pool.reader.PrepareSnapshot(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	original.Close()
	if err = os.RemoveAll(pool.source); err != nil {
		t.Fatal(err)
	}
	opts.DataDir = filepath.Join(t.TempDir(), "state")
	opts.CacheDir = filepath.Join(t.TempDir(), "cache")
	opts.RemoteBase = "https://github.com"
	f, err := NewPrepared(t.Context(), opts, preparedReadOnlyStore{pool.backend, t}, Target{Owner: "acme", Repository: "project"}, first)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if got := read(t, f, "acme/project/dir/hello"); got != "first revision\n" {
		t.Fatal(got)
	}
	if _, err := f.Lookup(t.Context(), "other/unknown"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("unexpected acquisition: %v", err)
	}
}
