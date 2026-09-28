package repo

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gyit/internal/store"
)

type directoryOnlyStore struct{ store.Store }

func (s directoryOnlyStore) Get(ctx context.Context, key string, off, size int64) ([]byte, string, error) {
	if !strings.HasPrefix(key, "index/progressive-") {
		return nil, "", fmt.Errorf("directory traversal requested unrelated object %s", key)
	}
	return s.Store.Get(ctx, key, off, size)
}

func TestDirectoryCursorDescendsWithoutObjectIndex(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-qb", "main")
	write(t, source, "sub/nested/file", []byte("contents"))
	sha := commit(t, source)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Import(t.Context(), backend, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	r, err := New(backend, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s, err := r.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := s.ReadDir(t.Context(), s.Tree, "", 128)
	if err != nil || len(entries) != 1 {
		t.Fatalf("root: %v %v", entries, err)
	}
	// The cursor must carry everything needed to descend. No cached index
	// pages, commits, or Git pack reads are available after this point.
	s.progressive.store = directoryOnlyStore{backend}
	s.progressive.cache = newCache(0)
	nested, err := s.LookupDirectory(t.Context(), entries[0], "nested")
	if err != nil {
		t.Fatal(err)
	}
	files, err := s.ReadDirectory(t.Context(), nested, "", 128)
	if err != nil || len(files) != 1 || files[0].Name != "file" || files[0].Size != 8 {
		t.Fatalf("nested directory: %v %v", files, err)
	}
}
