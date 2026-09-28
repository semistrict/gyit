package repo

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"gyit/internal/store"
)

type readonlyRepositoryStore struct{ store.Store }

func (s readonlyRepositoryStore) Put(context.Context, string, []byte, string) error {
	return fmt.Errorf("reader must not publish metadata")
}

func TestHistoricalReadsDoNotPublishMetadata(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-qb", "main")
	write(t, source, "sub/file", []byte("old contents\n"))
	old := commit(t, source)
	write(t, source, "sub/file", []byte("new contents\n"))
	commit(t, source)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Import(t.Context(), backend, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	before, token, err := backend.Get(t.Context(), "HEAD", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := New(readonlyRepositoryStore{backend}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	snapshot, err := reader.Open(t.Context(), old)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := snapshot.ReadDir(t.Context(), snapshot.Tree, "", 128)
	if err != nil || len(entries) != 1 || entries[0].Name != "sub" {
		t.Fatalf("historical root: %v %v", entries, err)
	}
	entry, err := snapshot.Resolve(t.Context(), "sub/file")
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, entry.Size)
	if _, err = snapshot.ReadAt(t.Context(), entry.OID, got, 0); err != nil || string(got) != "old contents\n" {
		t.Fatalf("historical file: %q %v", got, err)
	}
	after, newToken, err := backend.Get(t.Context(), "HEAD", 0, -1)
	if err != nil || !bytes.Equal(before, after) || newToken != token {
		t.Fatalf("read changed publication: %v", err)
	}
}

func TestLocalImportRejectsUnsupportedHashFormat(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q", "--object-format=sha256")
	write(t, source, "file", []byte("data"))
	commit(t, source)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Import(t.Context(), backend, ImportOptions{Repo: source}); err == nil {
		t.Fatal("accepted unsupported hash format")
	}
	if _, _, err = backend.Get(t.Context(), "HEAD", 0, -1); !IsNotFound(err) {
		t.Fatalf("unsupported import published objects: %v", err)
	}
}
