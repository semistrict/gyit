//go:build !js

package repo

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"gyit/internal/store"
)

func TestSnapshotPreparationUsesPublishedAcquisition(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	body := []byte("published content\n")
	write(t, dir, "dir/file", body)
	commit(t, dir)
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	measured := &indexReadStore{Store: backend}
	p, err := NewProgressive(t.Context(), measured, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gitdir := filepath.Join(dir, ".git")
	if err := p.PrepareSnapshot(t.Context(), sha, gitdir); err == nil {
		t.Fatal("prepared a snapshot from unpublished acquisition data")
	}
	if ready, err := p.HasPreparedSnapshot(t.Context()); err != nil || ready {
		t.Fatalf("unpublished acquisition created metadata: %v %v", ready, err)
	}
	if err := p.ImportPacks(t.Context(), gitdir); err != nil {
		t.Fatal(err)
	}
	if err := p.PrepareSnapshot(t.Context(), sha, gitdir); err != nil {
		t.Fatal(err)
	}
	if got := measured.packs.Load(); got != 0 {
		t.Fatalf("snapshot preparation re-read %d remote pack ranges despite published local acquisition", got)
	}
	if err := os.RemoveAll(gitdir); err != nil {
		t.Fatal(err)
	}
	reader, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s, err := reader.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.Resolve(t.Context(), "dir/file")
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, e.Size)
	if _, err := s.ReadAt(t.Context(), e.OID, got, 0); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("durable read %q: %v", got, err)
	}
}
