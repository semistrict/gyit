package repo

import (
	"bytes"
	"context"
	"testing"

	"gyit/internal/store"
)

// The backing publisher advances immediately after the first HEAD read.
type advancingViewStore struct {
	store.Store
	first []byte
	reads int
}

func (s *advancingViewStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if key == "HEAD" {
		s.reads++
		if s.reads == 1 {
			return s.first, "first", nil
		}
	}
	return s.Store.Get(ctx, key, off, n)
}

func TestViewPinsOnePublicationAcrossObjects(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	write(t, dir, "file", []byte("old\n"))
	old := commit(t, dir)
	local, _ := store.NewLocal(t.TempDir())
	if _, err := Import(ctx, local, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	head, _, err := local.Get(ctx, "HEAD", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	initial, _ := New(local, 1<<20)
	current, err := initial.Open(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	write(t, dir, "file", []byte("new\n"))
	commit(t, dir)
	if _, err := Import(ctx, local, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	advancing := &advancingViewStore{Store: local, first: head}
	r, _ := New(advancing, 1<<20)
	var out bytes.Buffer
	if err := r.View(ctx, current, ViewOptions{Command: "show", Args: []string{"main:file", "main:file"}}, &out); err != nil {
		t.Fatal(err)
	}
	if out.String() != "old\nold\n" {
		t.Fatalf("query mixed publications: %q", out.String())
	}
	if advancing.reads != 1 {
		t.Fatalf("read mutable HEAD %d times", advancing.reads)
	}
}
