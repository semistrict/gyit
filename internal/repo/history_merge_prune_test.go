//go:build !js

package repo

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
)

type mergeTreeGuard struct {
	store.Store
	key    string
	offset int64
}

func (s *mergeTreeGuard) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if key == s.key && off == s.offset {
		return nil, "", fmt.Errorf("read subtree unchanged from first parent")
	}
	return s.Store.Get(ctx, key, off, n)
}

func TestHistoryMergeSkipsFirstParentUnchangedSubtrees(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	write(t, dir, "unrelated/deep/file", []byte("base"))
	write(t, dir, "focus", []byte("base"))
	commit(t, dir)
	command(t, dir, "branch", "side")
	write(t, dir, "unrelated/deep/file", []byte("main"))
	commit(t, dir)
	command(t, dir, "checkout", "-q", "side")
	write(t, dir, "focus", []byte("side"))
	commit(t, dir)
	command(t, dir, "checkout", "-q", "main")
	command(t, dir, "merge", "--no-ff", "-qm", "merge", "side")
	tip := command(t, dir, "rev-parse", "HEAD")
	ignored := command(t, dir, "rev-parse", "HEAD:unrelated")
	command(t, dir, "repack", "-ad", "--window=0")
	backend, e := store.NewLocal(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	p, e := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	if e = p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); e != nil {
		t.Fatal(e)
	}
	var recipe pb.ProgressiveObject
	if e = p.get(t.Context(), "g/"+ignored, &recipe); e != nil {
		t.Fatal(e)
	}
	p.store = &mergeTreeGuard{Store: backend, key: progressivePackKey(recipe.Pack, recipe.Offset/progressiveSegment), offset: recipe.Offset % progressiveSegment}
	// Exercise the production preparation operation without speculative work
	// on other commits, whose own first-parent changes legitimately need this tree.
	h := &historyPreparation{p: p, ctx: t.Context(), closed: true}
	r := h.prepare(tip)
	defer r.close()
	if r.err != nil {
		t.Fatal(r.err)
	}
	paths := map[string][]byte{}
	if e = r.paths.walk(t.Context(), func(path string, mask []byte) error { paths[path] = append([]byte(nil), mask...); return nil }); e != nil {
		t.Fatal(e)
	}
	if len(paths) != 2 || fmt.Sprintf("%x", paths["focus"]) != "01" || fmt.Sprintf("%x", paths[""]) != "03" {
		t.Fatalf("merge path decisions: %v", paths)
	}
	p.store = backend
	if e = p.IngestHistory(t.Context(), tip, filepath.Join(dir, ".git")); e != nil {
		t.Fatal(e)
	}
	s, e := p.Open(t.Context(), tip)
	if e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{"focus", "unrelated", "unrelated/deep/file", "."} {
		for _, first := range []bool{false, true} {
			args := []string{"log", "--format=%H"}
			if first {
				args = append(args, "--first-parent")
			}
			args = append(args, tip, "--", path)
			want := command(t, dir, args...)
			var got []string
			e = s.LogWithOptions(t.Context(), LogOptions{Count: 100, FullCommitIDs: true, FirstParent: first, Paths: []string{path}}, func(entry LogEntry) error { got = append(got, entry.SHA); return nil })
			if e != nil || strings.Join(got, "\n") != want {
				t.Fatalf("%s first=%v: %v != %s (%v)", path, first, got, want, e)
			}
		}
	}
}
