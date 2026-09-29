//go:build !js

package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
)

type historyCoverageGuard struct {
	store.Store
	blocked atomic.Value
}

type historyParentReadCounter struct {
	store.Store
	key    string
	offset int64
	reads  atomic.Int64
}

func (s *historyParentReadCounter) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if key == s.key && off == s.offset {
		s.reads.Add(1)
	}
	return s.Store.Get(ctx, key, off, n)
}

func TestHistoryPreparationReusesParentMetadata(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for i := range 3 {
		write(t, dir, "file", []byte(fmt.Sprint(i)))
		commit(t, dir)
	}
	tip := command(t, dir, "rev-parse", "HEAD")
	parent := command(t, dir, "rev-parse", "HEAD~1")
	command(t, dir, "repack", "-ad", "--window=0")
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	counted := &historyParentReadCounter{Store: local}
	p, err := NewProgressive(t.Context(), counted, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	var recipe pb.ProgressiveObject
	if err := p.get(t.Context(), "g/"+parent, &recipe); err != nil {
		t.Fatal(err)
	}
	counted.key = progressivePackKey(recipe.Pack, recipe.Offset/progressiveSegment)
	counted.offset = recipe.Offset % progressiveSegment
	h := newHistoryPreparation(t.Context(), p)
	defer h.close()
	for _, sha := range []string{tip, parent} {
		r := h.get(sha)
		defer r.close()
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.paths == nil || r.commit.metadata == nil {
			t.Fatal("missing prepared changes or metadata")
		}
	}
	if got := counted.reads.Load(); got != 1 {
		t.Fatalf("parent commit decoded %d times, want once", got)
	}
}

func TestHistoryPreparationReadsEmptyCoverageOnce(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for i := range 4 {
		write(t, dir, "file", []byte(fmt.Sprint(i)))
		commit(t, dir)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	gitdir := filepath.Join(dir, ".git")
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	counted := &indexReadStore{Store: local}
	p, err := NewProgressive(t.Context(), counted, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ImportPacks(t.Context(), gitdir); err != nil {
		t.Fatal(err)
	}
	source, err := p.publishedHistorySource(t.Context(), []string{gitdir})
	if err != nil {
		t.Fatal(err)
	}
	defer source.close()
	counted.gets.Store(0)
	ctx := context.WithValue(t.Context(), historySourceKey{}, source)
	h := newHistoryPreparation(ctx, p)
	r := h.get(sha)
	h.close()
	defer r.close()
	if r.err != nil || r.paths == nil {
		t.Fatalf("preparation: %v", r.err)
	}
	// This fixture's entire immutable index is a single leaf and contains
	// only pack recipes. One coverage probe suffices for the entire pass.
	if got := counted.gets.Load(); got > 1 {
		t.Fatalf("empty coverage caused %d index reads; want at most one", got)
	}
}

func (s *historyCoverageGuard) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if blocked, _ := s.blocked.Load().(string); key == blocked {
		return nil, "", errors.New("history preparation reread a newer coverage index")
	}
	return s.Store.Get(ctx, key, off, n)
}

func TestHistoryPreparationKeepsCoverageViewAcrossPublication(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for i := range 3 {
		write(t, dir, "file", []byte(fmt.Sprint(i)))
		commit(t, dir)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	gitdir := filepath.Join(dir, ".git")
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	guard := &historyCoverageGuard{Store: local}
	p, err := NewProgressive(t.Context(), guard, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ImportPacks(t.Context(), gitdir); err != nil {
		t.Fatal(err)
	}
	source, err := p.publishedHistorySource(t.Context(), []string{gitdir})
	if err != nil {
		t.Fatal(err)
	}
	defer source.close()
	ctx := context.WithValue(t.Context(), historySourceKey{}, source)
	h := newHistoryPreparation(ctx, p)
	defer h.close()
	// Snapshot and pack publications may advance HEAD while this ingestion
	// pass runs. Previously covered history is immutable; rereading the new
	// index adds a remote dependency without teaching this pass anything new.
	if err := p.SetHistoryError(ctx, sha, "new publication test"); err != nil {
		t.Fatal(err)
	}
	guard.blocked.Store(p.index().root.Pack)
	r := h.get(sha)
	defer r.close()
	if r.err != nil || r.paths == nil {
		t.Fatalf("preparation depended on newer publication: %v", r.err)
	}
	paths := map[string]bool{}
	if err := r.paths.walk(ctx, func(path string, mask []byte) error {
		paths[path] = true
		if !bytes.Equal(mask, []byte{1}) {
			t.Fatalf("parent mask %q: %x", path, mask)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || !paths[""] || !paths["file"] {
		t.Fatalf("incorrect changed paths: %v", paths)
	}
}

type historyTreeReadGate struct {
	store.Store
	blockKey, nextKey       string
	blockOffset, nextOffset int64
	active                  bool
	blocked, next, release  chan struct{}
	first, later            sync.Once
}

func TestHistoryChangeMasksSurviveSpilling(t *testing.T) {
	dir := t.TempDir()
	c := newHistoryChanges(dir, 10)
	defer c.close()
	if err := c.add("", 0); err != nil {
		t.Fatal(err)
	}
	for i := range 20000 {
		if err := c.add(fmt.Sprintf("directory/long-common-prefix/file-%06d", i), 3); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.add("", 9); err != nil {
		t.Fatal(err)
	}
	for i := 19999; i >= 0; i-- {
		if err := c.add(fmt.Sprintf("directory/long-common-prefix/file-%06d", i), 9); err != nil {
			t.Fatal(err)
		}
	}
	count := 0
	err := c.walk(t.Context(), func(path string, mask []byte) error {
		want := []byte{0x08, 0x02}
		if path == "" {
			want = []byte{0x01, 0x02}
		}
		if !bytes.Equal(mask, want) {
			t.Fatalf("%q parents %x, want %x", path, mask, want)
		}
		count++
		return nil
	})
	if err != nil || count != 20001 {
		t.Fatalf("spilled changes: count=%d err=%v", count, err)
	}
	c.close()
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("spill files survived close: %v %v", entries, err)
	}
}

func (g *historyTreeReadGate) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if g.active && key == g.blockKey && off == g.blockOffset {
		g.first.Do(func() { close(g.blocked) })
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	if g.active && key == g.nextKey && off == g.nextOffset {
		g.later.Do(func() { close(g.next) })
	}
	return g.Store.Get(ctx, key, off, n)
}

func TestHistoryPreparationOverlapsWithoutPublishingPastGap(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for i := range 80 {
		write(t, dir, "file", []byte(fmt.Sprint(i)))
		commit(t, dir)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad", "--window=0")
	for _, mode := range []string{"complete", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			local, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			g := &historyTreeReadGate{Store: local, blocked: make(chan struct{}), next: make(chan struct{}), release: make(chan struct{})}
			temp := t.TempDir()
			p, err := NewProgressive(t.Context(), g, nil, temp)
			if err != nil {
				t.Fatal(err)
			}
			if err = p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
				t.Fatal(err)
			}
			locations := make([]pb.ProgressiveObject, 2)
			for i, rev := range []string{"HEAD^{tree}", "HEAD~1^{tree}"} {
				oid := command(t, dir, "rev-parse", rev)
				if err = p.get(t.Context(), "g/"+oid, &locations[i]); err != nil {
					t.Fatal(err)
				}
			}
			g.blockKey = progressivePackKey(locations[0].Pack, locations[0].Offset/progressiveSegment)
			g.blockOffset = locations[0].Offset % progressiveSegment
			g.nextKey = progressivePackKey(locations[1].Pack, locations[1].Offset/progressiveSegment)
			g.nextOffset = locations[1].Offset % progressiveSegment
			g.active = true
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			done := make(chan error, 1)
			finished := make(chan struct{})
			var release sync.Once
			unlock := func() { release.Do(func() { close(g.release) }) }
			go func() { defer close(finished); done <- p.IngestHistory(ctx, sha) }()
			defer func() { cancel(); unlock(); <-finished }()
			select {
			case <-g.blocked:
			case <-ctx.Done():
				t.Fatal("newest tree was not read")
			}
			select {
			case <-g.next:
			case <-time.After(time.Second):
				t.Fatal("older tree preparation stalled behind the newest tree read")
			}
			state, err := p.HistoryProgress(t.Context(), sha)
			if err != nil || state.Complete || state.CoveredCommits != 0 {
				t.Fatalf("coverage crossed unfinished commit: %v %v", state, err)
			}
			if mode == "cancel" {
				cancel()
			} else {
				unlock()
			}
			err = <-done
			if mode == "cancel" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation returned %v", err)
				}
				state, err = p.HistoryProgress(t.Context(), sha)
				if err != nil || state.Complete || state.CoveredCommits != 0 {
					t.Fatalf("cancellation published unfinished coverage: %v %v", state, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				assertFileHistory(t, local, dir, sha, "file")
			}
			entries, err := os.ReadDir(temp)
			if err != nil || len(entries) != 0 {
				t.Fatalf("preparation staging survived: %v %v", entries, err)
			}
		})
	}
}
