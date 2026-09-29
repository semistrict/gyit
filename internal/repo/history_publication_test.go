package repo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gyit/internal/store"
)

type historyPublishGate struct {
	store.Store
	active                atomic.Bool
	heads                 atomic.Int64
	graphs                atomic.Int64
	first, later, release chan struct{}
	once                  sync.Once
	reject                bool
}

func (s *historyPublishGate) Put(ctx context.Context, key string, b []byte, condition string) error {
	if s.active.Load() {
		if key == "HEAD" && s.heads.Add(1) == 1 {
			close(s.first)
			select {
			case <-s.release:
			case <-ctx.Done():
				return ctx.Err()
			}
			if s.reject {
				return store.ErrConflict
			}
		}
	}
	err := s.Store.Put(ctx, key, b, condition)
	if err == nil && s.active.Load() && strings.HasPrefix(key, "index/progressive-history-v2-graph-") && s.graphs.Add(1) >= 2 {
		// A later batch can finish uploading before the first HEAD request
		// enters the gate. Count uploads independently of that scheduling.
		s.once.Do(func() { close(s.later) })
	}
	return err
}

func TestHistoryIndexingOverlapsOrderedPublication(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for i := range 140 {
		write(t, dir, "file", []byte(fmt.Sprint(i)))
		commit(t, dir)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	for _, mode := range []string{"complete", "conflict", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			local, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			gate := &historyPublishGate{Store: local, first: make(chan struct{}), later: make(chan struct{}), release: make(chan struct{}), reject: mode == "conflict"}
			temp := t.TempDir()
			p, err := NewProgressive(t.Context(), gate, nil, temp)
			if err != nil {
				t.Fatal(err)
			}
			// Writers may seed decoded bytes before the upload finishes. Those
			// bytes must never make unpublished coverage visible to readers.
			p.cache = newCache(32 << 20)
			if err = p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
				t.Fatal(err)
			}
			gate.active.Store(true)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			done := make(chan error, 1)
			finished := make(chan struct{})
			var release sync.Once
			unlock := func() { release.Do(func() { close(gate.release) }) }
			go func() { defer close(finished); done <- p.IngestHistory(ctx, sha, filepath.Join(dir, ".git")) }()
			defer func() { cancel(); unlock(); <-finished }()
			select {
			case <-gate.first:
			case <-ctx.Done():
				t.Fatal("first publication never reached HEAD")
			}
			select {
			case <-gate.later:
			case <-time.After(time.Second):
				t.Fatal("indexing stopped while the preceding HEAD publication was blocked")
			}
			coverage, err := p.HistoryProgress(t.Context(), sha)
			if err != nil || coverage.CoveredCommits != 0 || coverage.Complete {
				t.Fatalf("uncommitted coverage became visible: %v %v", coverage, err)
			}
			if mode == "cancel" {
				cancel()
			} else {
				unlock()
			}
			select {
			case err = <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("publication pipeline did not finish")
			}
			switch mode {
			case "complete":
				if err != nil {
					t.Fatal(err)
				}
				assertFileHistory(t, local, dir, sha, "file")
				coverage, err = p.HistoryProgress(t.Context(), sha)
				if err != nil || !coverage.Complete || coverage.CoveredCommits != 140 {
					t.Fatalf("lost history after overlapping publication: %v %v", coverage, err)
				}
			case "conflict":
				if !errors.Is(err, store.ErrConflict) {
					t.Fatalf("CAS failure did not propagate: %v", err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel returned %v", err)
				}
			}
			if mode != "complete" {
				if n := gate.heads.Load(); n != 1 {
					t.Fatalf("later publication followed a failed one: %d HEAD attempts", n)
				}
				coverage, err = p.HistoryProgress(t.Context(), sha)
				if err != nil || coverage.CoveredCommits != 0 || coverage.Complete {
					t.Fatalf("failed publication changed coverage: %v %v", coverage, err)
				}
			}
			entries, err := os.ReadDir(temp)
			if err != nil || len(entries) != 0 {
				t.Fatalf("temporary publication state survived: %v %v", entries, err)
			}
		})
	}
}
