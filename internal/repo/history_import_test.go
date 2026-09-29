//go:build !js

package repo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gyit/internal/store"
)

type historyImportGate struct {
	store.Store
	pack, history, release chan struct{}
	oncePack, onceHistory  sync.Once
	heads                  atomic.Int64
	firstHead              []byte
	mode                   string
}

func (s *historyImportGate) Put(ctx context.Context, key string, b []byte, condition string) error {
	if strings.HasPrefix(key, "packs/progressive/") {
		s.oncePack.Do(func() { close(s.pack) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
		if s.mode == "upload-failure" {
			return errors.New("pack upload failed")
		}
	}
	if strings.HasPrefix(key, "index/progressive-history-v2-graph-") {
		s.onceHistory.Do(func() { close(s.history) })
	}
	if key == "HEAD" {
		n := s.heads.Add(1)
		if n == 1 {
			s.firstHead = append([]byte(nil), b...)
		}
		if n == 1 && s.mode == "conflict" {
			return store.ErrConflict
		}
	}
	return s.Store.Put(ctx, key, b, condition)
}

func TestHistoryPreparationOverlapsPackPublication(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for i := range 80 {
		write(t, dir, "file", []byte(fmt.Sprint(i)))
		commit(t, dir)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	for _, mode := range []string{"complete", "upload-failure", "conflict", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			acquisition := filepath.Join(t.TempDir(), "acquisition.git")
			command(t, dir, "clone", "--quiet", "--bare", "--no-hardlinks", dir, acquisition)
			local, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			gate := &historyImportGate{Store: local, pack: make(chan struct{}), history: make(chan struct{}), release: make(chan struct{}), mode: mode}
			temp := t.TempDir()
			p, err := NewProgressive(t.Context(), gate, nil, temp)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			done := make(chan error, 1)
			finished := make(chan struct{})
			go func() { defer close(finished); done <- p.ImportHistoryPacks(ctx, sha, acquisition) }()
			var release sync.Once
			unlock := func() { release.Do(func() { close(gate.release) }) }
			defer func() { cancel(); unlock(); <-finished }()
			select {
			case <-gate.pack:
			case <-ctx.Done():
				t.Fatal("pack upload did not start")
			}
			select {
			case <-gate.history:
			case <-time.After(2 * time.Second):
				t.Fatal("history preparation waited for pack publication")
			}
			state, err := p.HistoryProgress(ctx, sha)
			if err != nil || state.CoveredCommits != 0 || state.Complete || gate.heads.Load() != 0 {
				t.Fatalf("unpublished packs exposed coverage: %v %v heads=%d", state, err, gate.heads.Load())
			}
			if _, err := p.ObjectSize(ctx, sha); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("unpublished object visible: %v", err)
			}
			if mode == "cancel" {
				cancel()
			} else {
				unlock()
			}
			select {
			case err = <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("import did not stop")
			}
			if mode == "complete" {
				if err != nil {
					t.Fatal(err)
				}
				// Inspect the first immutable publication, not the final HEAD:
				// pack recipes and ready coverage must be visible together.
				first, err := NewProgressive(t.Context(), &historyManifestView{Store: local, head: gate.firstHead}, nil, t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				coverage, err := first.HistoryProgress(t.Context(), sha)
				if err != nil || coverage.CoveredCommits != 80 {
					t.Fatalf("first pack publication omitted history prepared during the upload: %v %v", coverage, err)
				}
				snapshot, err := first.Open(t.Context(), sha)
				if err != nil {
					t.Fatalf("first coverage publication omitted pack recipes or data: %v", err)
				}
				var firstEntry string
				if err := snapshot.LogWithOptions(t.Context(), LogOptions{Count: 1, FullCommitIDs: true, Paths: []string{"file"}}, func(e LogEntry) error { firstEntry = e.SHA; return nil }); err != nil || firstEntry != sha {
					t.Fatalf("first publication cannot serve its newest result: %s %v", firstEntry, err)
				}
			} else {
				if err == nil {
					t.Fatal("failed pack import reported success")
				}
				if mode == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				if mode == "conflict" && !errors.Is(err, store.ErrConflict) {
					t.Fatal(err)
				}
				if mode == "upload-failure" && !strings.Contains(err.Error(), "pack upload failed") {
					t.Fatal(err)
				}
				fresh, e := NewProgressive(t.Context(), local, nil, t.TempDir())
				if e != nil {
					t.Fatal(e)
				}
				state, e := fresh.HistoryProgress(t.Context(), sha)
				if e != nil || state.CoveredCommits != 0 || state.Complete {
					t.Fatalf("failed import published coverage: %v %v", state, e)
				}
				if _, e := fresh.ObjectSize(t.Context(), sha); !errors.Is(e, store.ErrNotFound) {
					t.Fatalf("failed joint publication exposed pack recipes: %v", e)
				}
				// Unreferenced uploads from the failed attempt must not affect a retry.
				if e := fresh.ImportHistoryPacks(t.Context(), sha, acquisition); e != nil {
					t.Fatal(e)
				}
			}
			if err := os.RemoveAll(acquisition); err != nil {
				t.Fatal(err)
			}
			assertFileHistory(t, local, dir, sha, "file")
			entries, e := os.ReadDir(temp)
			if e != nil || len(entries) != 0 {
				t.Fatalf("staging survived import: %v %v", entries, e)
			}
		})
	}
}

type historyManifestView struct {
	store.Store
	head []byte
}

func (s *historyManifestView) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if key == "HEAD" {
		return append([]byte(nil), s.head...), "pinned-publication", nil
	}
	return s.Store.Get(ctx, key, off, n)
}

func TestHistoryImportPublishesPacksWithoutCoverageAndResumes(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for i := range 3 {
		write(t, dir, "file", []byte(fmt.Sprint(i)))
		commit(t, dir)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	thin := filepath.Join(t.TempDir(), "thin.git")
	command(t, dir, "clone", "--quiet", "--bare", "--depth=1", "file://"+dir, thin)
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProgressive(t.Context(), local, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ImportHistoryPacks(t.Context(), sha, thin); !errors.Is(err, ErrHistoryIndexPending) {
		t.Fatalf("depth-one coverage: %v", err)
	}
	reader, err := NewProgressive(t.Context(), local, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ObjectSize(t.Context(), sha); err != nil {
		t.Fatalf("pack import returned before durability: %v", err)
	}
	state, err := reader.HistoryProgress(t.Context(), sha)
	if err != nil || state.Complete || state.CoveredCommits != 0 {
		t.Fatalf("depth-one frontier: %v %v", state, err)
	}
	command(t, thin, "fetch", "--quiet", "--keep", "--unshallow", "origin")
	if err := p.ImportHistoryPacks(t.Context(), sha, thin); err != nil {
		t.Fatal(err)
	}
	assertFileHistory(t, local, dir, sha, "file")
	// A second update reuses covered ancestry while indexing new commits.
	write(t, dir, "file", []byte("new version"))
	commit(t, dir)
	next := command(t, dir, "rev-parse", "HEAD")
	command(t, thin, "fetch", "--quiet", "--keep", "origin", "main:main")
	if err := p.ImportHistoryPacks(t.Context(), next, thin); err != nil {
		t.Fatal(err)
	}
	assertFileHistory(t, local, dir, next, "file")
	if err := p.ImportHistoryPacks(t.Context(), next, thin); err != nil {
		t.Fatal(err)
	}
}

// Display-size boundaries produce frames with fewer than 64 commits. The
// publication limit must still split the last frame rather than rounding up.
func TestHistoryPublicationCommitBound(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	var input strings.Builder
	message := strings.Repeat("m", 1024)
	input.WriteString("blob\nmark :1\ndata 1\nx\n")
	const commits = historyPublicationCommits + 104
	for i := range commits {
		fmt.Fprintf(&input, "commit refs/heads/main\nmark :%d\ncommitter Test <test@example.test> %d +0000\ndata %d\n%s\n", 100+i, 1100000000+i, len(message), message)
		if i > 0 {
			fmt.Fprintf(&input, "from :%d\n", 99+i)
		} else {
			input.WriteString("M 100644 :1 file\n")
		}
		input.WriteByte('\n')
	}
	cmd := exec.Command("git", "-C", dir, "fast-import", "--quiet")
	cmd.Stdin = strings.NewReader(input.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fast-import: %v %s", err, out)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProgressive(t.Context(), local, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gitdir := filepath.Join(dir, ".git")
	if err := p.ImportPacks(t.Context(), gitdir); err != nil {
		t.Fatal(err)
	}
	gate := &historyImportGate{Store: local, history: make(chan struct{})}
	p.store = gate
	if err := p.ingestHistoryInput(t.Context(), []string{sha}, func() (*historySource, error) {
		return p.publishedHistorySource(t.Context(), []string{gitdir})
	}, nil, make(chan struct{})); err != nil {
		t.Fatal(err)
	}
	first, err := NewProgressive(t.Context(), &historyManifestView{Store: local, head: gate.firstHead}, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	coverage, err := first.HistoryProgress(t.Context(), sha)
	if err != nil || coverage.CoveredCommits != historyPublicationCommits || coverage.Complete {
		t.Fatalf("first bounded publication: %v %v", coverage, err)
	}
	coverage, err = p.HistoryProgress(t.Context(), sha)
	if err != nil || coverage.CoveredCommits != commits || !coverage.Complete {
		t.Fatalf("remaining commits were not published: %v %v", coverage, err)
	}
}
