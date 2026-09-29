//go:build !js

package repo

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"gyit/internal/store"
)

func historyWriteCount(s *progressiveCountStore) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts
}

func TestHistoryUnchangedCoverageDoesNotPublish(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	command(t, dir, "config", "uploadpack.allowFilter", "true")
	for i := range 12 {
		write(t, dir, "file", []byte(fmt.Sprint(i)))
		commit(t, dir)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	thin := filepath.Join(t.TempDir(), "thin.git")
	command(t, dir, "clone", "--bare", "--depth=1", "--filter=blob:none", "file://"+dir, thin)
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	backend := &progressiveCountStore{Store: local}
	open := func() *Progressive {
		t.Helper()
		p, err := NewProgressive(ctx, backend, nil, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	p := open()
	if err := p.ImportPacks(ctx, thin); err != nil {
		t.Fatal(err)
	}
	checkPending := func(wantCovered uint64) {
		t.Helper()
		before, err := p.HistoryProgress(ctx, sha)
		if err != nil {
			t.Fatal(err)
		}
		writes := historyWriteCount(backend)
		_, token, err := backend.Get(ctx, "HEAD", 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			p = open()
			if err := p.IngestHistory(ctx, sha, thin); !errors.Is(err, ErrHistoryIndexPending) {
				t.Fatalf("pending ingestion: %v", err)
			}
			after, err := p.HistoryProgress(ctx, sha)
			if err != nil || after.CoveredCommits != wantCovered || !proto.Equal(before, after) {
				t.Fatalf("unchanged coverage: %v, %v", after, err)
			}
			_, current, err := backend.Get(ctx, "HEAD", 0, -1)
			if err != nil || current != token || historyWriteCount(backend) != writes {
				t.Fatalf("unchanged coverage published: writes=%d -> %d, err=%v", writes, historyWriteCount(backend), err)
			}
		}
	}
	checkPending(0)
	command(t, thin, "fetch", "--quiet", "--depth=4", "origin")
	if err := p.ImportPacks(ctx, thin); err != nil {
		t.Fatal(err)
	}
	if err := p.IngestHistory(ctx, sha, thin); !errors.Is(err, ErrHistoryIndexPending) {
		t.Fatal(err)
	}
	checkPending(3)
	covered, err := p.HistoryProgress(ctx, sha)
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range []string{"upstream unavailable", ""} {
		if err := p.SetHistoryError(ctx, sha, message); err != nil {
			t.Fatal(err)
		}
		after, err := p.HistoryProgress(ctx, sha)
		covered.Error = message
		if err != nil || !proto.Equal(covered, after) {
			t.Fatalf("error update changed coverage: %v, %v", after, err)
		}
	}
	command(t, thin, "fetch", "--quiet", "--unshallow", "origin")
	if err := p.ImportPacks(ctx, thin); err != nil {
		t.Fatal(err)
	}
	if err := p.IngestHistory(ctx, sha, thin); err != nil {
		t.Fatal(err)
	}
	assertFileHistory(t, local, dir, sha, "file")
	// A different selected tip can already have every commit indexed without
	// having its own completion marker. Zero new graph records must still publish EOF.
	older := command(t, dir, "rev-parse", "HEAD~5")
	if err := p.IngestHistory(ctx, older, thin); err != nil {
		t.Fatal(err)
	}
	state, err := p.HistoryProgress(ctx, older)
	if err != nil || !state.Complete {
		t.Fatalf("completion for indexed tip: %v %v", state, err)
	}
	assertFileHistory(t, local, dir, older, "file")
}

func TestHistoryErrorNoopChecksDurableVersion(t *testing.T) {
	ctx := t.Context()
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	backend := &progressiveCountStore{Store: local}
	open := func() *Progressive {
		t.Helper()
		p, err := NewProgressive(ctx, backend, nil, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	sha := strings.Repeat("a", 40)
	p, stale := open(), open()
	if err := p.SetHistoryError(ctx, sha, ""); err != nil {
		t.Fatal(err)
	}
	if historyWriteCount(backend) != 0 {
		t.Fatal("empty-store no-op published")
	}
	if err := p.SetHistoryError(ctx, sha, "upstream unavailable"); err != nil {
		t.Fatal(err)
	}
	if err := stale.SetHistoryError(ctx, sha, ""); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale clear: %v", err)
	}
	for _, message := range []string{"upstream unavailable", ""} {
		if err := p.SetHistoryError(ctx, sha, message); err != nil {
			t.Fatal(err)
		}
		writes := historyWriteCount(backend)
		if err := p.SetHistoryError(ctx, sha, message); err != nil {
			t.Fatal(err)
		}
		if historyWriteCount(backend) != writes {
			t.Fatalf("unchanged error %q published", message)
		}
	}
	stale = open()
	if err := p.SetHistoryError(ctx, sha, "new failure"); err != nil {
		t.Fatal(err)
	}
	if err := stale.SetHistoryError(ctx, sha, ""); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale existing clear: %v", err)
	}
}
