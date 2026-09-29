//go:build !js

package repo

import (
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
)

func TestHistoryFramesKeepFirstParentRunsTogether(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	write(t, dir, "file", []byte("base"))
	base := commit(t, dir)
	tree := command(t, dir, "rev-parse", "HEAD^{tree}")
	chain := []string{base}
	tip := base
	for i := range 8 {
		side := command(t, dir, "commit-tree", tree, "-p", base, "-m", fmt.Sprintf("side %d", i))
		tip = command(t, dir, "commit-tree", tree, "-p", tip, "-p", side, "-m", fmt.Sprintf("merge %d", i))
		chain = append(chain, tip)
	}
	command(t, dir, "update-ref", "refs/heads/main", tip)
	command(t, dir, "repack", "-ad")
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
	if e = p.IngestHistory(t.Context(), tip, filepath.Join(dir, ".git")); e != nil {
		t.Fatal(e)
	}
	var location pb.HistoryBatchLocation
	if e = p.get(t.Context(), historyBatchKey+tip, &location); e != nil {
		t.Fatal(e)
	}
	batch, e := p.readHistoryBatch(t.Context(), &location)
	if e != nil {
		t.Fatal(e)
	}
	if len(batch.Commits) < len(chain) {
		t.Fatalf("fixture unexpectedly split into short frames: %d", len(batch.Commits))
	}
	for i := range chain {
		got := hex.EncodeToString(batch.Commits[i].Oid)
		want := chain[len(chain)-1-i]
		if got != want {
			t.Fatalf("frame position %d interleaves a side branch: %s != first-parent %s", i, got, want)
		}
	}
	progress, e := p.HistoryProgress(t.Context(), tip)
	if e != nil || !progress.Complete || progress.CoveredCommits != 17 {
		t.Fatalf("side branches not covered: %+v (%v)", progress, e)
	}
}
