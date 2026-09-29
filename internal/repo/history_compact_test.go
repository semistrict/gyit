//go:build !js

package repo

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gyit/internal/store"
)

func TestLargeCompletedHistoryPacksFewPublications(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	var input strings.Builder
	input.WriteString("blob\nmark :1\ndata 1\nx\n")
	for i := range 512 {
		fmt.Fprintf(&input, "commit refs/heads/main\nmark :%d\ncommitter Test <test@example.test> %d +0000\ndata 1\nx\n", 100+i, 1100000000+i)
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
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	if err := p.IngestHistory(t.Context(), sha, filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	state, err := p.HistoryProgress(t.Context(), sha)
	if err != nil || !state.GraphCompacted || !state.Complete {
		t.Fatalf("large completed history remains fragmented: %v %v", state, err)
	}
	measured := &historyBenchmarkStore{Store: backend, calls: map[string]int{}, trace: true, elapsed: map[string]time.Duration{}}
	reader := directoryReader(t, &historyReadOnlyStore{Store: measured}, t.TempDir(), 32<<20)
	snapshot, err := reader.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := snapshot.LogWithOptions(t.Context(), LogOptions{Count: 1, FullCommitIDs: true, Paths: []string{"file"}}, func(e LogEntry) error { got = append(got, e.SHA); return nil }); err != nil {
		t.Fatal(err)
	}
	want := command(t, dir, "log", "--format=%H", "-n1", "--", "file")
	if strings.Join(got, "\n") != want {
		t.Fatalf("history %v, want %s", got, want)
	}
	graphs := 0
	for _, request := range measured.requests {
		if strings.Contains(request, "/progressive-history-v2-graph-") {
			graphs++
		}
	}
	if graphs != 1 {
		t.Fatalf("512 small commits needed %d graph requests, want one", graphs)
	}
}
