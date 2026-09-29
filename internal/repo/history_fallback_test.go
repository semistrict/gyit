package repo

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gyit/internal/store"
)

func TestUnindexedFileLogResumesWhenObjectsPublish(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	command(t, dir, "config", "uploadpack.allowFilter", "true")
	for i := range 5 {
		write(t, dir, "file", []byte{byte('a' + i)})
		commit(t, dir)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	thin := filepath.Join(t.TempDir(), "thin.git")
	command(t, dir, "clone", "--bare", "--depth=2", "--filter=blob:none", "file://"+dir, thin)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.ImportPacks(t.Context(), thin); err != nil {
		t.Fatal(err)
	}
	reader, err := NewProgressive(t.Context(), &historyReadOnlyStore{Store: backend}, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reader.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	results, done := make(chan string, 5), make(chan error, 1)
	go func() {
		done <- snapshot.LogWithOptions(ctx, LogOptions{Count: 10, FullCommitIDs: true, Paths: []string{"file"}}, func(e LogEntry) error { results <- e.SHA; return nil })
	}()
	select {
	case first := <-results:
		if first != sha {
			t.Fatalf("first=%s want=%s", first, sha)
		}
	case err := <-done:
		t.Fatalf("query ended before first available result: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case next := <-results:
		t.Fatalf("invented a change without its parent: %s", next)
	case err := <-done:
		t.Fatalf("missing objects became EOF: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	command(t, thin, "fetch", "--quiet", "--unshallow", "origin")
	if err := writer.ImportPacks(ctx, thin); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(results)
	got := []string{sha}
	for id := range results {
		got = append(got, id)
	}
	want := command(t, dir, "log", "--format=%H", "-n", "10", sha, "--", "file")
	if strings.Join(got, "\n") != want {
		t.Fatalf("got %v want %s", got, want)
	}
	state, err := writer.HistoryProgress(ctx, sha)
	if err != nil || state.CoveredCommits != 0 {
		t.Fatalf("unexpected index work: %v %v", state, err)
	}
	closed := errors.New("reader stopped")
	if err := snapshot.LogWithOptions(ctx, LogOptions{Count: 10, FullCommitIDs: true, Paths: []string{"file"}}, func(LogEntry) error { return closed }); !errors.Is(err, closed) {
		t.Fatalf("callback cancellation: %v", err)
	}
}

// Object availability and the derived all-path index are separate milestones.
// A reader must not wait for that index when published objects prove its answer.
func TestFileLogReadsPublishedObjectsBeforePathIndex(t *testing.T) {
	dir := historySkewFixture(t)
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	// A failed derived-index job must not make already-published Git objects
	// unreadable. No background indexer runs during this test.
	if err := writer.SetHistoryError(t.Context(), sha, "index worker paused"); err != nil {
		t.Fatal(err)
	}
	reader, err := NewProgressive(t.Context(), &historyReadOnlyStore{Store: backend}, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reader.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"hot", "nested/rare", "nested", ".", "absent"} {
		for _, firstParent := range []bool{false, true} {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			var got []string
			err := snapshot.LogWithOptions(ctx, LogOptions{Count: 10, FullCommitIDs: true, Paths: []string{path}, FirstParent: firstParent}, func(e LogEntry) error { got = append(got, e.SHA); return nil })
			cancel()
			if err != nil {
				t.Fatalf("%s first-parent=%v: %v", path, firstParent, err)
			}
			args := []string{"log", "--format=%H", "-n", "10"}
			if firstParent {
				args = append(args, "--first-parent")
			}
			args = append(args, sha, "--", path)
			want := command(t, dir, args...)
			if strings.Join(got, "\n") != want {
				t.Fatalf("%s: got %v, want %s", path, got, want)
			}
		}
	}
	state, err := writer.HistoryProgress(t.Context(), sha)
	if err != nil || state.CoveredCommits != 0 || state.Complete {
		t.Fatalf("query created index coverage: %v %v", state, err)
	}
}
