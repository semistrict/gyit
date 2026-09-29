package githubfs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gyit/internal/repo"
	"gyit/internal/store"
)

func historyFixture(t *testing.T) (*FS, *progressiveRepository, *repo.Snapshot, string, []string) {
	return historyFixtureLength(t, 30)
}

func historyFixtureLength(t *testing.T, count int) (*FS, *progressiveRepository, *repo.Snapshot, string, []string) {
	t.Helper()
	opts, oldest, _ := fixture(t)

	remote := filepath.Join(strings.TrimPrefix(opts.RemoteBase, "file://"), "acme", "project.git")
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", remote}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("config", "uploadpack.allowFilter", "true")
	git("config", "uploadpack.allowAnySHA1InWant", "true")
	tree := git("rev-parse", oldest+"^{tree}")
	tip := oldest
	for i := 0; i < count; i++ {
		tip = git("commit-tree", tree, "-p", tip, "-m", fmt.Sprintf("history %d", i))
	}
	git("update-ref", "refs/heads/main", tip)
	want := strings.Split(git("log", "--format=%H", "-n", "10", tip), "\n")
	f, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	p, s, err := f.prepareProgressive(t.Context(), Target{Owner: "acme", Repository: "project"}, func(string) {})
	if err != nil {
		t.Fatal(err)
	}
	return f, p, s, oldest, want
}

// A sparse path beyond the initial shallow window must eventually reach its
// creation commit, with complete coverage reusable without acquisition files.
func TestProgressiveFileLogBeyondInitialAcquisition(t *testing.T) {
	f, p, s, oldest, _ := historyFixtureLength(t, 160)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if err := f.ingestBackgroundHistory(ctx, p, s, func(string) {}); err != nil {
		t.Fatal(err)
	}
	coverage, err := p.reader.HistoryProgress(ctx, s.SHA)
	if err != nil || !coverage.Complete || coverage.CoveredCommits != 161 {
		t.Fatalf("incomplete selected ancestry: %v %v", coverage, err)
	}
	if err := os.RemoveAll(p.source); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(p.historySource); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(p.ancestrySource); err != nil {
		t.Fatal(err)
	}
	reader, err := repo.NewProgressive(ctx, p.backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reader.Open(ctx, s.SHA)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	err = snapshot.LogWithOptions(ctx, repo.LogOptions{Unlimited: true, Paths: []string{"dir/hello"}}, func(entry repo.LogEntry) error {
		got = append(got, entry.SHA)
		return nil
	})
	if err != nil || len(got) != 1 || got[0] != oldest {
		t.Fatalf("sparse durable file history %v: %v; want creation %s", got, err, oldest)
	}
}

func TestProgressiveLogAcquiresBoundedHistory(t *testing.T) {
	_, p, s, oldest, want := historyFixture(t)
	var got []string
	if err := s.Log(t.Context(), 10, false, func(e repo.LogEntry) error { got = append(got, e.SHA); return nil }); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("log got %v want %v", got, want)
	}
	if _, err := p.reader.ObjectSize(t.Context(), oldest); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("small log acquired unrelated old history: %v", err)
	}
}

func TestProgressiveLogDoesNotWaitForBackgroundFetch(t *testing.T) {
	_, p, s, _, want := historyFixture(t)
	// Background acquisition owns this lock throughout its network fetch.
	if err := p.lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer p.unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	var got []string
	if err := s.Log(ctx, 10, false, func(e repo.LogEntry) error { got = append(got, e.SHA); return nil }); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("log got %v want %v", got, want)
	}
}

func TestProgressiveFileLogContinuesWhileFullAcquisitionWaits(t *testing.T) {
	f, p, s, oldest, _ := historyFixtureLength(t, 160)
	// Hold only the full-history lane. Bounded shallow acquisition must still
	// reach changes older than the initial 64 levels and publish them durably.
	p.ancestryOperations <- struct{}{}
	defer func() { <-p.ancestryOperations }()
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.ingestBackgroundHistory(ctx, p, s, func(string) {}) }()
	var got []string
	err := s.LogWithOptions(ctx, repo.LogOptions{Unlimited: true, Paths: []string{"dir/hello"}}, func(e repo.LogEntry) error {
		got = append(got, e.SHA)
		return nil
	})
	if err != nil || len(got) != 1 || got[0] != oldest {
		t.Fatalf("history stalled behind full acquisition: %v %v", got, err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("completed incremental history left full-acquisition worker waiting")
	}
}

func TestProgressiveHistoryCancellationReleasesWaitingWorkers(t *testing.T) {
	f, p, s, _, _ := historyFixture(t)
	p.operations <- struct{}{}
	p.ancestryOperations <- struct{}{}
	defer func() { <-p.operations; <-p.ancestryOperations }()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waiting := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- f.ingestBackgroundHistory(ctx, p, s, func(message string) {
			if strings.Contains(message, "acquiring older ancestry") {
				select {
				case waiting <- struct{}{}:
				default:
				}
			}
		})
	}()
	select {
	case <-waiting:
	case <-time.After(3 * time.Second):
		t.Fatal("history worker did not reach acquisition")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation left an acquisition worker blocked")
	}
	if len(p.operations) != 1 || len(p.ancestryOperations) != 1 {
		t.Fatal("cancellation released a lane owned by another operation")
	}
}

func TestHistoryFetchCancellationCleansUpAndCanRetry(t *testing.T) {
	f, p, s, _, _ := historyFixture(t)
	// The mount can already be fetching into ancestry.git. Use an independent,
	// empty acquisition here so Git must contact the blocked HTTP endpoint.
	source := filepath.Join(t.TempDir(), "cancel.git")
	if out, err := f.git(t.Context(), "", "init", "--bare", "--quiet", source).CombinedOutput(); err != nil {
		t.Fatalf("init cancellation source: %v %s", err, out)
	}
	started, disconnected := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	var startOnce, disconnectOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startOnce.Do(func() { close(started) })
		select {
		case <-r.Context().Done():
			disconnectOnce.Do(func() { close(disconnected) })
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	setRemote := func(url string) {
		t.Helper()
		if out, err := f.git(t.Context(), source, "config", "remote.origin.url", url).CombinedOutput(); err != nil {
			t.Fatalf("remote: %v %s", err, out)
		}
	}
	setRemote(server.URL + "/project.git")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.fetchHistory(ctx, source, s.SHA, 0) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("fetch did not reach server")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fetch did not stop")
	}
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("fetch left an HTTP child connected")
	}
	if err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasSuffix(entry.Name(), ".lock") {
			return fmt.Errorf("left Git lock %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	setRemote(p.remote)
	if err := f.fetchHistory(t.Context(), source, s.SHA, 0); err != nil {
		t.Fatalf("retry after cancellation: %v", err)
	}
}

// A reader that reaches a real shallow boundary must acquire a bounded shared
// commit/tree window without waiting for either bulk acquisition lane. The
// next path query reuses those objects, without fetching a per-path history.
func TestFileHistoryGapAcquiresSharedWindow(t *testing.T) {
	_, p, s, oldest, _ := historyFixtureLength(t, 80)
	p.operations <- struct{}{}
	p.ancestryOperations <- struct{}{}
	defer func() { <-p.operations; <-p.ancestryOperations }()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	for _, path := range []string{"dir/", "dir/hello"} {
		var got []string
		err := s.LogWithOptions(ctx, repo.LogOptions{Count: 1, FullCommitIDs: true, Paths: []string{path}}, func(e repo.LogEntry) error { got = append(got, e.SHA); return nil })
		if err != nil || len(got) != 1 || got[0] != oldest {
			t.Fatalf("%s stalled behind bulk history: %v %v", path, got, err)
		}
		if path == "dir/" {
			packs, _ := filepath.Glob(filepath.Join(p.historySource, "objects", "pack", "*.pack"))
			if len(packs) == 0 {
				t.Fatal("history window was not acquired")
			}
			// Replacing the upstream with an unreachable endpoint proves the second
			// pathname uses the same already-published window rather than fetching.
			if out, err := exec.Command("git", "-C", p.historySource, "config", "remote.origin.url", "file:///does-not-exist").CombinedOutput(); err != nil {
				t.Fatalf("%v %s", err, out)
			}
		}
	}
}

func TestHistoryWindowOutlivesReaderAndStopsWithMount(t *testing.T) {
	f, p, s, _, _ := historyFixture(t)
	started, disconnected := make(chan struct{}), make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		<-r.Context().Done()
		select {
		case <-disconnected:
		default:
			close(disconnected)
		}
	}))
	defer server.Close()
	defer f.Close()
	if out, err := exec.Command("git", "-C", p.historySource, "config", "remote.origin.url", server.URL).CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.acquireHistoryWindow(ctx, p, s.SHA) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("window fetch did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader cancellation blocked")
	}
	select {
	case <-disconnected:
		t.Fatal("closing reader canceled shared repository acquisition")
	default:
	}
	closed := make(chan error, 1)
	go func() { closed <- f.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mount close left acquisition running")
	}
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("mount close left fetch connected")
	}
}
