package githubfs

import (
	"context"
	"errors"
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

// Observe the acquisition boundary, holding the depth-one request open. Full
// history should already be in flight, but may not publish through that hold.
func TestSnapshotAndHistoryAcquisitionOverlap(t *testing.T) {
	opts, _, sha := fixture(t)
	remote := filepath.Join(strings.TrimPrefix(opts.RemoteBase, "file://"), "acme", "project.git")
	for _, kv := range [][2]string{{"uploadpack.allowFilter", "true"}, {"uploadpack.allowAnySHA1InWant", "true"}} {
		if out, err := exec.Command("git", "-C", remote, "config", kv[0], kv[1]).CombinedOutput(); err != nil {
			t.Fatalf("config: %v %s", err, out)
		}
	}
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for key, value := range map[string]string{"GYIT_TEST_REAL_GIT": realGit, "GYIT_TEST_GATE": dir} {
		t.Setenv(key, value)
	}
	wrapper := `#!/bin/sh
case "$2" in
  */acquisition.git)
    case " $* " in
      *" --depth=1 "*)
        : > "$GYIT_TEST_GATE/snapshot"
        while [ ! -f "$GYIT_TEST_GATE/release" ]; do sleep 0.01; done
        ;;
    esac ;;
  */ancestry.git)
    case " $* " in *" fetch "*)
      : > "$GYIT_TEST_GATE/history"
      "$GYIT_TEST_REAL_GIT" "$@"
      result=$?
      : > "$GYIT_TEST_GATE/history-done"
      exit "$result"
    ;; esac ;;
esac
exec "$GYIT_TEST_REAL_GIT" "$@"
`
	if err := os.WriteFile(filepath.Join(dir, "git"), []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	f, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	type result struct {
		p   *progressiveRepository
		s   *repo.Snapshot
		err error
	}
	done := make(chan result, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		p, s, err := f.prepareProgressive(ctx, Target{Owner: "acme", Repository: "project", Revision: sha}, func(string) {})
		done <- result{p, s, err}
	}()
	defer func() { _ = os.WriteFile(filepath.Join(dir, "release"), nil, 0600); cancel(); <-finished }()
	waitFile := func(name string) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("%s acquisition did not start while depth-one fetch was held", name)
	}
	waitFile("snapshot")
	waitFile("history")
	waitFile("history-done")
	p, err := f.progressiveRepository(ctx, Target{Owner: "acme", Repository: "project"})
	if err != nil {
		t.Fatal(err)
	}
	coverage, err := p.reader.HistoryProgress(ctx, sha)
	if err != nil || coverage.Complete || coverage.CoveredCommits != 0 {
		t.Fatalf("unpublished acquisition exposed coverage: %v %v", coverage, err)
	}
	if _, err := p.reader.ObjectSize(ctx, sha); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("early acquisition exposed an unpublished object: %v", err)
	}
	select {
	case r := <-done:
		t.Fatalf("mount bypassed incomplete snapshot request: %v", r.err)
	default:
	}
	if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	if err := f.ingestBackgroundHistory(ctx, r.p, r.s, func(string) {}); err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := r.s.LogWithOptions(ctx, repo.LogOptions{Unlimited: true, Paths: []string{"dir/hello"}}, func(e repo.LogEntry) error { got = append(got, e.SHA); return nil }); err != nil {
		t.Fatal(err)
	}
	want, err := exec.Command(realGit, "-C", remote, "log", "--format=%H", sha, "--", "dir/hello").Output()
	if err != nil || strings.Join(got, "\n") != strings.TrimSpace(string(want)) {
		t.Fatalf("completed history differs from Git: %v %v", got, err)
	}
}

func TestCloseCancelsEarlyHistoryAcquisition(t *testing.T) {
	f, _, _, _, _ := historyFixture(t)
	started, disconnected := make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	var first, last sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first.Do(func() { close(started) })
		select {
		case <-r.Context().Done():
			last.Do(func() { close(disconnected) })
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	source := filepath.Join(t.TempDir(), "early.git")
	for _, args := range [][]string{{"init", "--bare", "--quiet", source}, {"-C", source, "config", "remote.origin.url", server.URL + "/project.git"}} {
		if out, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("setup: %v %s", err, out)
		}
	}
	p := &progressiveRepository{ancestrySource: source, ancestryOperations: make(chan struct{}, 1)}
	sha := strings.Repeat("1", 40)
	f.startEarlyAncestry(p, sha)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("early acquisition did not reach HTTP server")
	}
	// Repeated setup attempts share the in-flight transfer; another selected
	// revision leaves acquisition to its ordinary importer instead of queuing.
	a := p.takeEarlyAncestry(sha)
	if a == nil {
		t.Fatal("missing in-flight acquisition")
	}
	f.startEarlyAncestry(p, strings.Repeat("2", 40))
	if other := p.takeEarlyAncestry(strings.Repeat("2", 40)); other != nil {
		t.Fatal("queued another early transfer behind occupied lane")
	}
	closed := make(chan error, 1)
	go func() { closed <- f.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("filesystem close left early acquisition running")
	}
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("filesystem close left Git's HTTP helper connected")
	}
	select {
	case <-a.done:
	default:
		t.Fatal("filesystem close did not join acquisition")
	}
	if len(p.ancestryOperations) != 0 {
		t.Fatal("closed acquisition retained its lane")
	}
	f.startEarlyAncestry(p, sha)
	if again := p.takeEarlyAncestry(sha); again != nil {
		t.Fatal("started acquisition after filesystem close")
	}
}
