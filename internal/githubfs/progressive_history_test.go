package githubfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gyit/internal/repo"
	"gyit/internal/store"
)

func historyFixture(t *testing.T) (*FS, *progressiveRepository, *repo.Snapshot, string, []string) {
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
	for i := 0; i < 30; i++ {
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
