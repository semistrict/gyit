package githubfs

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gyit/internal/repo"
)

func TestProgressiveMountBeforeHistoryAndDurableReads(t *testing.T) {
	opts, first, second := fixture(t)

	remote := filepath.Join(strings.TrimPrefix(opts.RemoteBase, "file://"), "acme", "project.git")
	for _, kv := range [][2]string{{"uploadpack.allowFilter", "true"}, {"uploadpack.allowAnySHA1InWant", "true"}} {
		if out, err := exec.Command("git", "-C", remote, "config", kv[0], kv[1]).CombinedOutput(); err != nil {
			t.Fatalf("config: %v %s", err, out)
		}
	}
	f, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	waitReady(t, f, "acme/project")
	if got := read(t, f, "acme/project/dir/hello"); got != "first revision\n" {
		t.Fatalf("content: %q", got)
	}
	j := f.jobs[Target{Owner: "acme", Repository: "project"}.Key()]
	if j.progressive == nil {
		t.Fatal("legacy importer used")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	for {
		state, e := j.progressive.State(ctx, first)
		if e == nil && state.HistoryComplete && state.SnapshotComplete {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("background import: %v %v", state, e)
		case <-time.After(20 * time.Millisecond):
		}
	}
	// A separate revision reuses the repository-wide object pool.
	// A different branch is acquired explicitly; the selected tip's history
	// does not prefetch unrelated branch tips.
	p, _, err := f.prepareProgressive(ctx, Target{Owner: "acme", Repository: "project", Revision: second}, func(string) {})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.reader.PrepareSnapshot(ctx, second); err != nil {
		t.Fatal(err)
	}
	newer, err := p.reader.Open(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(strings.TrimPrefix(opts.RemoteBase, "file://")); err != nil {
		t.Fatal(err)
	}
	// Also remove acquisition state: published readers must only need Store.
	if err = os.RemoveAll(p.source); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		s    *repo.Snapshot
		want string
	}{{j.snapshot, "first revision\n"}, {newer, "second revision\n"}} {
		e, err := tc.s.Resolve(ctx, "dir/hello")
		if err != nil {
			t.Fatal(err)
		}
		b := make([]byte, e.Size)
		if _, err = tc.s.ReadAt(ctx, e.OID, b, 0); err != nil {
			t.Fatal(err)
		}
		if string(b) != tc.want {
			t.Fatalf("pinned snapshot content %q", b)
		}
	}
}

// Shared repository setup belongs to the filesystem: cancelling the request
// that happened to start it must not break the repository for later readers.
func TestCancelledFirstRequestDoesNotBreakSetup(t *testing.T) {
	opts, _, _ := fixture(t)
	f, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	target := Target{Owner: "acme", Repository: "project"}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.progressiveRepository(cancelled, target); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request returned %v", err)
	}
	p, err := f.progressiveRepository(t.Context(), target)
	if err != nil {
		t.Fatalf("setup after cancelled first request: %v", err)
	}
	if _, _, err := f.prepareProgressive(t.Context(), target, func(string) {}); err != nil {
		t.Fatalf("prepare after cancelled first request: %v", err)
	}
	if p.reader == nil {
		t.Fatal("setup left no reader")
	}
}
