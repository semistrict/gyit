//go:build !js

package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"gyit/internal/store"
)

func historyPrefixFixture(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	var input strings.Builder
	for i := range 260 {
		fmt.Fprintf(&input, "blob\nmark :%d\ndata %d\n%s\n", i+1, len(fmt.Sprint(i)), fmt.Sprint(i))
		fmt.Fprintf(&input, "commit refs/heads/main\nmark :%d\ncommitter Test <test@example.test> %d +0000\ndata 1\nx\n", 1000+i, 1100000000+i)
		if i > 0 {
			fmt.Fprintf(&input, "from :%d\n", 999+i)
		}
		fmt.Fprintf(&input, "M 100644 :%d changing\n", i+1)
		if i%70 == 0 {
			fmt.Fprintf(&input, "M 100644 :%d sparse\n", i+1)
		}
		input.WriteByte('\n')
	}
	cmd := exec.Command("git", "-C", dir, "fast-import", "--quiet")
	cmd.Stdin = strings.NewReader(input.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fixture: %v %s", err, out)
	}
	return dir, command(t, dir, "rev-parse", "HEAD")
}

func TestHistoryPrefixRebuildUsesPublishedSourceAndPreservesCoverage(t *testing.T) {
	dir, sha := historyPrefixFixture(t)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seed, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gitdir := filepath.Join(dir, ".git")
	if err := seed.ImportPacks(t.Context(), gitdir); err != nil {
		t.Fatal(err)
	}
	if err := seed.IngestHistory(t.Context(), sha, gitdir); err != nil {
		t.Fatal(err)
	}
	before, err := seed.HistoryProgress(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	old, err := seed.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	trace := &historyObjectTraceStore{Store: backend}
	scratch := t.TempDir()
	writer, err := NewProgressive(t.Context(), trace, nil, scratch)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.RebuildHistoryPrefix(t.Context(), sha, 192, gitdir); err != nil {
		t.Fatal(err)
	}
	if trace.kinds[0].reads.Load() != 0 || trace.kinds[1].reads.Load() != 0 {
		t.Fatal("prefix rebuild fetched old graph or path data")
	}
	after, err := writer.HistoryProgress(t.Context(), sha)
	if err != nil || !proto.Equal(before, after) {
		t.Fatalf("coverage changed: %v => %v (%v)", before, after, err)
	}
	contents, err := os.ReadDir(scratch)
	if err != nil || len(contents) != 0 {
		t.Fatalf("prefix scratch survived: %v %v", contents, err)
	}
	reader := directoryReader(t, &historyReadOnlyStore{Store: backend}, t.TempDir(), 32<<20)
	fresh, err := reader.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range []*Snapshot{old, fresh} {
		for _, path := range []string{"changing", "sparse", "DOES_NOT_EXIST"} {
			var got []string
			err := snapshot.LogWithOptions(t.Context(), LogOptions{Count: 10, Paths: []string{path}, FullCommitIDs: true}, func(e LogEntry) error { got = append(got, e.SHA); return nil })
			if err != nil {
				t.Fatal(err)
			}
			want := strings.TrimSpace(command(t, dir, "log", "-n10", "--format=%H", sha, "--", path))
			if strings.Join(got, "\n") != want {
				t.Fatalf("%s: %v != %s", path, got, want)
			}
		}
	}
}

func TestHistoryPrefixRebuildRequiresPublicationAndHonorsCAS(t *testing.T) {
	dir, sha := historyPrefixFixture(t)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	guard := &progressiveCountStore{Store: backend}
	p, err := NewProgressive(t.Context(), guard, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gitdir := filepath.Join(dir, ".git")
	if err := p.RebuildHistoryPrefix(t.Context(), sha, 128, gitdir); !errors.Is(err, store.ErrNotFound) && !errors.Is(err, ErrHistoryIndexPending) {
		t.Fatalf("unpublished source accepted: %v", err)
	}
	if _, _, err := backend.Get(t.Context(), "HEAD", 0, -1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unpublished source changed HEAD: %v", err)
	}
	if err := p.ImportPacks(t.Context(), gitdir); err != nil {
		t.Fatal(err)
	}
	if err := p.IngestHistory(t.Context(), sha, gitdir); err != nil {
		t.Fatal(err)
	}
	before, _, err := backend.Get(t.Context(), "HEAD", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	guard.conflict = true
	err = p.RebuildHistoryPrefix(t.Context(), sha, 128, gitdir)
	guard.conflict = false
	if !errors.Is(err, store.ErrConflict) {
		t.Fatalf("CAS error: %v", err)
	}
	after, _, err := backend.Get(t.Context(), "HEAD", 0, -1)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("failed CAS changed HEAD: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := p.RebuildHistoryPrefix(ctx, sha, 128, gitdir); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	// The old tip remains indexed, but the source now contains a different,
	// unpublished pack. Its mere presence on disk is not publication proof.
	write(t, dir, "new-source-file", []byte("unpublished"))
	commit(t, dir)
	command(t, dir, "repack", "-adf", "--window=0")
	packs, err := filepath.Glob(filepath.Join(gitdir, "objects", "pack", "*.pack"))
	if err != nil || len(packs) != 1 {
		t.Fatalf("fixture pack replacement: %v %v", packs, err)
	}
	if err := p.RebuildHistoryPrefix(t.Context(), sha, 128, gitdir); !errors.Is(err, store.ErrNotFound) && !errors.Is(err, ErrHistoryIndexPending) {
		t.Fatalf("indexed tip accepted unpublished replacement pack: %v", err)
	}
	after, _, err = backend.Get(t.Context(), "HEAD", 0, -1)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("unpublished replacement changed HEAD: %v", err)
	}

}

func TestHistoryPrefixRebuildPreservesMergeTraversal(t *testing.T) {
	dir := historySkewFixture(t)
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gitdir := filepath.Join(dir, ".git")
	if err := p.ImportPacks(t.Context(), gitdir); err != nil {
		t.Fatal(err)
	}
	if err := p.IngestHistory(t.Context(), sha, gitdir); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{1, 4, 128} {
		if err := p.RebuildHistoryPrefix(t.Context(), sha, limit, gitdir); err != nil {
			t.Fatal(err)
		}
		reader := directoryReader(t, &historyReadOnlyStore{Store: backend}, t.TempDir(), 32<<20)
		snapshot, err := reader.Open(t.Context(), sha)
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{"hot", "nested/rare", "nested", ".", "absent"} {
			for _, first := range []bool{false, true} {
				var got []string
				err := snapshot.LogWithOptions(t.Context(), LogOptions{Count: 10, Paths: []string{path}, FirstParent: first, FullCommitIDs: true}, func(e LogEntry) error { got = append(got, e.SHA); return nil })
				if err != nil {
					t.Fatal(err)
				}
				args := []string{"log", "-n10", "--format=%H"}
				if first {
					args = append(args, "--first-parent")
				}
				args = append(args, sha, "--", path)
				want := strings.TrimSpace(command(t, dir, args...))
				if strings.Join(got, "\n") != want {
					t.Fatalf("limit=%d path=%s first=%v: %v != %s", limit, path, first, got, want)
				}
			}
		}
	}
}

func TestHistoryPrefixCancellationDoesNotPublish(t *testing.T) {
	dir, sha := historyPrefixFixture(t)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gitdir := filepath.Join(dir, ".git")
	if err := p.ImportPacks(t.Context(), gitdir); err != nil {
		t.Fatal(err)
	}
	if err := p.IngestHistory(t.Context(), sha, gitdir); err != nil {
		t.Fatal(err)
	}
	before, _, err := backend.Get(t.Context(), "HEAD", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	gate := &historyPublishGate{Store: backend, first: make(chan struct{}), later: make(chan struct{}), release: make(chan struct{})}
	gate.active.Store(true)
	p.store = gate
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); done <- p.RebuildHistoryPrefix(ctx, sha, 128, gitdir) }()
	defer func() { cancel(); close(gate.release); <-finished }()
	select {
	case <-gate.first:
	case err := <-done:
		t.Fatalf("prefix exited before publication gate: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("no prefix publication")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	after, _, err := backend.Get(t.Context(), "HEAD", 0, -1)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("cancellation changed HEAD: %v", err)
	}
}

// Opt-in, repeatable maintenance benchmark over an existing packed repository.
// Acquisition and the seed publication are outside the timed region.
func BenchmarkHistoryPrefixRebuild(b *testing.B) {
	dir := os.Getenv("GYIT_HISTORY_SOURCE")
	if dir == "" {
		b.Skip("set GYIT_HISTORY_SOURCE to an existing packed repository")
	}
	revision := os.Getenv("GYIT_HISTORY_SHA")
	if revision == "" {
		revision = "HEAD"
	}
	sha := strings.TrimSpace(benchGit(b, dir, "", "rev-parse", "--verify", revision+"^{commit}"))
	backend, err := store.NewLocal(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	disk, err := store.NewDiskCache(nil, b.TempDir(), "prefix-benchmark", 32<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer disk.Close()
	p, err := NewProgressive(b.Context(), backend, disk, b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	if err := p.ImportPacks(b.Context(), dir); err != nil {
		b.Fatal(err)
	}
	err = p.ingestHistoryInputBounded(b.Context(), []string{sha}, func() (*historySource, error) { return p.publishedHistorySource(b.Context(), []string{dir}) }, nil, nil, 64)
	if err != nil && !errors.Is(err, ErrHistoryIndexPending) {
		b.Fatal(err)
	}
	const limit = 4096
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := p.RebuildHistoryPrefix(b.Context(), sha, limit, dir); err != nil {
			b.Fatal(err)
		}
	}
}
