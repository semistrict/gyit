package githubfs

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"gyit/internal/control"
	controlpb "gyit/internal/gen/gyit/control/v1"
	"gyit/internal/repo"
)

// Opt-in replay of an existing shallow acquisition, followed by real GitHub
// gap fetching. No bulk history worker runs. An optional isolated GCS root
// measures durable publication; the caller must delete that diagnostic root.
func TestSharedHistoryGapLatency(t *testing.T) {
	source, ids, sha, baseline := os.Getenv("GYIT_SHALLOW_SOURCE"), os.Getenv("GYIT_SHALLOW_PACKS"), os.Getenv("GYIT_HISTORY_SHA"), os.Getenv("GYIT_HISTORY_SOURCE")
	if source == "" || ids == "" || sha == "" || baseline == "" {
		t.Skip("set shallow source/packs, history SHA and native baseline")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	dir := t.TempDir()
	f, err := New(Options{DataDir: filepath.Join(dir, "state"), CacheDir: filepath.Join(dir, "cache"), CacheBytes: 32 << 20, StoreRoot: os.Getenv("GYIT_GAP_STORE")})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	p, err := f.progressiveRepository(ctx, Target{Owner: "torvalds", Repository: "linux"})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range strings.Split(ids, ",") {
		if !fullSHA(id) {
			t.Fatal("invalid pack ID")
		}
		for _, ext := range []string{".pack", ".idx"} {
			name := "pack-" + id + ext
			if err := os.Symlink(filepath.Join(source, "objects", "pack", name), filepath.Join(p.source, "objects", "pack", name)); err != nil {
				t.Fatal(err)
			}
		}
	}
	start := time.Now()
	if err := p.reader.ImportPacks(ctx, p.source); err != nil {
		t.Fatal(err)
	}
	t.Logf("initial window publication=%s", time.Since(start))
	acquire := p.reader.DemandHistory
	requests := 0
	p.reader.DemandHistory = func(ctx context.Context, sha string) error {
		requests++
		start := time.Now()
		err := acquire(ctx, sha)
		t.Logf("gap=%s fetch+publication=%s err=%v", sha, time.Since(start), err)
		return err
	}
	s, err := p.reader.Open(ctx, sha)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"lib", "Documentation/", "Documentation/"} {
		start := time.Now()
		var first time.Duration
		var got []string
		before := requests
		err := s.LogWithOptions(ctx, repo.LogOptions{Count: 10, FullCommitIDs: true, Paths: []string{path}}, func(e repo.LogEntry) error {
			if len(got) == 0 {
				first = time.Since(start)
			}
			got = append(got, e.SHA)
			return nil
		})
		elapsed := time.Since(start)
		t.Logf("%s first=%s total=%s results=%d new_fetches=%d", path, first, elapsed, len(got), requests-before)
		if err != nil {
			t.Fatal(err)
		}
		start = time.Now()
		cmd := exec.CommandContext(ctx, "git", "-C", baseline, "log", "--format=%H", "-n", "10", sha, "--", path)
		cmd.Env = append(os.Environ(), "GIT_NO_LAZY_FETCH=1", "GIT_TERMINAL_PROMPT=0", "GIT_ALLOW_PROTOCOL=")
		want, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(got, "\n") != strings.TrimSpace(string(want)) {
			t.Fatalf("history differs: %v vs %s", got, want)
		}
		t.Logf("%s native=%s", path, time.Since(start))
	}
	if requests == 0 {
		t.Fatal("fixture did not exercise a history gap")
	}
	t.Logf("total shared-window acquisitions=%d", requests)
}

// Fresh setup through the normal namespace/control path. Unlike the window
// replay above, no acquisition files or repository data are reused by gyit.
// The existing full source belongs only to Git's correctness baseline.
func TestFreshDirectoryHistoryStartup(t *testing.T) {
	root, sha, baseline := os.Getenv("GYIT_FRESH_DIRECTORY_STORE"), os.Getenv("GYIT_HISTORY_SHA"), os.Getenv("GYIT_HISTORY_SOURCE")
	if root == "" || sha == "" || baseline == "" {
		t.Skip("set an isolated fresh store, selected SHA and native baseline")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	dir := t.TempDir()
	f, err := New(Options{DataDir: filepath.Join(dir, "state"), CacheDir: filepath.Join(dir, "cache"), CacheBytes: 32 << 20, StoreRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	start := time.Now()
	data, err := f.Endpoint(ctx, "torvalds/linux@"+sha)
	if err != nil {
		t.Fatal(err)
	}
	var endpoint controlpb.MountEndpoint
	if err := proto.Unmarshal(data, &endpoint); err != nil {
		t.Fatal(err)
	}
	client := control.Client{Endpoint: endpoint.Socket}
	for {
		if _, err := client.Status(ctx); err == nil {
			break
		} else if strings.Contains(err.Error(), "Setup failed:") {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Logf("mount ready=%s", time.Since(start))
	for _, path := range []string{"lib", "Documentation/", "Documentation/"} {
		query := time.Now()
		var first time.Duration
		var got []string
		err := client.LogPaths(ctx, &controlpb.LogRequest{MaxCount: 10, Paths: [][]byte{[]byte(path)}, FullCommitIds: true}, func(entry *controlpb.LogEntry) error {
			if len(got) == 0 {
				first = time.Since(query)
			}
			got = append(got, entry.Sha)
			return nil
		})
		t.Logf("%s since start=%s first=%s query=%s results=%d", path, time.Since(start), first, time.Since(query), len(got))
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, "git", "-C", baseline, "log", "--format=%H", "-n", "10", sha, "--", path)
		cmd.Env = append(os.Environ(), "GIT_NO_LAZY_FETCH=1", "GIT_TERMINAL_PROMPT=0", "GIT_ALLOW_PROTOCOL=")
		want, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(got, "\n") != strings.TrimSpace(string(want)) {
			t.Fatalf("%s history differs: %v vs %s", path, got, want)
		}
	}
}
