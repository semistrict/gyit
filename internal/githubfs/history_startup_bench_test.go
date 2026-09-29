package githubfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"gyit/internal/repo"
	"gyit/internal/store"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"gyit/internal/control"
	pb "gyit/internal/gen/gyit/control/v1"
)

// Run against a NEW disposable store prefix; the caller removes that prefix.
// Compare a blobless, single-branch bare clone (no checkout) with a fresh mount.
// Both sides must return the same first ten commits touching README.md.
func TestFreshFileHistoryStartup(t *testing.T) {
	root, project := os.Getenv("GYIT_STARTUP_STORE"), os.Getenv("GYIT_STARTUP_REPOSITORY")
	if root == "" || project == "" {
		t.Skip("set isolated GYIT_STARTUP_STORE and public owner/repository GYIT_STARTUP_REPOSITORY")
	}
	if len(strings.Split(project, "/")) != 2 {
		t.Fatal("expected owner/repository")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
	defer cancel()
	dir := t.TempDir()
	clone := filepath.Join(dir, "native.git")
	start := time.Now()
	cmd := exec.CommandContext(ctx, "git", "clone", "--bare", "--single-branch", "--filter=blob:none", "--quiet", "https://github.com/"+project+".git", clone)
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("clone: %v %s", err, out)
	}
	cloneTime := time.Since(start)
	revision := os.Getenv("GYIT_STARTUP_SHA")
	if revision == "" {
		revision = "HEAD"
	}
	shaBytes, err := exec.CommandContext(ctx, "git", "-C", clone, "rev-parse", "--verify", "--end-of-options", revision+"^{commit}").Output()
	if err != nil {
		t.Fatal(err)
	}
	sha := strings.TrimSpace(string(shaBytes))
	headBytes, err := exec.CommandContext(ctx, "git", "-C", clone, "rev-parse", "--verify", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	logStart := time.Now()
	want, err := exec.CommandContext(ctx, "git", "-C", clone, "log", "--format=%H", "-n", "10", sha, "--", "README.md").Output()
	if err != nil {
		t.Fatal(err)
	}
	nativeLog := time.Since(logStart)
	t.Logf("git clone=%s file-log=%s total=%s sha=%s cloned-tip=%s", cloneTime, nativeLog, time.Since(start), sha, strings.TrimSpace(string(headBytes)))

	f, err := New(Options{DataDir: filepath.Join(dir, "state"), CacheDir: filepath.Join(dir, "cache"), CacheBytes: 1 << 30, StoreRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	start = time.Now()
	data, err := f.Endpoint(ctx, project+"@"+sha)
	if err != nil {
		t.Fatal(err)
	}
	var ep pb.MountEndpoint
	if err := proto.Unmarshal(data, &ep); err != nil {
		t.Fatal(err)
	}
	client := control.Client{Endpoint: ep.Socket}
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
	// Observe phase changes without adding remote store reads to the benchmark.
	f.mu.Lock()
	j := f.jobs[(Target{Owner: strings.Split(project, "/")[0], Repository: strings.Split(project, "/")[1], Revision: sha}).Key()]
	f.mu.Unlock()
	watchDone := make(chan struct{})
	watchStopped := make(chan struct{})
	go func() {
		defer close(watchStopped)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		previous := ""
		for {
			j.mu.RLock()
			message := j.notice
			j.mu.RUnlock()
			if message != previous {
				t.Logf("phase at %s: %s", time.Since(start), message)
				previous = message
			}
			select {
			case <-watchDone:
				return
			case <-ticker.C:
			}
		}
	}()
	defer func() { close(watchDone); <-watchStopped }()
	var got []string
	err = client.LogPaths(ctx, &pb.LogRequest{MaxCount: 10, Paths: [][]byte{[]byte("README.md")}, FullCommitIds: true}, func(entry *pb.LogEntry) error {
		got = append(got, entry.Sha)
		if len(got) == 1 || len(got) == 10 {
			t.Logf("gyit result=%d elapsed=%s", len(got), time.Since(start))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if strings.Join(got, "\n") != strings.TrimSpace(string(want)) {
		t.Fatalf("file history differs: got %v want %s", got, want)
	}
	t.Logf("fresh first-ten gyit=%s git=%s ratio=%.2fx", elapsed, cloneTime+nativeLog, float64(elapsed)/float64(cloneTime+nativeLog))
	// Retain the native full clone until completed-store cold reads are compared.
	// This also proves background ingestion continues after the limited query.
	parts := strings.Split(project, "/")
	p, err := f.progressiveRepository(ctx, Target{Owner: parts[0], Repository: parts[1]})
	if err != nil {
		t.Fatal(err)
	}
	for {
		state, err := p.reader.State(ctx, sha)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			t.Fatal(err)
		}
		coverage, err := p.reader.HistoryProgress(ctx, sha)
		if err != nil {
			t.Fatal(err)
		}
		if coverage.Error != "" {
			t.Fatal(coverage.Error)
		}
		if state.HistoryComplete && state.SnapshotComplete {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Logf("complete snapshot and history at %s", time.Since(start))
	// Completion belongs to the data CAS. Graph compaction may still be running;
	// wait for the actual worker before comparing stable completed-store reads.
	if background, ok := p.background.Load(sha); ok {
		select {
		case <-background.(chan struct{}):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	t.Logf("background work finished at %s", time.Since(start))
	for _, path := range []string{"README.md", "AGENTS.md", "package.json"} {
		disk, err := store.NewDiskCache(nil, t.TempDir(), "cold-file-history", 32<<20)
		if err != nil {
			t.Fatal(err)
		}
		measured := &startupHistoryReadStore{Store: p.backend}
		coldStarted := time.Now()
		reader, err := repo.NewProgressive(ctx, measured, disk, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := reader.Open(ctx, sha)
		if err != nil {
			t.Fatal(err)
		}
		bootstrap := time.Since(coldStarted)
		coldStarted = time.Now()
		var output bytes.Buffer
		err = snapshot.LogWithOptions(ctx, repo.LogOptions{Count: 10, FullCommitIDs: true, Paths: []string{path}}, func(e repo.LogEntry) error {
			if output.Len() > 0 {
				output.WriteByte('\n')
			}
			return repo.WriteLogEntry(&output, e, false)
		})
		elapsed := time.Since(coldStarted)
		if err != nil {
			t.Fatal(err)
		}
		nativeStarted := time.Now()
		want, err := exec.CommandContext(ctx, "git", "-C", clone, "log", "--format=medium", "--no-color", "--no-decorate", "-n10", sha, "--", path).Output()
		native := time.Since(nativeStarted)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(output.Bytes(), want) {
			t.Fatalf("cold history differs for %s", path)
		}
		t.Logf("cold %s query=%s bootstrap=%s Git=%s ratio=%.2fx history_GETs=%d index_GETs=%d", path, elapsed, bootstrap, native, float64(elapsed)/float64(native), measured.history.Load(), measured.index.Load())
		if err := disk.Close(); err != nil {
			t.Fatal(err)
		}
	}

}

// Independent query readers cannot publish repository or query-specific data.
type startupHistoryReadStore struct {
	store.Store
	history, index atomic.Int64
}

func (s *startupHistoryReadStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if strings.HasPrefix(key, "index/progressive-history-") {
		s.history.Add(1)
	} else if strings.HasPrefix(key, "index/") {
		s.index.Add(1)
	}
	return s.Store.Get(ctx, key, off, n)
}
func (s *startupHistoryReadStore) Put(context.Context, string, []byte, string) error {
	return fmt.Errorf("file history reader attempted a write")
}
