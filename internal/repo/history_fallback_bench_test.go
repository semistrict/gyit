//go:build !js

package repo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gyit/internal/store"
)

// Compare the production reader (including publication checks) with native Git
// on the same existing acquisition. Set GYIT_HISTORY_REMOTE_ONLY=1 to deny the
// reader local acquisition access; the source then belongs only to Git's baseline.
// Otherwise acquisition-file locality is an explicit measurement condition.
func TestPublishedHistoryLogLatency(t *testing.T) {
	location, dir, sha := os.Getenv("GYIT_HISTORY_STORE"), os.Getenv("GYIT_HISTORY_SOURCE"), os.Getenv("GYIT_HISTORY_SHA")
	if location == "" || dir == "" || sha == "" {
		t.Skip("set GYIT_HISTORY_STORE, GYIT_HISTORY_SOURCE and GYIT_HISTORY_SHA")
	}
	readTimeout := 20 * time.Second
	if value := os.Getenv("GYIT_HISTORY_READ_TIMEOUT"); value != "" {
		var err error
		readTimeout, err = time.ParseDuration(value)
		if err != nil || readTimeout <= 0 {
			t.Fatal("invalid GYIT_HISTORY_READ_TIMEOUT")
		}
	}
	paths := []string{"Kconfig", "COPYING", "README"}
	if path := os.Getenv("GYIT_HISTORY_PATH"); path != "" {
		paths = []string{path}
	}
	for _, path := range paths {
		for _, count := range []int{1, 10} {
			t.Run(fmt.Sprintf("%s/n%d", path, count), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), readTimeout)
				defer cancel()
				start := time.Now()
				cmd := exec.CommandContext(ctx, "git", "-C", dir, "log", "--format=%H", "-n", fmt.Sprint(count), sha, "--", path)
				cmd.Env = append(os.Environ(), "GIT_NO_LAZY_FETCH=1", "GIT_TERMINAL_PROMPT=0", "GIT_ALLOW_PROTOCOL=")
				want, err := cmd.Output()
				if err != nil {
					t.Fatal(err)
				}
				native := time.Since(start)
				backend, err := store.Open(ctx, location, "", "")
				if err != nil {
					t.Fatal(err)
				}
				trace := &historyObjectTraceStore{Store: backend}
				disk, err := store.NewDiskCache(nil, t.TempDir(), "history-locality-probe", 32<<20)
				if err != nil {
					t.Fatal(err)
				}
				defer disk.Close()
				start = time.Now()
				p, err := NewProgressive(ctx, trace, disk, t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				if os.Getenv("GYIT_HISTORY_REMOTE_ONLY") != "1" {
					p.UsePublishedHistorySources(dir)
				}
				s, err := p.Open(ctx, sha)
				if err != nil {
					t.Fatal(err)
				}
				bootstrap := time.Since(start)
				reads, bytes := trace.reads.Load(), trace.bytes.Load()
				start = time.Now()
				var got []string
				var first time.Duration
				err = s.LogWithOptions(ctx, LogOptions{Count: count, Paths: []string{path}, FullCommitIDs: true}, func(e LogEntry) error {
					if len(got) == 0 {
						first = time.Since(start)
					}
					got = append(got, e.SHA)
					return nil
				})
				elapsed := time.Since(start)
				t.Logf("bootstrap=%s first=%s query=%s Git=%s ratio=%.2fx GETs=%d bytes=%d results=%d", bootstrap, first, elapsed, native, float64(elapsed)/float64(native), trace.reads.Load()-reads, trace.bytes.Load()-bytes, len(got))
				t.Logf("request classes, including bootstrap: %s", trace.classes())
				if err != nil {
					t.Fatal(err)
				}
				if strings.Join(got, "\n") != strings.TrimSpace(string(want)) {
					t.Fatalf("history differs: %v != %s", got, want)
				}
			})
		}
	}
}

type historyObjectTraceStore struct {
	store.Store
	reads, bytes atomic.Int64
	kinds        [4]struct{ reads, bytes atomic.Int64 }
}

func (s *historyObjectTraceStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	s.reads.Add(1)
	b, token, err := s.Store.Get(ctx, key, off, n)
	s.bytes.Add(int64(len(b)))
	i := -1
	switch {
	case strings.HasPrefix(key, "index/progressive-history-v2-graph-"):
		i = 0
	case strings.HasPrefix(key, "index/progressive-history-v2-"):
		i = 1
	case strings.HasPrefix(key, "index/"):
		i = 2
	case strings.HasPrefix(key, "packs/"):
		i = 3
	}
	if i >= 0 {
		s.kinds[i].reads.Add(1)
		s.kinds[i].bytes.Add(int64(len(b)))
	}
	return b, token, err
}

func (s *historyObjectTraceStore) classes() string {
	var parts []string
	for i, name := range []string{"graph", "paths/display", "global-index", "raw-pack"} {
		parts = append(parts, fmt.Sprintf("%s=%d GETs/%d bytes", name, s.kinds[i].reads.Load(), s.kinds[i].bytes.Load()))
	}
	return strings.Join(parts, "; ")
}

// This opt-in diagnostic reads existing durable objects only, with no local
// acquisition view. It measures the remote access cost for one commit/path
// comparison, not an entire history query and not an import.
func TestStoreUnindexedHistoryAccess(t *testing.T) {
	location, sha := os.Getenv("GYIT_HISTORY_STORE"), os.Getenv("GYIT_HISTORY_SHA")
	if location == "" || sha == "" {
		t.Skip("set GYIT_HISTORY_STORE and GYIT_HISTORY_SHA")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	backend, err := store.Open(ctx, location, "", "")
	if err != nil {
		t.Fatal(err)
	}
	trace := &historyObjectTraceStore{Store: backend}
	disk, err := store.NewDiskCache(nil, t.TempDir(), "history-object-probe", 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	start := time.Now()
	p, err := NewProgressive(ctx, trace, disk, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("bootstrap=%s GETs=%d bytes=%d", time.Since(start), trace.reads.Load(), trace.bytes.Load())
	reader := &unindexedHistoryReader{p: p, path: "Kconfig"}
	comparisons := 2
	walk := os.Getenv("GYIT_HISTORY_WALK")
	if walk != "" {
		comparisons, err = strconv.Atoi(walk)
		if err != nil || comparisons < 1 || comparisons > 256 {
			t.Fatal("GYIT_HISTORY_WALK must be between 1 and 256")
		}
	}
	for i := range comparisons {
		start := time.Now()
		reads, bytes := trace.reads.Load(), trace.bytes.Load()
		c, mask, err := reader.load(ctx, sha)
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprintf("%x", c.Oid) != sha || len(mask) == 0 {
			t.Fatal("invalid path comparison")
		}
		t.Logf("comparison=%d elapsed=%s GETs=%d bytes=%d parents=%d mask=%x", i, time.Since(start), trace.reads.Load()-reads, trace.bytes.Load()-bytes, len(c.Parents), mask)
		if walk != "" {
			if len(c.Parents) == 0 {
				break
			}
			sha = fmt.Sprintf("%x", c.Parents[0])
		}
	}
}

// Isolate Go query CPU from acquisition, remote I/O and all-path preparation.
// This uses existing immutable acquisition files as benchmark input only; it
// does not establish object-store latency or authorize unpublished reader data.
func BenchmarkRepositoryUnindexedFileLog(b *testing.B) {
	dir := os.Getenv("GYIT_HISTORY_SOURCE")
	if dir == "" {
		b.Skip("set GYIT_HISTORY_SOURCE to an existing packed repository")
	}
	revision := os.Getenv("GYIT_HISTORY_SHA")
	if revision == "" {
		revision = "HEAD"
	}
	path := os.Getenv("GYIT_HISTORY_PATH")
	if path == "" {
		path = "Kconfig"
	}
	count := 10
	if value := os.Getenv("GYIT_HISTORY_COUNT"); value != "" {
		var err error
		count, err = strconv.Atoi(value)
		if err != nil || count < 1 || count > MaxLogCount {
			b.Fatal("invalid GYIT_HISTORY_COUNT")
		}
	}
	sha := strings.TrimSpace(benchGit(b, dir, "", "rev-parse", "--verify", revision+"^{commit}"))
	want := strings.TrimSpace(benchGit(b, dir, "", "log", "--format=%H", "-n", strconv.Itoa(count), sha, "--", path))
	local, err := store.NewLocal(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	p, err := NewProgressive(b.Context(), local, nil, b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	s := &Snapshot{SHA: sha, progressive: p, idx: p.historyIndex()}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		source, err := openHistorySource([]string{dir})
		if err != nil {
			b.Fatal(err)
		}
		ctx := context.WithValue(b.Context(), historySourceKey{}, source)
		b.StartTimer()
		var got []string
		err = s.LogWithOptions(ctx, LogOptions{Count: count, FullCommitIDs: true, Paths: []string{path}}, func(e LogEntry) error { got = append(got, e.SHA); return nil })
		b.StopTimer()
		source.close()
		if err != nil {
			b.Fatal(err)
		}
		if strings.Join(got, "\n") != want {
			b.Fatalf("history differs: %v != %s", got, want)
		}
		b.StartTimer()
	}
}
