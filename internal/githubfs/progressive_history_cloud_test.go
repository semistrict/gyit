package githubfs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gyit/internal/repo"
	"gyit/internal/store"
)

type historyMeasuredStore struct {
	store.Store
	mu      sync.Mutex
	calls   map[string]int
	elapsed map[string]time.Duration
}

func (s *historyMeasuredStore) record(key string, start time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[key]++
	s.elapsed[key] += time.Since(start)
}
func (s *historyMeasuredStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	start := time.Now()
	defer s.record("GET "+strings.Split(key, "/")[0], start)
	return s.Store.Get(ctx, key, off, n)
}
func (s *historyMeasuredStore) Put(ctx context.Context, key string, b []byte, c string) error {
	start := time.Now()
	defer s.record("PUT "+strings.Split(key, "/")[0], start)
	return s.Store.Put(ctx, key, b, c)
}

// This opt-in probe writes to an isolated test prefix. Never point it at a live
// writer's store. Seed a depth-one snapshot before timing; deeper history must
// be absent. Guest end-to-end measurements remain the acceptance test.
func TestCloudHistoryLogPerformance(t *testing.T) {
	root := os.Getenv("GYIT_HISTORY_BENCHMARK_STORE_ROOT")
	if root == "" {
		t.Skip("set an isolated GYIT_HISTORY_BENCHMARK_STORE_ROOT")
	}
	sha := os.Getenv("GYIT_HISTORY_BENCHMARK_SHA")
	if !fullSHA(sha) {
		t.Fatal("set GYIT_HISTORY_BENCHMARK_SHA")
	}
	dir := t.TempDir()
	f, err := New(Options{DataDir: filepath.Join(dir, "state"), CacheDir: filepath.Join(dir, "cache"), CacheBytes: 1 << 30, StoreRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	p, err := f.progressiveRepository(t.Context(), Target{Owner: "torvalds", Repository: "linux"})
	if err != nil {
		t.Fatal(err)
	}
	measured := &historyMeasuredStore{Store: p.backend, calls: map[string]int{}, elapsed: map[string]time.Duration{}}
	reader, err := repo.NewProgressive(t.Context(), measured, f.cache, dir)
	if err != nil {
		t.Fatal(err)
	}
	reader.Demand, reader.DemandCommits = p.reader.Demand, p.reader.DemandCommits
	p.reader = reader
	s, err := reader.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	for _, label := range []string{"cold-history", "warm"} {
		measured.calls = map[string]int{}
		measured.elapsed = map[string]time.Duration{}
		start := time.Now()
		var out strings.Builder
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		err = s.Log(ctx, 10, false, func(e repo.LogEntry) error { return repo.WriteLogEntry(&out, e, true) })
		cancel()
		t.Logf("%s elapsed=%s error=%v\n%s", label, time.Since(start), err, out.String())
		for key, n := range measured.calls {
			t.Logf("%s calls=%d time=%s", key, n, measured.elapsed[key])
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

func (s *historyMeasuredStore) PutVersion(ctx context.Context, key string, b []byte, c string) (string, error) {
	start := time.Now()
	defer s.record("PUT "+strings.Split(key, "/")[0], start)
	return s.Store.(store.VersionedWriter).PutVersion(ctx, key, b, c)
}
