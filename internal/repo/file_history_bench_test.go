package repo

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gyit/internal/store"
)

type historyBenchmarkStore struct {
	store.Store
	mu       sync.Mutex
	calls    map[string]int
	elapsed  map[string]time.Duration
	trace    bool
	origin   time.Time
	requests []string
}

func (s *historyBenchmarkStore) record(op, key string, start time.Time) {
	family := strings.Split(key, "/")[0]
	if strings.HasPrefix(key, "index/progressive-history-") {
		family = "history"
	}
	label := op + " " + family
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[label]++
	s.elapsed[label] += time.Since(start)
	if s.trace {
		if s.origin.IsZero() {
			s.origin = start
		}
		s.requests = append(s.requests, fmt.Sprintf("%s %s start=%s elapsed=%s", op, key, start.Sub(s.origin), time.Since(start)))
	}
}
func (s *historyBenchmarkStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	start := time.Now()
	defer s.record("GET", key, start)
	return s.Store.Get(ctx, key, off, n)
}
func (s *historyBenchmarkStore) Put(ctx context.Context, key string, b []byte, c string) error {
	start := time.Now()
	defer s.record("PUT", key, start)
	return s.Store.Put(ctx, key, b, c)
}

// Opt-in, repeatable real-repository test. The acquisition fixture is reused;
// the durable output and reader caches are fresh for every run.
func TestFileHistoryMedium(t *testing.T) {
	source := os.Getenv("GYIT_HISTORY_SOURCE")
	if source == "" {
		t.Skip("set GYIT_HISTORY_SOURCE to a packed bare repository")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	location := os.Getenv("GYIT_HISTORY_STORE")
	if location == "" {
		location = t.TempDir()
	}
	backend, err := store.Open(ctx, location, "", "")
	if err != nil {
		t.Fatal(err)
	}
	measured := &historyBenchmarkStore{Store: backend, calls: map[string]int{}, elapsed: map[string]time.Duration{}}
	defer func() {
		measured.mu.Lock()
		defer measured.mu.Unlock()
		for op, n := range measured.calls {
			t.Logf("%s count=%d summed=%s mean=%s", op, n, measured.elapsed[op], measured.elapsed[op]/time.Duration(n))
		}
	}()
	writes := &progressiveCountStore{Store: measured}
	disk, err := store.NewDiskCache(nil, t.TempDir(), "history-import", 128<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	p, err := NewProgressive(ctx, writes, disk, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sha := os.Getenv("GYIT_HISTORY_SHA")
	if sha == "" {
		sha = command(t, source, "rev-parse", "HEAD")
	}
	packStart := time.Now()
	if err = p.ImportPacks(ctx, source); err != nil {
		t.Fatal(err)
	}
	t.Logf("pack import: %s", time.Since(packStart))
	writes.bytes, writes.puts = 0, 0
	// Begin the reader before indexing. The callbacks measure first visibility
	// and the first page, rather than timing only a fully built index.
	streamReader, err := NewProgressive(ctx, backend, disk, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := streamReader.Open(ctx, sha)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	type streamResult struct {
		first, ten time.Duration
		ids        []string
		err        error
	}
	streamed := make(chan streamResult, 1)
	go func() {
		result := streamResult{}
		result.err = snapshot.LogWithOptions(ctx, LogOptions{Count: 10, FullCommitIDs: true, Paths: []string{"README.md"}}, func(e LogEntry) error {
			result.ids = append(result.ids, e.SHA)
			if len(result.ids) == 1 {
				result.first = time.Since(start)
				t.Logf("first streamed result: %s", result.first)
			}
			if len(result.ids) == 10 {
				result.ten = time.Since(start)
				t.Logf("first ten streamed results: %s", result.ten)
			}
			return nil
		})
		streamed <- result
	}()
	if err = p.IngestHistory(ctx, sha, source); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	result := <-streamed
	if result.err != nil {
		t.Fatal(result.err)
	}
	wantIDs := command(t, source, "log", "--format=%H", "-n10", sha, "--", "README.md")
	if strings.Join(result.ids, "\n") != wantIDs {
		t.Fatal("streamed output differs from Git")
	}
	t.Logf("stream first=%s first10=%s; ingestion=%s, %d durable bytes, %d writes", result.first, result.ten, elapsed, writes.bytes, writes.puts)

	for _, path := range []string{"README.md", "AGENTS.md", "package.json"} {
		for _, n := range []int{10, 100} {
			start = time.Now()
			want := command(t, source, "-c", "color.ui=false", "log", "--no-decorate", "--format=medium", fmt.Sprintf("-n%d", n), sha, "--", path)
			native := time.Since(start)
			readerDisk, err := store.NewDiskCache(nil, t.TempDir(), "history-reader", 128<<20)
			if err != nil {
				t.Fatal(err)
			}
			reads := &historyBenchmarkStore{Store: backend, calls: map[string]int{}, elapsed: map[string]time.Duration{}}
			ro := &historyReadOnlyStore{Store: reads}
			start = time.Now()
			reader, err := NewProgressive(ctx, ro, readerDisk, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := reader.Open(ctx, sha)
			if err != nil {
				t.Fatal(err)
			}
			bootstrap := time.Since(start)
			start = time.Now()
			ro.denyPacks = true
			var out bytes.Buffer
			err = snapshot.LogWithOptions(ctx, LogOptions{Count: n, Paths: []string{path}, FullCommitIDs: true}, func(e LogEntry) error {
				if out.Len() > 0 {
					out.WriteByte('\n')
				}
				return WriteLogEntry(&out, e, false)
			})
			elapsed := time.Since(start)
			readerDisk.Close()
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimSpace(out.String()) != want {
				os.WriteFile("/tmp/gyit-history-got", out.Bytes(), 0600)
				os.WriteFile("/tmp/gyit-history-want", []byte(want+"\n"), 0600)
				t.Fatalf("%s n%d output differs from Git (details in /tmp/gyit-history-{got,want})", path, n)
			}
			t.Logf("first mounted query %s n%d: %s; Git %s; %.2fx; bootstrap=%s total=%s; requests=%v", path, n, elapsed, native, float64(elapsed)/float64(native), bootstrap, bootstrap+elapsed, reads.calls)
		}
	}
}

func BenchmarkFileHistoryFirstQuery(b *testing.B) {
	source := b.TempDir()
	// Fast-import creates a long history whose selected path changes sparsely.
	var stream strings.Builder
	stream.WriteString("blob\nmark :1\ndata 1\nx\n")
	for i := 0; i < 2000; i++ {
		fmt.Fprintf(&stream, "commit refs/heads/main\nmark :%d\ncommitter Test <test@example.test> %d +0000\ndata 1\nx\n", 100+i, 1000000000+i)
		if i > 0 {
			fmt.Fprintf(&stream, "from :%d\n", 99+i)
		}
		if i%100 == 0 {
			fmt.Fprintf(&stream, "M 100644 :1 history-%d\n", i)
		}
		stream.WriteString("\n")
	}
	benchGit(b, source, "", "init", "-qb", "main")
	benchGit(b, source, stream.String(), "fast-import", "--quiet")
	sha := strings.TrimSpace(benchGit(b, source, "", "rev-parse", "HEAD"))
	backend, _ := store.NewLocal(b.TempDir())
	p, err := NewProgressive(b.Context(), backend, nil, b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	if err = p.ImportPacks(b.Context(), filepath.Join(source, ".git")); err != nil {
		b.Fatal(err)
	}
	if err = p.IngestHistory(b.Context(), sha, filepath.Join(source, ".git")); err != nil {
		b.Fatal(err)
	}
	// Prepare outside timing; the measurement starts with a new reader each time.
	if err = p.PrepareSnapshot(b.Context(), sha); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ro := &historyReadOnlyStore{Store: backend}
		reader, err := NewProgressive(b.Context(), ro, nil, source)
		if err != nil {
			b.Fatal(err)
		}
		s, err := reader.Open(b.Context(), sha)
		if err != nil {
			b.Fatal(err)
		}
		ro.denyPacks = true
		count := 0
		err = s.LogWithOptions(b.Context(), LogOptions{Count: 10, Paths: []string{"history-0"}, FullCommitIDs: true}, func(LogEntry) error { count++; return nil })
		if err != nil {
			b.Fatal(err)
		}
		if count != 1 {
			b.Fatalf("got %d entries", count)
		}
	}
}

func benchGit(b *testing.B, dir, input string, args ...string) string {
	b.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Stdin = strings.NewReader(input)
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	out, err := cmd.CombinedOutput()
	if err != nil {
		b.Fatalf("%v: %v %s", args, err, out)
	}
	return string(out)
}

// Read-only probe of a live immutable publication. Each path starts with an
// empty decoded cache. It never imports, prepares metadata, or writes to Store.
func TestFileHistoryReadPerformance(t *testing.T) {
	location, source, sha := os.Getenv("GYIT_HISTORY_READ_STORE"), os.Getenv("GYIT_HISTORY_SOURCE"), os.Getenv("GYIT_HISTORY_SHA")
	if location == "" || source == "" || sha == "" {
		t.Skip("set GYIT_HISTORY_READ_STORE, GYIT_HISTORY_SOURCE and GYIT_HISTORY_SHA")
	}
	backend, err := store.Open(t.Context(), location, "", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"README.md", "AGENTS.md", "package.json"} {
		disk, err := store.NewDiskCache(nil, t.TempDir(), "read-history", 32<<20)
		if err != nil {
			t.Fatal(err)
		}
		measured := &historyBenchmarkStore{Store: backend, calls: map[string]int{}, elapsed: map[string]time.Duration{}, trace: true}
		p, err := NewProgressive(t.Context(), &historyReadOnlyStore{Store: measured}, disk, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		s, err := p.Open(t.Context(), sha)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		var output bytes.Buffer
		err = s.LogWithOptions(t.Context(), LogOptions{Count: 10, FullCommitIDs: true, Paths: []string{path}}, func(e LogEntry) error {
			if output.Len() > 0 {
				output.WriteByte('\n')
			}
			return WriteLogEntry(&output, e, false)
		})
		elapsed := time.Since(start)
		if err != nil {
			t.Fatal(err)
		}
		start = time.Now()
		want, err := exec.Command("git", "-C", source, "log", "--format=medium", "--no-color", "--no-decorate", "-n10", sha, "--", path).Output()
		native := time.Since(start)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(output.Bytes(), want) {
			t.Fatal("file history differs from Git")
		}
		t.Logf("%s gyit=%s Git=%s ratio=%.2fx requests=%v", path, elapsed, native, float64(elapsed)/float64(native), measured.calls)
		for _, request := range measured.requests {
			t.Log(request)
		}
		if err := disk.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
