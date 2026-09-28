package repo

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gyit/internal/store"
)

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
	backend, err := store.NewLocal(location)
	if err != nil {
		t.Fatal(err)
	}
	writes := &progressiveCountStore{Store: backend}
	disk, err := store.NewDiskCache(nil, t.TempDir(), "history-import", 128<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	p, err := NewProgressive(ctx, writes, disk, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sha := command(t, source, "rev-parse", "HEAD")
	if err = p.ImportPacks(ctx, source); err != nil {
		t.Fatal(err)
	}
	writes.bytes, writes.puts = 0, 0
	start := time.Now()
	if err = p.IngestHistory(ctx, sha, source); err != nil {
		t.Fatal(err)
	}
	t.Logf("ingestion: %s, %d durable bytes, %d writes", time.Since(start), writes.bytes, writes.puts)

	for _, path := range []string{"README.md", "AGENTS.md", "package.json"} {
		for _, n := range []int{10, 100} {
			start = time.Now()
			want := command(t, source, "-c", "color.ui=false", "log", "--no-decorate", "--format=medium", fmt.Sprintf("-n%d", n), sha, "--", path)
			native := time.Since(start)
			readerDisk, err := store.NewDiskCache(nil, t.TempDir(), "history-reader", 128<<20)
			if err != nil {
				t.Fatal(err)
			}
			ro := &historyReadOnlyStore{Store: backend}
			start = time.Now()
			reader, err := NewProgressive(ctx, ro, readerDisk, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := reader.Open(ctx, sha)
			if err != nil {
				t.Fatal(err)
			}
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
			t.Logf("first query %s n%d: %s; Git %s; %.2fx", path, n, elapsed, native, float64(elapsed)/float64(native))
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
