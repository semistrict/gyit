package repo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"gyit/internal/store"
)

func TestFileLogBeyondVisitedMemoryWindow(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	var input strings.Builder
	for i := 1; i <= 4; i++ {
		fmt.Fprintf(&input, "blob\nmark :%d\ndata 1\n%d\n", i, i)
	}
	ancestors := 12010
	if value := os.Getenv("GYIT_TRAVERSAL_COMMITS"); value != "" {
		var err error
		ancestors, err = strconv.Atoi(value)
		if err != nil || ancestors < 12010 || ancestors > 150000 {
			t.Fatal("GYIT_TRAVERSAL_COMMITS must be 12010..150000")
		}
	}
	for i := 0; i < ancestors; i++ {
		fmt.Fprintf(&input, "commit refs/heads/main\nmark :%d\ncommitter Test <test@example.test> %d +0000\ndata 1\nx\n", 100+i, 1000000000+i)
		if i == 0 {
			input.WriteString("M 100644 :1 file\n")
		}
		input.WriteByte('\n')
	}
	fmt.Fprintf(&input, "commit refs/heads/left\nmark :200001\ncommitter Test <test@example.test> 1100000000 +0000\ndata 4\nleft\nfrom :%d\nM 100644 :2 file\n\n", 99+ancestors)
	fmt.Fprintf(&input, "commit refs/heads/right\nmark :200002\ncommitter Test <test@example.test> 1100000000 +0000\ndata 5\nright\nfrom :%d\nM 100644 :3 file\n\n", 99+ancestors)
	input.WriteString("commit refs/heads/main\ncommitter Test <test@example.test> 1200000000 +0000\ndata 5\nmerge\nfrom :200001\nmerge :200002\nM 100644 :4 file\n\n")
	verifyLargeFileHistory(t, dir, input.String())
}

func TestFileLogBeyondFrontierMemoryWindow(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	var input strings.Builder
	const nodes = 16383 // A two-parent merge tree with 8192 leaf candidates.
	for i := nodes; i > 0; i-- {
		value := fmt.Sprint(i)
		fmt.Fprintf(&input, "blob\nmark :%d\ndata %d\n%s\n", i, len(value), value)
		// Parents at the same depth have identical dates, exercising stable ties.
		depth := 0
		for n := i; n > 1; n /= 2 {
			depth++
		}
		fmt.Fprintf(&input, "commit refs/heads/main\nmark :%d\ncommitter Test <test@example.test> %d +0000\ndata 1\nx\n", 20000+i, 1100000000-depth)
		if 2*i <= nodes {
			fmt.Fprintf(&input, "from :%d\nmerge :%d\n", 20000+2*i, 20001+2*i)
		} else {
			input.WriteString("from 0000000000000000000000000000000000000000\n")
		}
		fmt.Fprintf(&input, "M 100644 :%d file\n\n", i)
	}
	verifyLargeFileHistory(t, dir, input.String())
}

func verifyLargeFileHistory(t *testing.T, dir, input string) {
	t.Helper()
	cmd := exec.Command("git", "-C", dir, "fast-import", "--quiet")
	cmd.Stdin = strings.NewReader(input)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fixture: %v %s", err, out)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	backend, _ := store.NewLocal(t.TempDir())
	scratch := t.TempDir()
	disk, err := store.NewDiskCache(nil, t.TempDir(), "large-history", 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	p, err := NewProgressive(ctx, backend, disk, scratch)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := p.ImportPacks(ctx, filepath.Join(dir, ".git")); err != nil {
		t.Fatalf("import: %v", err)
	}
	t.Logf("pack import %s", time.Since(started))
	started = time.Now()
	if err := p.IngestHistory(ctx, sha, filepath.Join(dir, ".git")); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	t.Logf("history ingestion %s", time.Since(started))
	started = time.Now()
	reader, err := NewProgressive(ctx, &historyReadOnlyStore{Store: backend}, disk, scratch)
	if err != nil {
		t.Fatal(err)
	}
	s, err := reader.Open(ctx, sha)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := s.LogWithOptions(ctx, LogOptions{Unlimited: true, FullCommitIDs: true, Paths: []string{"file"}}, func(e LogEntry) error { got = append(got, e.SHA); return nil }); err != nil {
		t.Fatal(err)
	}
	readTime := time.Since(started)
	started = time.Now()
	want := command(t, dir, "log", "--format=%H", "--", "file")
	nativeTime := time.Since(started)
	t.Logf("file history read %s; Git %s; %.2fx (ingestion cache retained)", readTime, nativeTime, float64(readTime)/float64(nativeTime))
	if strings.Join(got, "\n") != want {
		t.Fatalf("large history order differs: got %d entries, want %d", len(got), len(strings.Fields(want)))
	}
	entries, err := os.ReadDir(scratch)
	if err != nil || len(entries) != 0 {
		t.Fatalf("traversal left scratch data: %v %v", entries, err)
	}
}
