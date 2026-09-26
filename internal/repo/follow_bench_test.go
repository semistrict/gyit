package repo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gyit/internal/store"
)

// Exercise edited-rename discovery, not just --follow on an unchanged pathname.
func BenchmarkLogEditedRename(b *testing.B) {
	source := b.TempDir()
	git := func(args ...string) string {
		b.Helper()
		cmd := exec.Command("git", append([]string{"-C", source}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
		data, err := cmd.CombinedOutput()
		if err != nil {
			b.Fatalf("fixture git: %v %s", err, data)
		}
		return strings.TrimSpace(string(data))
	}
	git("init", "-qb", "main")
	for i := 0; i < 256; i++ {
		text := strings.Repeat(fmt.Sprintf("unique file %04d contents for rename comparison\n", i), 40)
		if err := os.WriteFile(filepath.Join(source, fmt.Sprintf("file-%04d", i)), []byte(text), 0644); err != nil {
			b.Fatal(err)
		}
	}
	git("add", ".")
	git("commit", "-qm", "sources")
	git("mv", "file-0199", "destination")
	file, err := os.OpenFile(filepath.Join(source, "destination"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := file.WriteString("newly appended content\n"); err != nil {
		b.Fatal(err)
	}
	if err := file.Close(); err != nil {
		b.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "edited rename")
	tip := git("rev-parse", "HEAD")
	local, err := store.NewLocal(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	if _, err := Import(b.Context(), local, ImportOptions{Repo: source}); err != nil {
		b.Fatal(err)
	}
	measured := &countedStore{Store: local}
	gets, fetched := 0, 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := New(measured, 32<<20)
		s, err := r.Open(context.Background(), tip)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		if err := s.LogWithOptions(context.Background(), LogOptions{Count: 20, Follow: true, Paths: []string{"destination"}}, func(LogEntry) error { n++; return nil }); err != nil {
			b.Fatal(err)
		}
		if n != 2 {
			b.Fatalf("got %d commits", n)
		}
		gets += measured.gets
		fetched += measured.bytes
		measured.reset()
	}
	b.StopTimer()
	b.ReportMetric(float64(gets)/float64(b.N), "GETs/op")
	b.ReportMetric(float64(fetched)/float64(b.N), "fetched-B/op")
}
