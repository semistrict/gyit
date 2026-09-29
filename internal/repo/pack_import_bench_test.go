//go:build !js

package repo

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gyit/internal/store"
)

// A single immutable pack with enough recipes to spill several staging runs.
// Every iteration imports into a new store; acquisition is outside the timer.
func BenchmarkPackImport(b *testing.B) {
	source := b.TempDir()
	var stream strings.Builder
	for i := range 16384 {
		body := fmt.Sprintf("revision %d\n%s", i, strings.Repeat("content\n", 64))
		fmt.Fprintf(&stream, "blob\nmark :%d\ndata %d\n%s\n", 2*i+1, len(body), body)
		fmt.Fprintf(&stream, "commit refs/heads/main\nmark :%d\ncommitter Test <test@example.test> %d +0000\ndata 1\nx\n", 2*i+2, 1000000000+i)
		if i > 0 {
			fmt.Fprintf(&stream, "from :%d\n", 2*i)
		}
		fmt.Fprintf(&stream, "M 100644 :%d file\n\n", 2*i+1)
	}
	benchGit(b, source, "", "init", "-qb", "main")
	benchGit(b, source, stream.String(), "fast-import", "--quiet")
	benchGit(b, source, "", "repack", "-ad")
	sha := strings.TrimSpace(benchGit(b, source, "", "rev-parse", "HEAD"))
	parent := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		dir, err := os.MkdirTemp(parent, "iteration-*")
		if err != nil {
			b.Fatal(err)
		}
		backend, err := store.NewLocal(filepath.Join(dir, "objects"))
		if err != nil {
			b.Fatal(err)
		}
		p, err := NewProgressive(b.Context(), backend, nil, dir)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		err = p.ImportPacks(b.Context(), filepath.Join(source, ".git"))
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := p.Open(b.Context(), sha); err != nil {
			b.Fatal(err)
		}
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}
