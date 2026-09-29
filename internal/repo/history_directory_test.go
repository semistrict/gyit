package repo

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gyit/internal/store"
)

// A trailing slash selects only descendants. A same-named file (or symlink)
// must not contribute history, including across replacements and merges.
func TestDirectoryHistoryTypeChanges(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for i := range 2 {
		write(t, dir, "docs", []byte(fmt.Sprint(i)))
		commit(t, dir)
	}
	command(t, dir, "rm", "docs")
	write(t, dir, "docs/nested/file", []byte("first"))
	commit(t, dir)
	command(t, dir, "branch", "side")
	write(t, dir, "docs/nested/file", []byte("main"))
	commit(t, dir)
	command(t, dir, "checkout", "-q", "side")
	write(t, dir, "docs/other", []byte("side"))
	commit(t, dir)
	command(t, dir, "checkout", "-q", "main")
	command(t, dir, "merge", "--no-ff", "-m", "merge", "side")
	command(t, dir, "rm", "-r", "docs")
	if err := os.Symlink("elsewhere", filepath.Join(dir, "docs")); err != nil {
		t.Fatal(err)
	}
	commit(t, dir)
	command(t, dir, "rm", "docs")
	write(t, dir, "docs", []byte("file again"))
	commit(t, dir)
	write(t, dir, "docs", []byte("file edited"))
	commit(t, dir)
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprint(indexed), func(t *testing.T) {
			backend, _ := store.NewLocal(t.TempDir())
			p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
				t.Fatal(err)
			}
			if indexed {
				if err := p.IngestHistory(t.Context(), sha, filepath.Join(dir, ".git")); err != nil {
					t.Fatal(err)
				}
			}
			s, err := p.Open(t.Context(), sha)
			if err != nil {
				t.Fatal(err)
			}
			// Fail if a query fetches blobs, acquires more history, or scans raw packs
			// when complete all-path postings already contain the answer.
			p.Demand = func(context.Context, []string) error { return fmt.Errorf("unexpected acquisition") }
			if indexed {
				p.store = &historyReadOnlyStore{Store: backend, denyPacks: true}
			} else {
				p.UsePublishedHistorySources(filepath.Join(dir, ".git"))
			}
			for _, path := range []string{"docs/", "docs", "./", "docs/nested/", "missing/"} {
				for _, firstParent := range []bool{false, true} {
					var got []string
					err := s.LogWithOptions(t.Context(), LogOptions{Unlimited: true, FullCommitIDs: true, FirstParent: firstParent, Paths: []string{path}}, func(e LogEntry) error { got = append(got, e.SHA); return nil })
					if err != nil {
						t.Fatalf("%s: %v", path, err)
					}
					args := []string{"log", "--format=%H"}
					if firstParent {
						args = append(args, "--first-parent")
					}
					args = append(args, sha, "--", path)
					want := command(t, dir, args...)
					if strings.Join(got, "\n") != want {
						t.Fatalf("%s first-parent=%v\ngot %v\nwant %s", path, firstParent, got, want)
					}
				}
			}
		})
	}
}
