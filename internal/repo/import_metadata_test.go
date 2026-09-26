package repo

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"

	"gyit/internal/store"
)

// A large reachable graph exercises metadata preparation beyond a single
// in-memory batch. Compare every traversable parent with native Git, including
// a merge, and check that file sizes and contents survive the import.
func TestImportLargeMetadataGraph(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "--bare", "-q", "-b", "main", "--object-format=sha256")
	const commits = 65536
	var input strings.Builder
	input.WriteString("blob\nmark :1\ndata 7\nstable\n\n")
	for i := 0; i < commits; i++ {
		fmt.Fprintf(&input, "commit refs/heads/main\nmark :%d\ncommitter Test <test@example.test> %d +0000\ndata 7\nfixture\n", i+2, 1700000000+i)
		if i == 0 {
			input.WriteString("M 100644 :1 space name\n")
		}
		input.WriteByte('\n')
	}
	fmt.Fprintf(&input, "commit refs/heads/main\ncommitter Test <test@example.test> %d +0000\ndata 6\nmerge\nmerge :30000\n\n", 1700000000+commits)
	cmd := git(t.Context(), source, "-c", "gc.auto=0", "fast-import", "--quiet")
	cmd.Stdin = strings.NewReader(input.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fixture: %v %s", err, out)
	}
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()
	stats, err := Import(t.Context(), local, ImportOptions{Repo: source, TempDir: scratch})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Objects != commits+3 {
		t.Fatalf("imported %d objects, want %d", stats.Objects, commits+3)
	}
	r, _ := New(local, 32<<20)
	s, err := r.OpenRevision(t.Context(), "HEAD", "")
	if err != nil {
		t.Fatal(err)
	}
	want, got := sha256.New(), sha256.New()
	cmd = git(t.Context(), source, "rev-list", "--parents", "HEAD")
	cmd.Stdout = want
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	if err := r.ViewGraph(t.Context(), s, ViewOptions{Command: "rev-list", Args: []string{"--parents", "HEAD"}}, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Sum(nil), want.Sum(nil)) {
		t.Fatalf("parent graph differs from Git: %x != %x", got.Sum(nil), want.Sum(nil))
	}
	checkObjectViewParity(t, t.Context(), r, s, source, "", []string{"ls-tree", "-l", "HEAD"})
	checkObjectViewParity(t, t.Context(), r, s, source, "", []string{"cat-file", "-p", "HEAD:space name"})
	stats, err = Import(t.Context(), local, ImportOptions{Repo: source, TempDir: scratch})
	if err != nil || stats.Objects != 0 {
		t.Fatalf("unchanged import: objects=%d err=%v", stats.Objects, err)
	}
	if files, err := os.ReadDir(scratch); err != nil || len(files) != 0 {
		t.Fatalf("import left temporary metadata files: %v, err=%v", files, err)
	}
}
