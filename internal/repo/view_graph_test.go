package repo

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gyit/internal/store"
)

func graphGit(t *testing.T, dir string, args ...string) ([]byte, int) {
	t.Helper()
	c := exec.Command("git", append([]string{"-C", dir}, args...)...)
	c.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "LC_ALL=C", "GIT_PAGER=cat")
	b, err := c.Output()
	if err == nil {
		return b, 0
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return b, exit.ExitCode()
	}
	t.Fatal(err)
	return nil, 0
}
func graphParity(t *testing.T, r *Repository, s *Snapshot, source, command string, args ...string) {
	t.Helper()
	want, code := graphGit(t, source, append([]string{command}, args...)...)
	var got bytes.Buffer
	err := r.ViewGraph(context.Background(), s, ViewOptions{Command: command, Args: args}, &got)
	actual := 0
	if errors.Is(err, ErrViewNoMatch) {
		actual = 1
	} else if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want) || actual != code {
		t.Fatalf("%s %v\noutput=%q status=%d\nGit=%q status=%d", command, args, got.String(), actual, want, code)
	}
}
func TestViewGraphNativeParity(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	command(t, dir, "init", "-q", "-b", "main")
	write(t, dir, "base", []byte("base\n"))
	root := commit(t, dir)
	command(t, dir, "checkout", "-qb", "side")
	write(t, dir, "side", []byte("side\n"))
	command(t, dir, "add", ".")
	command(t, dir, "commit", "-qm", "Side work", "--author=Other <other@example.test>")
	side := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "checkout", "-q", "main")
	write(t, dir, "main", []byte("main\n"))
	command(t, dir, "add", ".")
	command(t, dir, "commit", "-qm", "[PATCH v2] Main work")
	command(t, dir, "merge", "-q", "--no-ff", "side", "-m", "Merge side")
	write(t, dir, ".mailmap", []byte("Mapped Author <mapped@example.test> Other <other@example.test>\nGeneric Author <generic@example.test> <other@example.test>\n"))
	commit(t, dir)
	local, _ := store.NewLocal(t.TempDir())
	if _, err := Import(ctx, local, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	r, _ := New(local, 1<<20)
	s, err := r.OpenRevision(ctx, "main", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"HEAD"}, {"--count", "HEAD"}, {"--parents", "HEAD"}, {"--first-parent", "HEAD"}, {"-n", "2", "HEAD"}, {"HEAD", "--max-count=2"}, {"--reverse", "--max-count=3", "HEAD"}, {root + "..HEAD"}, {"HEAD", "^" + side}, {"--count", root + "..HEAD"}, {"--max-count=0", "HEAD"}} {
		t.Run("rev-list/"+strings.Join(args, " "), func(t *testing.T) { graphParity(t, r, s, dir, "rev-list", args...) })
	}
	for _, args := range [][]string{{"main", "side"}, {"--all", "side", "main"}, {"--is-ancestor", root, "HEAD"}, {"--is-ancestor", "main", "side"}} {
		t.Run("merge-base/"+strings.Join(args, " "), func(t *testing.T) { graphParity(t, r, s, dir, "merge-base", args...) })
	}
	for _, args := range [][]string{{"HEAD"}, {"-s", "HEAD"}, {"-sne", "HEAD"}, {"--summary", "--numbered", "--email", root + "..HEAD"}, {"--first-parent", "HEAD"}, {"--no-merges", "HEAD"}, {"--no-merges", "--max-count=2", "HEAD"}} {
		t.Run("shortlog/"+strings.Join(args, " "), func(t *testing.T) { graphParity(t, r, s, dir, "shortlog", args...) })
	}
	// Resolve every endpoint against one publication but use the mounted HEAD.
	pinned, err := r.OpenRevision(ctx, root, "")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := r.ViewGraph(ctx, pinned, ViewOptions{Command: "rev-list", Args: []string{"--count", "HEAD"}}, &out); err != nil {
		t.Fatal(err)
	}
	if out.String() != "1\n" {
		t.Fatalf("mounted HEAD ignored: %q", out.String())
	}
	for _, q := range []ViewOptions{{Command: "rev-list", Args: []string{"--bogus", "HEAD"}}, {Command: "rev-list", Args: []string{"HEAD...side"}}, {Command: "merge-base", Args: []string{"HEAD"}}, {Command: "shortlog", Args: []string{"--group=committer"}}} {
		if err := r.ViewGraph(ctx, s, q, io.Discard); err == nil {
			t.Fatalf("unsupported request accepted: %+v", q)
		}
	}
}
func TestViewGraphCrissCross(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	command(t, dir, "init", "-q", "-b", "main")
	write(t, dir, "base", []byte("base"))
	root := commit(t, dir)
	tree := command(t, dir, "rev-parse", "HEAD^{tree}")
	a := command(t, dir, "commit-tree", tree, "-p", root, "-m", "A")
	b := command(t, dir, "commit-tree", tree, "-p", root, "-m", "B")
	left := command(t, dir, "commit-tree", tree, "-p", a, "-p", b, "-m", "left")
	right := command(t, dir, "commit-tree", tree, "-p", b, "-p", a, "-m", "right")
	command(t, dir, "update-ref", "refs/heads/left", left)
	command(t, dir, "update-ref", "refs/heads/right", right)
	disconnected := command(t, dir, "commit-tree", tree, "-m", "disconnected")
	command(t, dir, "update-ref", "refs/heads/disconnected", disconnected)
	local, _ := store.NewLocal(t.TempDir())
	if _, err := Import(ctx, local, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	r, _ := New(local, 1<<20)
	s, _ := r.OpenRevision(ctx, "main", "")
	graphParity(t, r, s, dir, "merge-base", "left", "disconnected")
	graphParity(t, r, s, dir, "merge-base", "--is-ancestor", "left", "disconnected")
	want, code := graphGit(t, dir, "merge-base", "--all", "left", "right")
	var got bytes.Buffer
	if err := r.ViewGraph(ctx, s, ViewOptions{Command: "merge-base", Args: []string{"--all", "left", "right"}}, &got); err != nil {
		t.Fatal(err)
	}
	w, g := strings.Fields(string(want)), strings.Fields(got.String())
	if code != 0 || len(w) != 2 || len(g) != 2 || !(g[0] == w[0] && g[1] == w[1] || g[1] == w[0] && g[0] == w[1]) {
		t.Fatalf("criss-cross bases mismatch")
	}
}
func TestViewGraphMediumParity(t *testing.T) {
	if os.Getenv("GYIT_MEDIUM_PARITY_TEST") == "" {
		t.Skip("set GYIT_MEDIUM_PARITY_TEST=1 to compare the existing medium fixture")
	}
	source := "../../.testdata/medium-repo.git"
	path := "../../.testdata/lima-store"
	local, err := store.NewLocal(path)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := New(local, 32<<20)
	sha := "595cc91e8cbb1c2ca822d0311dcf12709410c582"
	s, err := r.OpenRevision(context.Background(), sha, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []ViewOptions{{Command: "rev-list", Args: []string{"--parents", "--max-count=100", sha}}, {Command: "rev-list", Args: []string{"--count", sha + "~100.." + sha}}, {Command: "merge-base", Args: []string{sha, sha + "~100"}}, {Command: "shortlog", Args: []string{"-sne", sha + "~100.." + sha}}, {Command: "shortlog", Args: []string{"-sne", sha}}, {Command: "rev-list", Args: []string{"--count", sha}}} {
		t.Run(q.Command+"/"+strings.Join(q.Args, " "), func(t *testing.T) {
			start := time.Now()
			graphParity(t, r, s, source, q.Command, q.Args...)
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Logf("SLOW: comparison including native Git took %s", elapsed)
			}
		})
	}
}
func BenchmarkViewGraphMedium(b *testing.B) {
	path := os.Getenv("GYIT_BENCH_STORE")
	if path == "" {
		path = "../../.testdata/lima-store"
	}
	if _, err := os.Stat(filepath.Join(path, "HEAD")); err != nil {
		b.Skip("import medium fixture first")
	}
	sha := "595cc91e8cbb1c2ca822d0311dcf12709410c582"
	for _, q := range []ViewOptions{{Command: "rev-list", Args: []string{"--max-count=100", "HEAD"}}, {Command: "rev-list", Args: []string{"--count", "HEAD"}}, {Command: "merge-base", Args: []string{"HEAD", "HEAD~100"}}, {Command: "shortlog", Args: []string{"-sne", "HEAD"}}} {
		name := q.Command
		if q.Command == "rev-list" && q.Args[0] == "--count" {
			name += "-count"
		}
		for _, warm := range []bool{false, true} {
			temperature := "fresh"
			if warm {
				temperature = "reused"
			}
			b.Run(name+"/"+temperature, func(b *testing.B) {
				local, _ := store.NewLocal(path)
				counted := &countedStore{Store: local}
				open := func() (*Repository, *Snapshot) {
					r, _ := New(counted, 32<<20)
					s, err := r.OpenRevision(context.Background(), sha, "")
					if err != nil {
						b.Fatal(err)
					}
					return r, s
				}
				r, s := open()
				run := func() {
					if err := r.ViewGraph(context.Background(), s, q, io.Discard); err != nil {
						b.Fatal(err)
					}
				}
				if warm {
					run()
				}
				counted.reset()
				gets, bytes := 0, 0
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if !warm {
						b.StopTimer()
						r, s = open()
						counted.reset()
						b.StartTimer()
					}
					started := time.Now()
					run()
					if elapsed := time.Since(started); elapsed > time.Second {
						b.Logf("SLOW (>1s): %s %s", q.Command, elapsed)
					}
					if !warm {
						gets += counted.gets
						bytes += counted.bytes
					}
				}
				b.StopTimer()
				if warm {
					gets, bytes = counted.gets, counted.bytes
				}
				b.ReportMetric(float64(gets)/float64(b.N), "GETs/op")
				b.ReportMetric(float64(bytes)/float64(b.N), "fetched-B/op")
			})
		}
	}
}
