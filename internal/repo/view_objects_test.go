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

func TestObjectViewsGitParity(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	command(t, dir, "init", "-q")
	for name, body := range map[string]string{"a.c": "hello world\nSecond HELLO\nend\n", "a/z.txt": "hello nested\n", "a/sub/deep": "deep\n", "b": "test 123\nhello\n", "empty": "", "space name": "hello\n", "tab\tname": "hello\n", "unicode-ü": "hello\n", "binary": "first\x00hello\n", "crlf": "hello\r\n"} {
		write(t, dir, name, []byte(body))
	}
	if err := os.Symlink("a.c", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	first := commit(t, dir)
	write(t, dir, "a.c", []byte("new hello\nSecond HELLO\nend\n"))
	commit(t, dir)
	command(t, dir, "tag", "-a", "release", "-m", "release")
	tag := command(t, dir, "rev-parse", "release")
	local, _ := store.NewLocal(t.TempDir())
	if _, err := Import(ctx, local, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	r, _ := New(local, 1<<20)
	s, err := r.OpenRevision(ctx, "HEAD", "")
	if err != nil {
		t.Fatal(err)
	}
	blob := command(t, dir, "rev-parse", "HEAD:a.c")
	tree := command(t, dir, "rev-parse", "HEAD^{tree}")
	cases := [][]string{
		{"ls-tree", "HEAD"}, {"ls-tree", "release"}, {"ls-tree", "-r", "HEAD"}, {"ls-tree", "-rt", "HEAD"}, {"ls-tree", "-rd", "HEAD"}, {"ls-tree", "--name-only", "-r", "HEAD"}, {"ls-tree", "-lz", "HEAD"}, {"ls-tree", "--abbrev=8", "HEAD"}, {"ls-tree", "HEAD", "a"}, {"ls-tree", "HEAD", "a/z.txt"}, {"ls-tree", "HEAD:a"}, {"ls-tree", tree},
		{"ls-files"}, {"ls-files", "-s"}, {"ls-files", "-z"}, {"ls-files", "--", ":(glob)**/*.txt"}, {"ls-files", "--", ":(exclude)a"},
		{"cat-file", "-t", "release"}, {"cat-file", "-t", tag}, {"cat-file", "-t", tag[:10]}, {"cat-file", "-e", "release"}, {"cat-file", "-p", "HEAD:a.c"}, {"cat-file", "blob", blob}, {"cat-file", "-p", tree}, {"cat-file", "tree", tree}, {"cat-file", "tree", "HEAD"}, {"cat-file", "tree", "release"}, {"cat-file", "-t", "HEAD"}, {"cat-file", "-t", tree}, {"cat-file", "-s", blob}, {"cat-file", "-s", "HEAD"}, {"cat-file", "-e", blob}, {"cat-file", "-p", blob[:9]},
		{"grep", "hello"}, {"grep", "-n", "hello"}, {"grep", "-nil", "hello"}, {"grep", "-F", "hello"}, {"grep", "-E", "hello|deep"}, {"grep", "-e", "hello", "-e", "deep"}, {"grep", "-v", "hello"}, {"grep", "-c", "hello"}, {"grep", "-l", "-z", "hello"}, {"grep", "-n", "-z", "hello"}, {"grep", "-h", "hello"}, {"grep", "-I", "hello"}, {"grep", "-a", "hello"}, {"grep", "hello", "--", ":(glob)a/**/*.txt"}, {"grep", "-n", "hello", first}, {"grep", "hello", first, "--", "a.c"}, {"grep", "xyz-no-match"}, {"grep", "-q", "hello"}, {"grep", "h.*o", "--", "a.c"}, {"grep", `hello\|deep`},
	}
	for _, prefix := range []string{"", "a", "a/sub"} {
		for _, args := range [][]string{
			{"grep", "-n", "-F", "hello", "HEAD:a.c"},
			{"grep", "-n", "-F", "hello", "HEAD:a"},
			{"grep", "-n", "-F", "hello", "HEAD~1:a"},
			{"grep", "-n", "hello", "HEAD", "HEAD~1", "--", ":(top)a.c"},
			{"grep", "-n", "hello", "HEAD:a.c", "HEAD:b"},
			{"grep", "-n", "hello", "HEAD:a", "HEAD~1:a"},
			{"grep", "-n", "hello", "HEAD:a", "--", ":(top)z.txt"},
			{"grep", "-n", "hello", "HEAD:a.c", "--", "no-such-filter"},
			{"grep", "-n", "--full-name", "hello", "HEAD:a.c"},
			{"grep", "-n", "hello", "HEAD:tab\tname"},
			{"grep", "-nz", "hello", "HEAD:tab\tname"},
			{"grep", "-n", "hello", blob, tree},
		} {
			t.Run("selectors "+prefix+" "+strings.Join(args, " "), func(t *testing.T) { checkObjectViewParity(t, ctx, r, s, dir, prefix, args) })
		}
	}
	for _, args := range [][]string{
		{"grep", "hello", "HEAD:a.c", "HEAD:missing"},
		{"grep", "hello", "HEAD", "HEAD:no-such-dir"},
		{"grep", "hello", "HEAD", "no-such-revision", "--", "a.c"},
		{"grep", "hello", "HEAD", "no-such-revision"},
	} {
		t.Run("invalid selectors "+strings.Join(args, " "), func(t *testing.T) {
			native := exec.Command("git", append([]string{"-C", dir}, args...)...)
			if err := native.Run(); err == nil {
				t.Fatal("expected invalid native selector")
			}
			var out bytes.Buffer
			err := r.ViewObjects(ctx, s, ViewOptions{Command: args[0], Args: args[1:]}, &out)
			if err == nil || errors.Is(err, ErrViewNoMatch) {
				t.Fatalf("invalid selector must error, got %v", err)
			}
			if out.Len() != 0 {
				t.Fatal("invalid selector emitted partial matches")
			}
		})
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) { checkObjectViewParity(t, ctx, r, s, dir, "", args) })
	}
	for _, args := range [][]string{{"ls-tree", "HEAD"}, {"ls-tree", "-r", "HEAD"}, {"ls-tree", "--full-tree", "HEAD"}, {"ls-tree", "--full-name", "HEAD"}, {"ls-tree", "HEAD", "z.txt"}, {"ls-files"}, {"ls-files", "--full-name"}, {"ls-files", "../a.c"}, {"grep", "-n", "hello"}, {"grep", "--full-name", "hello"}, {"cat-file", "-p", "HEAD:./z.txt"}} {
		t.Run("subdir "+strings.Join(args, " "), func(t *testing.T) { checkObjectViewParity(t, ctx, r, s, dir, "a", args) })
	}
}
func checkObjectViewParity(t *testing.T, ctx context.Context, r *Repository, s *Snapshot, dir, prefix string, args []string) {
	t.Helper()
	c := exec.Command("git", append([]string{"-C", filepath.Join(dir, prefix), "-c", "color.ui=false"}, args...)...)
	c.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "LC_ALL=C")
	want, gitErr := c.Output()
	var got bytes.Buffer
	err := r.ViewObjects(ctx, s, ViewOptions{Command: args[0], Args: args[1:], Prefix: prefix}, &got)
	if gitErr != nil {
		var ex *exec.ExitError
		if !errors.As(gitErr, &ex) || ex.ExitCode() != 1 {
			t.Fatalf("git failed: %v %s", gitErr, ex.Stderr)
		}
		if !errors.Is(err, ErrViewNoMatch) {
			t.Fatalf("expected exit 1, got %v", err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("output mismatch\ngot  %q\nwant %q", got.Bytes(), want)
	}
}

func BenchmarkObjectViewsMedium(b *testing.B) {
	dir := os.Getenv("GYIT_BENCH_STORE")
	if dir == "" {
		dir = "../../.testdata/lima-store"
	}
	if _, err := os.Stat(filepath.Join(dir, "HEAD")); err != nil {
		b.Skip("import medium fixture or set GYIT_BENCH_STORE")
	}
	for _, args := range [][]string{{"ls-tree", "HEAD"}, {"ls-tree", "-r", "HEAD"}, {"ls-files"}, {"cat-file", "-p", "HEAD:README.md"}, {"grep", "-n", "the", "--", "README.md"}, {"grep", "-n", "the", "HEAD:README.md"}, {"grep", "-n", "the", "HEAD:docs"}, {"grep", "-n", "the", "HEAD~1:docs"}, {"grep", "-l", "the"}} {
		b.Run(strings.Join(args, "_"), func(b *testing.B) {
			for _, warm := range []bool{false, true} {
				label := "fresh"
				if warm {
					label = "reused"
				}
				b.Run(label, func(b *testing.B) {
					local, _ := store.NewLocal(dir)
					measured := &countedStore{Store: local}
					var r *Repository
					var s *Snapshot
					open := func() {
						r, _ = New(measured, 32<<20)
						var err error
						s, err = r.OpenRevision(context.Background(), "HEAD", "")
						if err != nil {
							b.Fatal(err)
						}
					}
					open()
					run := func() {
						if err := r.ViewObjects(context.Background(), s, ViewOptions{Command: args[0], Args: args[1:]}, io.Discard); err != nil {
							b.Fatal(err)
						}
					}
					if warm {
						run()
					}
					measured.reset()
					gets, fetched := 0, 0
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if !warm {
							b.StopTimer()
							open()
							measured.reset()
							b.StartTimer()
						}
						start := time.Now()
						run()
						if time.Since(start) > time.Second {
							b.Logf("SLOW (>1s): %s: %s", strings.Join(args, " "), time.Since(start))
						}
						if !warm {
							gets += measured.gets
							fetched += measured.bytes
						}
					}
					b.StopTimer()
					if warm {
						gets, fetched = measured.gets, measured.bytes
					}
					b.ReportMetric(float64(gets)/float64(b.N), "GETs/op")
					b.ReportMetric(float64(fetched)/float64(b.N), "fetched-B/op")
				})
			}
		})
	}
}

func TestObjectViewsRejectUnsupportedSemantics(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	command(t, dir, "init", "-q")
	write(t, dir, "file", []byte("text\n"))
	commit(t, dir)
	local, _ := store.NewLocal(t.TempDir())
	if _, err := Import(ctx, local, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	r, _ := New(local, 1<<20)
	s, err := r.OpenRevision(ctx, "HEAD", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"cat-file", "-p", "HEAD"}, {"cat-file", "commit", "HEAD"}, {"ls-files", "--deleted"}, {"grep", "-P", "text"}, {"grep", `(text)\1`}, {"grep", "-G", "-E", "text"}} {
		var out bytes.Buffer
		if err := r.ViewObjects(ctx, s, ViewOptions{Command: args[0], Args: args[1:]}, &out); err == nil {
			t.Fatalf("unsupported %v accepted", args)
		}
		if out.Len() != 0 {
			t.Fatalf("unsupported %v emitted output", args)
		}
	}
}
