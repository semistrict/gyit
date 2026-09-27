package githubfs

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

	"gyit/internal/repo"
	"gyit/internal/store"
)

func nativeGit(t *testing.T, dir string, args ...string) []byte {
	t.Helper()
	c := exec.CommandContext(t.Context(), "git", append([]string{"-C", dir}, args...)...)
	c.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_PAGER=cat", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test", "LC_ALL=C")
	out, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return out
}

// Materialize the virtual repository, using paginated directory reads
// and deliberately short file reads. Native Git itself is the output oracle.
func exportGitDirectory(t *testing.T, f *FS, root, dest string) {
	t.Helper()
	var walk func(string, string)
	walk = func(source, target string) {
		if err := os.MkdirAll(target, 0700); err != nil {
			t.Fatal(err)
		}
		after := ""
		for {
			entries, err := f.ReadDir(t.Context(), source, after, 1)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) == 0 {
				return
			}
			for _, e := range entries {
				name := source + "/" + e.Name
				local := filepath.Join(target, e.Name)
				if e.Mode == 0040000 {
					walk(name, local)
				} else {
					info, err := f.Lookup(t.Context(), name)
					if err != nil || info.Size != e.Size {
						t.Fatalf("stat %s: %v", name, err)
					}
					var out bytes.Buffer
					for off := int64(0); ; {
						b := make([]byte, 137)
						n, err := f.Read(t.Context(), name, b, off)
						if err != nil {
							t.Fatal(err)
						}
						if n == 0 {
							break
						}
						out.Write(b[:n])
						off += int64(n)
					}
					if int64(out.Len()) != e.Size {
						t.Fatalf("size %s: %d != %d", name, out.Len(), e.Size)
					}
					if err := os.WriteFile(local, out.Bytes(), 0400); err != nil {
						t.Fatal(err)
					}
				}
				after = e.Name
			}
		}
	}
	walk(root, dest)
}

func TestNativeGitLogFromVirtualDirectory(t *testing.T) {
	f, opts, first, second := openFixture(t)
	source := filepath.Join(strings.TrimPrefix(opts.RemoteBase, "file://"), "acme", "project.git")
	nativeGit(t, source, "update-ref", "--no-deref", "HEAD", first)
	nativeGit(t, source, "tag", "-a", "release", "-m", "annotated release", second)
	paths := []string{"acme/project", "acme/project@" + second, "acme/project@feature%2Flogin"}
	ids := []string{first, second, second}
	for i, p := range paths {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			waitReady(t, f, p)
			dest := t.TempDir()
			exportGitDirectory(t, f, p, dest)
			for _, entry := range bytes.Split(nativeGit(t, dest, "ls-files", "-v", "-z"), []byte{0}) {
				if len(entry) > 0 && entry[0] != 'H' {
					t.Fatalf("virtual index bypasses normal tracked-file scanning: %q", entry)
				}
			}
			// Nested-directory discovery must work without Git environment overrides.
			nested := filepath.Join(dest, "nested", "directory")
			if err := os.MkdirAll(nested, 0700); err != nil {
				t.Fatal(err)
			}
			for _, dir := range []string{dest, nested} {
				start := time.Now()
				got := nativeGit(t, dir, "log")
				elapsed := time.Since(start)
				want := nativeGit(t, source, "log", ids[i])
				if !bytes.Equal(got, want) {
					t.Fatalf("native log differs:\n%s\nwant:\n%s", got, want)
				}
				if elapsed > time.Second {
					t.Logf("OVER 1s: native git log: %s", elapsed)
				}
			}
			// This also verifies annotated tag objects and byte-exact commit identity.
			nativeGit(t, dest, "fsck", "--no-reflogs")
			oracle := t.TempDir()
			nativeGit(t, source, "worktree", "add", "--detach", oracle, ids[i])
			for _, args := range [][]string{
				{"status"}, {"status", "--porcelain"}, {"show", "HEAD"},
				{"diff", first, second}, {"diff"}, {"diff", "--cached"},
				{"blame", "--", "dir/hello"}, {"ls-files"}, {"ls-tree", "HEAD"},
				{"rev-parse", "HEAD"}, {"rev-list", "HEAD"}, {"branch", "--list"}, {"tag", "--list"},
			} {
				started := time.Now()
				got := nativeGit(t, dest, args...)
				elapsed := time.Since(started)
				want := nativeGit(t, oracle, args...)
				if !bytes.Equal(got, want) {
					t.Fatalf("git %v differs:\n%s\nwant:\n%s", args, got, want)
				}
				if elapsed > time.Second {
					t.Logf("OVER 1s: git %v: %s", args, elapsed)
				}
			}
			changed := filepath.Join(dest, "dir", "hello")
			if err := os.Chmod(changed, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(changed, []byte("changed tracked content\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if out := nativeGit(t, dest, "status", "--porcelain"); !bytes.Contains(out, []byte(" M dir/hello")) {
				t.Fatalf("native status missed changed tracked content: %q", out)
			}

		})
	}
	// The transport repository is disposable; reads require only durable data.
	if err := os.RemoveAll(strings.TrimPrefix(opts.RemoteBase, "file://")); err != nil {
		t.Fatal(err)
	}
	exportGitDirectory(t, f, paths[1], t.TempDir())
}

// Archive imports must not upload another copy of their original Git pack.
func TestNativeGitSharesImportedArchive(t *testing.T) {
	opts, _, _ := fixture(t)
	source := filepath.Join(strings.TrimPrefix(opts.RemoteBase, "file://"), "acme", "project.git")
	nativeGit(t, source, "repack", "-ad")
	revs, _ := filepath.Glob(filepath.Join(source, "objects", "pack", "*.rev"))
	for _, r := range revs {
		if err := os.Remove(r); err != nil {
			t.Fatal(err)
		}
	}
	backend, err := store.NewLocal(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	stats, err := repo.Import(t.Context(), backend, repo.ImportOptions{Repo: source, CompressionWorkers: 2})
	if err != nil {
		t.Fatal(err)
	}
	retained, prefix, size := stats.RetainedGitPack()
	if retained == "" || size == 0 {
		t.Fatal("source pack was not retained")
	}
	f, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err = f.prepareGitDirectory(t.Context(), source, backend, stats); err != nil {
		t.Fatal(err)
	}
	view, err := openGitDirectory(t.Context(), backend)
	if err != nil {
		t.Fatal(err)
	}
	file := view.files["objects/pack/"+filepath.Base(retained)]
	if file == nil || file.SegmentPrefix != prefix || file.Size != size {
		t.Fatal("native Git duplicated its source archive")
	}
	// A cold pack read issues just the requested byte range.
	meter := &gitRangeStore{Store: backend}
	view.backend = meter
	buf := make([]byte, 17)
	n, err := view.read(t.Context(), file.Path, buf, 11)
	if err != nil || n != 17 || meter.bytes != 17 {
		t.Fatalf("range read n=%d bytes=%d err=%v", n, meter.bytes, err)
	}
}

type gitRangeStore struct {
	store.Store
	bytes int64
}

func (s *gitRangeStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	s.bytes += n
	return s.Store.Get(ctx, key, off, n)
}
