package controlcli

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"gat/internal/control"
	"gat/internal/repo"
	"gat/internal/store"
)

func TestBlameTypeChangesAgainstGit(t *testing.T) {
	for _, parentSymlink := range []bool{false, true} {
		name := "regular-to-symlink"
		if parentSymlink {
			name = "symlink-to-regular"
		}
		t.Run(name, func(t *testing.T) {
			source := t.TempDir()
			git := func(args ...string) []byte {
				t.Helper()
				cmd := exec.CommandContext(t.Context(), "git", append([]string{"-C", source}, args...)...)
				cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Author", "GIT_AUTHOR_EMAIL=a@example.test", "GIT_COMMITTER_NAME=Committer", "GIT_COMMITTER_EMAIL=c@example.test", "GIT_AUTHOR_DATE=1000000000 +0530", "GIT_COMMITTER_DATE=1000000010 -0730", "LC_ALL=C")
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("Git %v: %v: %s", args, err, out)
				}
				return out
			}
			path := filepath.Join(source, "file")
			write := func(symlink bool) {
				t.Helper()
				var err error
				if symlink {
					err = os.Symlink("same target", path)
				} else {
					err = os.WriteFile(path, []byte("same target"), 0600)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			git("init", "-q", "-b", "main")
			write(parentSymlink)
			git("add", ".")
			git("commit", "-qm", "Original type")
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			write(!parentSymlink)
			git("add", ".")
			git("commit", "-qm", "Changed type")
			backend, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = repo.Import(t.Context(), backend, repo.ImportOptions{Repo: source}); err != nil {
				t.Fatal(err)
			}
			r, err := repo.New(backend, repo.DefaultCacheBytes)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := r.OpenRevision(t.Context(), "main", "")
			if err != nil {
				t.Fatal(err)
			}
			dir, err := os.MkdirTemp("", "gat-blame-type-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			socket := filepath.Join(dir, "s")
			server, err := control.ListenStream(t.Context(), socket, control.New(r, snapshot).Serve)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			for _, revision := range []string{"HEAD", "HEAD^"} {
				args := []string{"blame", "--line-porcelain", revision, "--", "file"}
				want := git(args...)
				var out, stderr bytes.Buffer
				if err = Run(t.Context(), append([]string{"blame", "--socket", socket}, args[1:]...), &out, &stderr); err != nil {
					t.Fatal(err, stderr.String())
				}
				if !bytes.Equal(out.Bytes(), want) {
					t.Fatalf("type-change attribution and previous differ:\n got %s\nwant %s", out.Bytes(), want)
				}
			}
		})
	}
}
