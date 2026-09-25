package repo

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"gat/internal/store"
)

// Older Git histories contain noncanonical permission bits. Filesystem views
// must use Git's normalized permissions while raw tree reads retain the object.
func TestImportHistoricalTreeModes(t *testing.T) {
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			dir := t.TempDir()
			command(t, dir, "init", "-q", "--object-format="+format)
			write(t, dir, "file", []byte("historical contents\n"))
			commit(t, dir)
			blob := command(t, dir, "rev-parse", "HEAD:file")
			nested := command(t, dir, "rev-parse", "HEAD^{tree}")
			var raw bytes.Buffer
			for _, entry := range []struct {
				name, oid string
				mode      uint32
			}{
				{"a-group-write", blob, 0100664},
				{"dir", nested, 0040775},
				{"exec", blob, 0100744},
				{"group-exec", blob, 0100655},
				{"link", blob, 0120777},
				{"private", blob, 0100600},
				{"raw-\xff", blob, 0100644},
			} {
				fmt.Fprintf(&raw, "%o %s\x00", entry.mode, entry.name)
				oid, err := hex.DecodeString(entry.oid)
				if err != nil {
					t.Fatal(err)
				}
				raw.Write(oid)
			}
			c := exec.Command("git", "-C", dir, "hash-object", "--literally", "-w", "-t", "tree", "--stdin")
			c.Stdin = bytes.NewReader(raw.Bytes())
			out, err := c.Output()
			if err != nil {
				t.Fatal(err)
			}
			tree := strings.TrimSpace(string(out))
			sha := command(t, dir, "commit-tree", tree, "-m", "historical modes")
			command(t, dir, "update-ref", "HEAD", sha)
			local, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Import(t.Context(), local, ImportOptions{Repo: dir}); err != nil {
				t.Fatal(err)
			}
			r, _ := New(local, 1<<20)
			s, err := r.Open(t.Context(), sha)
			if err != nil {
				t.Fatal(err)
			}
			for _, args := range [][]string{
				{"ls-tree", "HEAD"}, {"ls-tree", "-r", "HEAD"},
				{"cat-file", "-p", tree}, {"cat-file", "tree", tree},
			} {
				checkObjectViewParity(t, t.Context(), r, s, dir, "", args)
			}
			for name, mode := range map[string]uint32{
				"dir": 0040000, "dir/file": 0100644, "exec": 0100755,
				"group-exec": 0100644, "a-group-write": 0100644,
				"link": 0120000, "private": 0100644,
				"raw-\xff": 0100644,
			} {
				e, err := s.Resolve(t.Context(), name)
				if err != nil || e.Mode != mode {
					t.Fatalf("%s: mode=%o want=%o error=%v", name, e.Mode, mode, err)
				}
			}
		})
	}
}

func TestImportRejectsOverflowTreeMode(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q")
	write(t, source, "file", []byte("contents\n"))
	commit(t, source)
	blob, err := hex.DecodeString(command(t, source, "rev-parse", "HEAD:file"))
	if err != nil {
		t.Fatal(err)
	}
	// 2^32 + 0100644. Truncating the mode to uint32 would silently accept it
	// as an ordinary file and publish an unreconstructable tree object.
	raw := append([]byte("40000100644 file\x00"), blob...)
	c := exec.Command("git", "-C", source, "hash-object", "--literally", "-w", "-t", "tree", "--stdin")
	c.Stdin = bytes.NewReader(raw)
	out, err := c.Output()
	if err != nil {
		t.Fatal(err)
	}
	sha := command(t, source, "commit-tree", strings.TrimSpace(string(out)), "-m", "overflow")
	command(t, source, "update-ref", "HEAD", sha)
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Import(t.Context(), local, ImportOptions{Repo: source}); err == nil {
		t.Fatal("accepted overflowing tree mode")
	}
	if _, _, err := local.Get(t.Context(), "HEAD", 0, -1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("invalid source published HEAD: %v", err)
	}
}
