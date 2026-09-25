package repo

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"gat/internal/store"
)

func TestNonCommitReferencesMatchGit(t *testing.T) {
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			source := t.TempDir()
			command(t, source, "init", "-q", "-b", "main", "--object-format="+format)
			write(t, source, "file", []byte("tracked contents\n"))
			commit(t, source)
			blob := command(t, source, "rev-parse", "HEAD:file")
			tree := command(t, source, "rev-parse", "HEAD^{tree}")
			for name, oid := range map[string]string{"blob-light": blob, "tree-light": tree} {
				command(t, source, "tag", name, oid)
				command(t, source, "tag", "-am", name, name+"-annotated", oid)
				command(t, source, "tag", "-am", name, name+"-nested", name+"-annotated")
			}
			// A ref's identity remains listable even when its object is outside
			// the importer's committed payload closure.
			write(t, source, "orphan", []byte("not in a commit\n"))
			orphan := command(t, source, "hash-object", "-w", "orphan")
			command(t, source, "tag", "orphan", orphan)
			command(t, source, "tag", "-am", "orphan tag", "orphan-annotated", orphan)
			command(t, source, "symbolic-ref", "refs/tags/tree-symbolic", "refs/tags/tree-light")
			local, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Import(t.Context(), local, ImportOptions{Repo: source}); err != nil {
				t.Fatal(err)
			}
			r, _ := New(local, 1<<20)
			current, err := r.OpenRevision(t.Context(), "main", "")
			if err != nil {
				t.Fatal(err)
			}
			cases := [][]string{
				{"show-ref"}, {"show-ref", "-d"}, {"show-ref", "--tags"}, {"show-ref", "--verify", "refs/tags/orphan"},
				{"tag", "--list"}, {"tag", "--format=%(refname:short) %(objectname)"},
				{"rev-parse", "--all"}, {"rev-parse", "--tags"},
				{"rev-parse", "orphan", "orphan-annotated", "tree-symbolic"},
				{"ls-tree", "-rl", "tree-light"}, {"ls-tree", "-rl", "tree-light-nested"},
				{"cat-file", "tree", "tree-light-annotated"}, {"cat-file", "blob", "blob-light-annotated"},
				{"cat-file", "-p", "tree-light:file"}, {"cat-file", "-p", "tree-light-nested:file"},
			}
			for _, name := range []string{"blob-light", "blob-light-annotated", "blob-light-nested", "tree-light", "tree-light-annotated", "tree-light-nested"} {
				cases = append(cases, []string{"cat-file", "-t", name}, []string{"cat-file", "-e", name}, []string{"cat-file", "-p", name + "^{}"}, []string{"rev-parse", name, name + "^{}"})
				kind := "blob"
				if strings.HasPrefix(name, "tree-") {
					kind = "tree"
				}
				cases = append(cases, []string{"rev-parse", name + "^{" + kind + "}"})
				for _, revision := range []string{name, name + "^{}", name + "^{commit}", name + "~1"} {
					if _, err := r.OpenRevision(t.Context(), revision, ""); !errors.Is(err, ErrInvalidRevision) {
						t.Errorf("noncommit mount %q: want invalid revision, got %v", revision, err)
					}
				}
			}
			for _, args := range cases {
				t.Run(strings.Join(args, " "), func(t *testing.T) {
					var out bytes.Buffer
					if err := r.View(t.Context(), current, ViewOptions{Command: args[0], Args: args[1:]}, &out); err != nil {
						t.Fatal(err)
					}
					want := refViewGit(t, source, args...)
					if !bytes.Equal(out.Bytes(), want) {
						t.Fatalf("got %q\nwant %q", out.Bytes(), want)
					}
				})
			}
			for _, selector := range []string{"tree-light^{blob}", "blob-light^{tree}", "blob-light-annotated^{tree}"} {
				if err := r.View(t.Context(), current, ViewOptions{Command: "cat-file", Args: []string{"-p", selector}}, io.Discard); err == nil {
					t.Errorf("accepted wrong object type %q", selector)
				}
			}
			// Refreshing only refs must neither reread payloads nor keep deleted
			// names. The old snapshot can still inspect the latest ref index.
			command(t, source, "tag", "-d", "blob-light-nested")
			command(t, source, "tag", "new-tree", tree)
			stats, err := Import(t.Context(), local, ImportOptions{Repo: source})
			if err != nil || stats.Objects != 0 {
				t.Fatalf("ref-only refresh: %+v %v", stats, err)
			}
			var out bytes.Buffer
			if err := r.View(t.Context(), current, ViewOptions{Command: "show-ref", Args: []string{"-d"}}, &out); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(out.Bytes(), refViewGit(t, source, "show-ref", "-d")) {
				t.Fatal("updated noncommit references differ from Git")
			}
		})
	}
}
