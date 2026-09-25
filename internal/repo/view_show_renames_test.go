package repo

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gat/internal/store"
)

func showRenameFixture(t *testing.T, edited bool) (*Snapshot, *Snapshot, string, []showChange) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	command(t, dir, "init", "-q", "-b", "main")
	text := "alpha one\nbeta two\ngamma three\ndelta four\nepsilon five\nzeta six\n"
	write(t, dir, "before file", []byte(text))
	write(t, dir, "old empty", nil)
	write(t, dir, "old mode", []byte("mode text\n"))
	write(t, dir, "old binary", []byte{0, 1, 2, 3})
	if err := os.Symlink("destination", filepath.Join(dir, "old-link")); err != nil {
		t.Fatal(err)
	}
	a := commit(t, dir)
	for _, names := range [][2]string{{"before file", "after file"}, {"old empty", "new empty"}, {"old mode", "new mode"}, {"old binary", "new binary"}, {"old-link", "new-link"}} {
		command(t, dir, "mv", names[0], names[1])
	}
	if edited {
		write(t, dir, "after file", []byte(strings.Replace(text, "epsilon five", "edited content", 1)))
	}
	if err := os.Chmod(filepath.Join(dir, "new mode"), 0755); err != nil {
		t.Fatal(err)
	}
	b := commit(t, dir)
	local, _ := store.NewLocal(t.TempDir())
	if _, err := Import(ctx, local, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	r, _ := New(local, 1<<20)
	old, err := r.Open(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	to, err := r.Open(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	var changes []showChange
	if err := old.walkChanges(ctx, old.Tree, to.Tree, nil, func(name string, a, b Entry) error { changes = append(changes, showChange{name, a, b}); return nil }); err != nil {
		t.Fatal(err)
	}
	sort.SliceStable(changes, func(i, j int) bool { return changes[i].name < changes[j].name })
	return old, to, dir, changes
}
func TestShowRenamePatchParity(t *testing.T) {
	for _, edited := range []bool{false, true} {
		name := "exact"
		if edited {
			name = "edited"
		}
		t.Run(name, func(t *testing.T) {
			old, to, dir, changes := showRenameFixture(t, edited)
			for _, noRenames := range []bool{false, true} {
				for _, kind := range []string{"patch", "name-only", "name-status"} {
					args := []string{"diff", "--no-ext-diff", "--no-textconv", "--no-color", "--no-indent-heuristic"}
					opt := DiffOptions{Context: 3}
					switch kind {
					case "name-only":
						opt.NameOnly = true
						args = append(args, "--name-only")
					case "name-status":
						opt.NameStatus = true
						args = append(args, "--name-status")
					}
					if noRenames {
						args = append(args, "--no-renames")
					} else {
						args = append(args, "--find-renames=50%")
					}
					args = append(args, old.SHA, to.SHA)
					want, status := graphGit(t, dir, args...)
					var got bytes.Buffer
					if err := showDiffChanges(context.Background(), old, to, changes, opt, noRenames, &got); err != nil {
						t.Fatal(err)
					}
					if status != 0 || !bytes.Equal(want, got.Bytes()) {
						t.Fatalf("%s no-renames=%v\nGot:\n%s\nWant:\n%s", kind, noRenames, got.String(), want)
					}
				}
			}
		})
	}
}
func TestShowRenameAmbiguityFailsExplicitly(t *testing.T) {
	old, to, _, changes := showRenameFixture(t, true)
	var editedSource, editedTarget showChange
	for _, c := range changes {
		if c.name == "before file" {
			editedSource = c
		}
		if c.name == "after file" {
			editedTarget = c
		}
	}
	second := editedSource
	second.name = "other-before"
	changes = []showChange{editedTarget, editedSource, second}
	err := showDiffChanges(context.Background(), old, to, changes, DiffOptions{NameStatus: true}, false, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "--no-renames") {
		t.Fatalf("ambiguous edited sources must be explicit: %v", err)
	}
	if err := showDiffChanges(context.Background(), old, to, changes, DiffOptions{NameStatus: true}, true, io.Discard); err != nil {
		t.Fatal(err)
	}
}
