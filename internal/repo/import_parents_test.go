package repo

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"gyit/internal/store"
)

// A shallow boundary keeps the raw parent header in the commit object, while
// Git's traversable graph hides that parent. Import must preserve that graph,
// including after another commit is appended to the same shallow source.
func TestImportShallowParentsAndObjectNameHints(t *testing.T) {
	for _, preload := range []bool{false, true} {
		name := "normal"
		if preload {
			name = "preloaded"
		}
		t.Run(name, func(t *testing.T) {
			importer := Import
			if preload {
				importer = func(ctx context.Context, backend store.Store, opt ImportOptions) (Stats, error) {
					return importWithMetadataThreshold(ctx, backend, opt, 0)
				}
			}

			for _, format := range []string{"sha1", "sha256"} {
				for _, noDeltas := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/no-deltas=%t", format, noDeltas), func(t *testing.T) {
						source := t.TempDir()
						command(t, source, "init", "-q", "--object-format="+format)
						write(t, source, "file", []byte("root\n"))
						root := commit(t, source)
						write(t, source, "file", []byte("boundary\n"))
						boundary := commit(t, source)
						// This path hint looks like two parent object IDs. Only the
						// object type can distinguish it from enumerated parent records.
						write(t, source, root+" "+boundary, []byte("object-shaped name\n"))
						commit(t, source)
						if err := os.WriteFile(filepath.Join(source, ".git", "shallow"), []byte(boundary+"\n"), 0600); err != nil {
							t.Fatal(err)
						}
						local, err := store.NewLocal(t.TempDir())
						if err != nil {
							t.Fatal(err)
						}
						for iteration := 0; iteration < 2; iteration++ {
							if iteration > 0 {
								write(t, source, "file", []byte("incremental\n"))
								commit(t, source)
							}
							if _, err := importer(t.Context(), local, ImportOptions{Repo: source, DisableDeltas: noDeltas}); err != nil {
								t.Fatal(err)
							}
							r, _ := New(local, 64<<10)
							snap, err := r.OpenRevision(t.Context(), "HEAD", "")
							if err != nil {
								t.Fatal(err)
							}
							for _, args := range [][]string{{"--parents", "HEAD"}, {"--count", "HEAD"}, {"--reverse", "HEAD"}, {"--parents", boundary}} {
								graphParity(t, r, snap, source, "rev-list", args...)
							}
							checkObjectViewParity(t, t.Context(), r, snap, source, "", []string{"ls-tree", "-r", "HEAD"})
						}
					})
				}
			}
		})
	}
}
