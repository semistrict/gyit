package repo

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"gyit/internal/store"
)

type modelFile struct {
	entry Entry
	body  []byte
}

type modelRevision struct {
	oid   string
	files map[string]modelFile
}

// Generate actual Git objects from a separate byte-map model. Keep every
// generated tip reachable, including siblings and merges. A seed determines
// the graph and content; Git assigns identities and writes the object format.
func modelHistory(t *testing.T, source string, seed int64) []modelRevision {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	names := []string{"a", "a.b", "-option", "space name", "tab\tname", "line\nname", "unicode-ü"}
	var revisions []modelRevision
	for step := range 10 {
		files := make(map[string]modelFile)
		var parents []string
		if step > 0 {
			parent := rng.Intn(step)
			for name, file := range revisions[parent].files {
				files[name] = file
			}
			parents = append(parents, "-p", revisions[parent].oid)
			if step > 1 && step%3 == 0 {
				other := (parent + 1) % step
				parents = append(parents, "-p", revisions[other].oid)
			}
		}
		// Cycle through modes and names to guarantee unusual names, empty
		// files and symlinks even in the ordinary small seed set.
		name := names[step%len(names)]
		body := make([]byte, rng.Intn(512)+1)
		if _, err := rng.Read(body); err != nil {
			t.Fatal(err)
		}
		mode := uint32(0100644)
		switch step % 4 {
		case 0:
			body = nil
		case 1:
			mode = 0100755
		case 2:
			mode, body = 0120000, []byte("../target\n")
		}
		oid := archiveImportInput(t, source, string(body), "hash-object", "-w", "--stdin")
		files[name] = modelFile{Entry{Name: name, OID: oid, Mode: mode, Size: int64(len(body))}, body}
		if step%3 == 2 {
			delete(files, names[(step+len(names)-1)%len(names)])
		}
		ordered := make([]string, 0, len(files))
		for name := range files {
			ordered = append(ordered, name)
		}
		sort.Strings(ordered)
		var input bytes.Buffer
		for _, name := range ordered {
			file := files[name]
			fmt.Fprintf(&input, "%o blob %s\t%s%c", file.entry.Mode, file.entry.OID, name, 0)
		}
		tree := archiveImportInput(t, source, input.String(), "mktree", "-z")
		oid = archiveImportInput(t, source, fmt.Sprintf("seed=%d step=%d\n", seed, step), append([]string{"commit-tree", tree}, parents...)...)
		archiveImportInput(t, source, "", "update-ref", fmt.Sprintf("refs/heads/step-%02d", step), oid)
		revisions = append(revisions, modelRevision{oid, files})
	}
	archiveImportInput(t, source, "", "symbolic-ref", "HEAD", "refs/heads/step-09")
	return revisions
}

func checkModelRevision(t *testing.T, reader *Repository, revision modelRevision) {
	t.Helper()
	snapshot, err := reader.Open(t.Context(), revision.oid)
	if err != nil {
		t.Fatal(err)
	}
	var want []Entry
	for _, file := range revision.files {
		want = append(want, file.entry)
	}
	sort.Slice(want, func(i, j int) bool { return want[i].Name < want[j].Name })
	// One-entry pages force exclusive cursors through prefix names, tabs,
	// newlines and non-ASCII. Bound the loop so a cursor regression fails.
	var got []Entry
	after := ""
	for page := 0; page <= len(want); page++ {
		entries, err := snapshot.ReadDir(t.Context(), snapshot.Tree, after, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			break
		}
		if len(entries) != 1 || entries[0].Name <= after {
			t.Fatalf("non-advancing page after %q: %+v", after, entries)
		}
		got = append(got, entries...)
		after = entries[0].Name
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tree for %s: got %+v want %+v", revision.oid, got, want)
	}
	for _, entry := range want {
		resolved, err := snapshot.Resolve(t.Context(), entry.Name)
		if err != nil || resolved != entry {
			t.Fatalf("resolve %q: %+v %v", entry.Name, resolved, err)
		}
		body := revision.files[entry.Name].body
		for _, offset := range []int{0, len(body) / 2, len(body), len(body) + 1} {
			buffer := bytes.Repeat([]byte{0xcc}, len(body)+3)
			n, err := snapshot.ReadAt(t.Context(), entry.OID, buffer, int64(offset))
			expected := body[min(offset, len(body)):]
			if n != len(expected) || err != io.EOF || !bytes.Equal(buffer[:n], expected) {
				t.Fatalf("read %q offset=%d: n=%d err=%v", entry.Name, offset, n, err)
			}
			if !bytes.Equal(buffer[n:], bytes.Repeat([]byte{0xcc}, len(buffer)-n)) {
				t.Fatalf("read %q overwrote bytes beyond n", entry.Name)
			}
		}
	}
}

func TestSeededGitHistoryAcrossStorageLayouts(t *testing.T) {
	// No user configuration, wall-clock commit timestamps or global hooks
	// may change the generated repository or its object identities.
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_AUTHOR_DATE", "2001-01-01T00:00:00Z")
	t.Setenv("GIT_COMMITTER_DATE", "2001-01-01T00:00:00Z")
	for _, format := range []string{"sha1", "sha256"} {
		for _, seed := range []int64{1, 7, 23} {
			t.Run(fmt.Sprintf("%s/seed=%d", format, seed), func(t *testing.T) {
				source := filepath.Join(t.TempDir(), "source")
				if err := os.Mkdir(source, 0700); err != nil {
					t.Fatal(err)
				}
				command(t, source, "init", "-q", "--object-format="+format)
				revisions := modelHistory(t, source, seed)
				var backends []store.Store
				for _, layout := range []string{"loose", "packed", "no-deltas"} {
					storeRoot := t.TempDir()
					t.Run(layout, func(t *testing.T) {
						if layout == "packed" {
							command(t, source, "-c", "pack.writeReverseIndex=true", "repack", "-adf")
							command(t, source, "prune-packed")
						}
						backend, err := store.NewLocal(storeRoot)
						if err != nil {
							t.Fatal(err)
						}
						scratch := t.TempDir()
						stats, err := Import(t.Context(), backend, ImportOptions{Repo: source, TempDir: scratch, CompressionWorkers: 2, DisableDeltas: layout == "no-deltas"})
						if err != nil {
							t.Fatal(err)
						}
						wantMode := "reachable"
						if format == "sha1" && layout == "packed" {
							wantMode = "archive"
						}
						if stats.ImportMode != wantMode {
							t.Fatalf("import mode=%s want=%s (%s)", stats.ImportMode, wantMode, stats.FallbackReason)
						}
						replacementScratchEmpty(t, scratch)
						reader, err := New(backend, DefaultCacheBytes)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { reader.Close() })
						for _, revision := range revisions {
							checkModelRevision(t, reader, revision)
						}
						backends = append(backends, backend)
					})
				}
				if err := os.RemoveAll(source); err != nil {
					t.Fatal(err)
				}
				for _, backend := range backends {
					reader, err := New(backend, DefaultCacheBytes)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { reader.Close() })
					for _, revision := range revisions {
						checkModelRevision(t, reader, revision)
					}
				}
			})
		}
	}
}
