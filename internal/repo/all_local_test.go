package repo

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"gat/internal/store"
)

func allLocalTestEnv(t *testing.T, pack string) {
	t.Helper()
	archiveImportEnv(t, pack)
	packMetadataTestEnv(t)
	for _, key := range []string{"GIT_GRAFT_FILE", "GIT_REPLACE_REF_BASE", "GIT_NAMESPACE", "GIT_DIR", "GIT_COMMON_DIR", "GIT_WORK_TREE", "GIT_CONFIG", "GIT_CONFIG_PARAMETERS", "GIT_CONFIG_COUNT", "GAT_DIAGNOSTIC_OBJECTS"} {
		value, present := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			var err error
			if present {
				err = os.Setenv(key, value)
			} else {
				err = os.Unsetenv(key)
			}
			if err != nil {
				t.Error(err)
			}
		})
	}
}

// Both orphan commits remain unreferenced. The merge's first parent is itself
// an orphan while its second is reachable, exercising ordered raw parent data
// and readers that must cross from catalog-only ancestry into the history graph.
func makeAllLocalCommitFixture(t *testing.T) (packMetadataFixture, string) {
	t.Helper()
	f := makePackMetadataFixture(t)
	merge := archiveImportInput(t, f.source, "orphan merge\n", "commit-tree", f.orphanTree, "-p", f.orphanCommit, "-p", f.tip)
	var ids strings.Builder
	f.local = nil
	for _, row := range strings.Split(archiveImportInput(t, f.source, "", "cat-file", "--batch-all-objects", "--batch-check=%(objectname) %(objecttype) %(objectsize)"), "\n") {
		fields := strings.Fields(row)
		if len(fields) != 3 {
			t.Fatal("fixture inventory", row)
		}
		size, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		f.local = append(f.local, packMetadataObject{fields[0], fields[1], size})
		fmt.Fprintln(&ids, fields[0])
	}
	base := filepath.Join(f.source, ".git", "objects", "pack", "pack")
	hash := archiveImportInput(t, f.source, ids.String(), "-c", "pack.writeReverseIndex=true", "pack-objects", "--window=50", "--depth=20", base)
	newPack := base + "-" + hash
	if len(hash) != 40 || newPack == f.pack {
		t.Fatal("new complete fixture pack required", hash)
	}
	oldFiles, err := filepath.Glob(f.pack + ".*")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range oldFiles {
		if err = os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	objects := filepath.Join(f.source, ".git", "objects")
	entries, err := os.ReadDir(objects)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || len(entry.Name()) != 2 {
			continue
		}
		if _, err = strconv.ParseUint(entry.Name(), 16, 8); err != nil {
			continue
		}
		if err = os.RemoveAll(filepath.Join(objects, entry.Name())); err != nil {
			t.Fatal(err)
		}
	}
	f.pack = newPack
	if _, err = os.Stat(f.pack + ".rev"); err != nil {
		t.Fatal(err)
	}
	return f, merge
}

func TestAllLocalCommitImportOrderedParentsAndReachableHistory(t *testing.T) {
	f, merge := makeAllLocalCommitFixture(t)
	allLocalTestEnv(t, f.pack)
	// Fail any separate commit/object selection walk. The unchanged history
	// producer's reverse topological rev-list has no --parents and still runs.
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	script := "#!/bin/sh\nrev=0\nparents=0\nfor arg do\n [ \"$arg\" = rev-list ] && rev=1\n [ \"$arg\" = --parents ] && parents=1\ndone\nif [ $rev = 1 ] && [ $parents = 1 ]; then echo unexpected-parent-walk >&2; exit 77; fi\nexec '" + strings.ReplaceAll(gitPath, "'", "'\\''") + "' \"$@\"\n"
	if err = os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	capture := deferredCapture(t)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	stats, err := importWithMetadataThreshold(ctx, backend, ImportOptions{Repo: f.source, TempDir: scratch, CompressionWorkers: 3}, 0)
	if err != nil {
		t.Fatal(err)
	}
	trace := capture()
	if stats.Objects != int64(len(f.local)) || stats.Phase != "done" {
		t.Fatalf("all local objects missing: %+v want%d", stats, len(f.local))
	}
	commits := int64(len(f.parents) + 2)
	if trace["all_local_commit_rows"] != commits || trace["all_local_parent_records_staged"] != commits || trace["commit_source_verified"] != commits {
		t.Fatal("raw commit path population", trace)
	}
	if count, ok := trace["all_local_commit_walk_selections"]; !ok || count != 0 {
		t.Fatal("selection walk must be absent", trace)
	}
	if trace["archive_plan_reused"] != 1 {
		t.Fatal("native source path missing", trace)
	}
	m, _, err := readHead(ctx, backend)
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != formatVersion || m.HistoryCount != uint64(len(f.parents)) || len(m.Tips) != 1 || m.Tips[0] != f.tip {
		t.Fatalf("reachable manifest changed: %+v", m)
	}
	r, err := New(backend, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	s, err := r.Open(ctx, f.tip)
	if err != nil {
		t.Fatal(err)
	}
	for _, obj := range f.local {
		var o object
		if err = s.idx.get(ctx, "o/"+obj.oid, &o); err != nil || o.Kind != obj.kind || o.Size != obj.size {
			t.Fatalf("identity%s %+v: %v", obj.oid, o, err)
		}
	}
	wantParents := map[string][]string{merge: {f.orphanCommit, f.tip}, f.orphanCommit: nil}
	for oid, want := range f.parents {
		wantParents[oid] = want
	}
	for oid, want := range wantParents {
		var p parents
		if err = s.idx.get(ctx, "p/"+oid, &p); err != nil || strings.Join(p.Parents, " ") != strings.Join(want, " ") {
			t.Fatalf("ordered parents%s got%v want%v: %v", oid, p.Parents, want, err)
		}
		var info commitInfo
		if err = s.idx.get(ctx, "c/"+oid, &info); err != nil {
			t.Fatal(err)
		}
	}
	for _, oid := range []string{merge, f.orphanCommit} {
		var position historyPosition
		if err = s.history.get(ctx, "g/"+oid, &position); !errors.Is(err, store.ErrNotFound) {
			t.Fatal("orphan entered reachable history", oid, err)
		}
	}
	cursor := &historyCursor{idx: s.history}
	seen := map[string]bool{}
	for pos := uint64(1); pos <= m.HistoryCount; pos++ {
		n, err := cursor.get(ctx, pos)
		if err != nil {
			t.Fatal(err)
		}
		oid := hex.EncodeToString(n.Oid)
		want, ok := f.parents[oid]
		if !ok || seen[oid] {
			t.Fatal("unexpected history node", oid)
		}
		seen[oid] = true
		var got []string
		for _, p := range n.Parents {
			ancestor, err := cursor.get(ctx, p)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, hex.EncodeToString(ancestor.Oid))
		}
		if strings.Join(got, " ") != strings.Join(want, " ") {
			t.Fatal("history parent order", oid, got, want)
		}
	}
	deferredCheckReadback(t, backend, f.source, f.tip, []string{"fast.txt", "empty", "large", "dir/child"})
	deferredCheckReadback(t, backend, f.source, merge, []string{"orphan.txt"})
	if files, err := os.ReadDir(scratch); err != nil || len(files) != 0 {
		t.Fatal("scratch leaked", files, err)
	}
}

func TestAllLocalCommitParentParser(t *testing.T) {
	first, second := strings.Repeat("a1", 20), strings.Repeat("b2", 20)
	head := "tree " + strings.Repeat("c3", 20) + "\n"
	tail := "author Person <p@example.test> 1 +0000\ncommitter Person <p@example.test> 2 +0000\n\nbody\nparent ignored\n"
	t.Run("ordered-and-continuation", func(t *testing.T) {
		var got []string
		body := head + "parent " + strings.ToUpper(second) + "\ngpgsig fake\n parent " + first + "\nparent " + first + "\n" + tail
		_, info, err := parseBufferedCommitParents(bufio.NewReader(strings.NewReader(body)), &got, 20)
		if err != nil || !reflect.DeepEqual(got, []string{second, first}) || string(info.Message) != "body\nparent ignored\n" {
			t.Fatal(got, info, err)
		}
	})
	t.Run("maximum-ordered-parents", func(t *testing.T) {
		var got []string
		body := head + strings.Repeat("parent "+first+"\n", maxLogParents) + tail
		if _, _, err := parseBufferedCommitParents(bufio.NewReader(strings.NewReader(body)), &got, 20); err != nil || len(got) != maxLogParents {
			t.Fatal(len(got), err)
		}
	})
	for _, parentHeaders := range []string{"parent abc\n", "parent " + strings.Repeat("g", 40) + "\n", "parent " + strings.Repeat("f", 8192) + "\n", strings.Repeat("parent "+first+"\n", maxLogParents+1)} {
		var got []string
		if _, _, err := parseBufferedCommitParents(bufio.NewReader(strings.NewReader(head+parentHeaders+tail)), &got, 20); err == nil {
			t.Fatal("invalid or oversized parent list accepted")
		}
	}
	// Ordinary parsing remains unchanged when no raw-parent sink is requested.
	if _, _, err := parseBufferedCommit(bufio.NewReader(strings.NewReader(head + "parent invalid-ignored\n" + tail))); err != nil {
		t.Fatal("ordinary parser changed", err)
	}
}

func TestAllLocalCommitQualification(t *testing.T) {
	f := makePackMetadataFixture(t)
	allLocalTestEnv(t, f.pack)
	for _, name := range []string{"shallow", "grafts", "replacement", "graft-env", "replace-env", "namespace-env", "object-env-empty", "config-env"} {
		t.Run(name, func(t *testing.T) {
			var path string
			switch name {
			case "shallow":
				path = filepath.Join(f.source, ".git", "shallow")
			case "grafts":
				path = filepath.Join(f.source, ".git", "info", "grafts")
			case "replacement":
				command(t, f.source, "update-ref", "refs/replace/"+f.tip, f.orphanCommit)
				t.Cleanup(func() { command(t, f.source, "update-ref", "-d", "refs/replace/"+f.tip) })
			case "graft-env":
				t.Setenv("GIT_GRAFT_FILE", "")
			case "replace-env":
				t.Setenv("GIT_REPLACE_REF_BASE", "refs/private-rewrite/")
			case "namespace-env":
				t.Setenv("GIT_NAMESPACE", "private")
			case "object-env-empty":
				t.Setenv("GIT_OBJECT_DIRECTORY", "")
			case "config-env":
				t.Setenv("GIT_CONFIG_COUNT", "0")
			}
			if path != "" {
				if err := os.WriteFile(path, []byte(f.tip+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := os.Remove(path); err != nil {
						t.Error(err)
					}
				})
			}
			tmp := t.TempDir()
			ctx := context.WithValue(t.Context(), importConfigurationKey{}, importConfiguration{pack: f.pack})
			info, used, err := loadPackSourceInfo(ctx, tmp, f.source, 20)
			if err == nil || !used || info != nil {
				t.Fatalf("unsupported raw-parent source must reject: info%v used%t err%v", info, used, err)
			}
			if files, err := os.ReadDir(tmp); err != nil || len(files) != 0 {
				t.Fatal("qualification allocated scratch", files, err)
			}
		})
	}
	// The public entry preserves Git's traversable history through the ordinary
	// path when raw commit headers are not authoritative.
	grafts := filepath.Join(f.source, ".git", "info", "grafts")
	if err := os.WriteFile(grafts, []byte(f.tip+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stats, err := Import(t.Context(), backend, ImportOptions{Repo: f.source, TempDir: t.TempDir(), CompressionWorkers: 3})
	if err != nil || stats.ImportMode != "reachable" {
		t.Fatalf("grafted source fallback: %+v %v", stats, err)
	}
	m, _, err := readHead(t.Context(), backend)
	if err != nil {
		t.Fatal(err)
	}
	want, err := strconv.ParseUint(archiveImportInput(t, f.source, "", "rev-list", "--count", "--all"), 10, 64)
	if err != nil || m.HistoryCount != want {
		t.Fatalf("grafted history count %d want%d: %v", m.HistoryCount, want, err)
	}

}

func TestAllLocalCommitFailureNeverPublishes(t *testing.T) {
	packMetadataRunInherited(t, 3, TestArchiveImportFailureNeverPublishes)
}
func TestAllLocalCommitCASPreservesWinner(t *testing.T) {
	packMetadataRunInherited(t, 1, TestArchiveImportCASPreservesWinner)
}
func TestAllLocalCommitCancellationJoinsCopy(t *testing.T) {
	packMetadataRunInherited(t, 1, TestArchiveImportCancellationJoinsCopy)
}
