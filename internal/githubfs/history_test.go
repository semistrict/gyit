package githubfs

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gyit/internal/repo"
)

func TestSetupImportsFullHistoryAndAllRefs(t *testing.T) {
	opts, first, second := fixture(t)
	remote := filepath.Join(strings.TrimPrefix(opts.RemoteBase, "file://"), "acme", "project.git")
	if out, err := exec.Command("git", "-C", remote, "tag", "release", second).CombinedOutput(); err != nil {
		t.Fatalf("tag: %v %s", err, out)
	}
	// An old shallow publication must never satisfy a full-history setup.
	legacy := filepath.Join(opts.DataDir, "snapshots", storeID(Target{Owner: "acme", Repository: "project"}, first))
	if err := os.MkdirAll(legacy, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "HEAD"), []byte("legacy"), 0600); err != nil {
		t.Fatal(err)
	}
	f, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r, pinned, err := f.prepare(t.Context(), Target{Owner: "acme", Repository: "project"}, func(string) {})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if pinned.SHA != first {
		t.Fatalf("pin = %s", pinned.SHA)
	}
	// Read only gyit storage after deleting the transport source.
	if err := os.RemoveAll(strings.TrimPrefix(opts.RemoteBase, "file://")); err != nil {
		t.Fatal(err)
	}
	for _, rev := range []string{"feature/login", "release", second} {
		snap, err := r.OpenRevision(t.Context(), rev, "")
		if err != nil {
			t.Fatal(err)
		}
		var commits []string
		err = snap.Log(t.Context(), 10, false, func(e repo.LogEntry) error { commits = append(commits, e.SHA); return nil })
		if err != nil {
			t.Fatal(err)
		}
		if len(commits) != 2 || commits[0] != second || commits[1] != first {
			t.Fatalf("%s history: %v", rev, commits)
		}
	}
	old, err := r.OpenRevision(t.Context(), "feature/login~1", "")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := old.Resolve(t.Context(), "dir/hello")
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, entry.Size)
	if _, err := old.ReadAt(t.Context(), entry.OID, b, 0); err != nil {
		t.Fatal(err)
	}
	if string(b) != "first revision\n" {
		t.Fatalf("historical content: %q", b)
	}
}
