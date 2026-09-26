package macfs

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"gyit/internal/control"
	pb "gyit/internal/gen/gyit/control/v1"
	"gyit/internal/repo"
	"gyit/internal/store"
)

func TestSnapshotFilesystem(t *testing.T) {
	source := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		c := exec.Command("git", append([]string{"-C", source}, args...)...)
		c.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
		b, e := c.CombinedOutput()
		if e != nil {
			t.Fatalf("git: %v %s", e, b)
		}
		return string(bytes.TrimSpace(b))
	}
	git("init", "-q")
	os.Mkdir(filepath.Join(source, "dir"), 0755)
	os.WriteFile(filepath.Join(source, "dir", "hello"), []byte("hello from a snapshot\n"), 0644)
	os.Symlink("dir/hello", filepath.Join(source, "link"))
	git("add", ".")
	git("commit", "-qm", "fixture")
	sha := git("rev-parse", "HEAD")
	backend, e := store.NewLocal(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	if _, e = repo.Import(t.Context(), backend, repo.ImportOptions{Repo: source}); e != nil {
		t.Fatal(e)
	}
	r, e := repo.NewDisk(backend, t.TempDir(), "mac-fixture", 8<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	s, e := New(t.Context(), r, sha)
	if e != nil {
		t.Fatal(e)
	}
	root, e := s.Lookup(t.Context(), "")
	if e != nil || root.Mode != 0040000 {
		t.Fatalf("root %v %v", root, e)
	}
	entries, e := s.ReadDir(t.Context(), "", "", 1)
	if e != nil || len(entries) != 1 || entries[0].Name != "dir" {
		t.Fatalf("entries %v %v", entries, e)
	}
	entries, e = s.ReadDir(t.Context(), "", entries[0].Name, 1)
	if e != nil || len(entries) != 1 || entries[0].Name != "link" {
		t.Fatalf("page %v %v", entries, e)
	}
	b := make([]byte, 5)
	n, e := s.Read(t.Context(), "dir/hello", b, 6)
	if e != nil || n != 5 || string(b) != "from " {
		t.Fatalf("read %q %d %v", b, n, e)
	}
	link, e := s.Readlink(t.Context(), "link")
	if e != nil || link != "dir/hello" {
		t.Fatalf("link %q %v", link, e)
	}
	if _, e = s.Lookup(t.Context(), "../dir/hello"); e == nil {
		t.Fatal("accepted traversal")
	}
	if _, e = s.Lookup(t.Context(), "missing"); !repo.IsNotFound(e) {
		t.Fatalf("missing: %v", e)
	}
	var response *pb.Response
	send := func(r *pb.Response) error { response = r; return nil }
	if e = s.Serve(t.Context(), &pb.Request{Version: control.Version, Operation: &pb.Request_Switch{Switch: &pb.SwitchRequest{Revision: "HEAD"}}}, send); e != nil {
		t.Fatal(e)
	}
	if response.GetError() == nil {
		t.Fatal("native switch must fail until open-handle semantics are verified")
	}
	if e = s.Serve(t.Context(), &pb.Request{Version: control.Version, Operation: &pb.Request_Status{Status: &pb.StatusRequest{}}}, send); e != nil {
		t.Fatal(e)
	}
	if response.GetSnapshot().GetSha() != sha {
		t.Fatal("status lost pinned revision")
	}

}
