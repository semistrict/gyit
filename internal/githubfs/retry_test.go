package githubfs

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestTouchNoticeRetriesFailedSetup(t *testing.T) {
	f, opts, _, _ := openFixture(t)
	remote := filepath.Join(strings.TrimPrefix(opts.RemoteBase, "file://"), "acme", "project.git")
	if err := os.Rename(remote, remote+".saved"); err != nil {
		t.Fatal(err)
	}
	path := "acme/project"
	if _, err := f.ReadDir(t.Context(), path, "", 128); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	j := f.jobs["acme/project@"]
	done := j.done
	f.mu.Unlock()
	<-done
	if notice := read(t, f, path+"/NOTICE"); !strings.Contains(notice, "fatal:") || !strings.Contains(notice, "Touch NOTICE") {
		t.Fatalf("missing actionable failure: %s", notice)
	}
	if err := os.Rename(remote+".saved", remote); err != nil {
		t.Fatal(err)
	}
	for range 10 {
		if err := f.Retry(path + "/NOTICE"); err != nil {
			t.Fatal(err)
		}
	}
	waitReady(t, f, path)
	if got := read(t, f, path+"/dir/hello"); got != "first revision\n" {
		t.Fatal(got)
	}
	// A real NOTICE becomes immutable immediately after successful publication.
	if err := f.Retry(path + "/NOTICE"); !errors.Is(err, syscall.EROFS) {
		t.Fatalf("touch real NOTICE: %v", err)
	}
	if got := read(t, f, path+"/NOTICE"); got != "real repository notice\n" {
		t.Fatal(got)
	}
}
func TestVolumeNamespace(t *testing.T) {
	f, _, _, _ := openFixture(t)
	n := Namespace{f}
	entries, err := n.ReadDir(t.Context(), "", "", 128)
	if err != nil || len(entries) != 1 || entries[0].Name != "github.com" {
		t.Fatalf("volume root: %v %v", entries, err)
	}
	if _, err := n.Lookup(t.Context(), "acme/project"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("old root visible: %v", err)
	}
	path := "github.com/acme/project"
	if _, err := n.Lookup(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	waitReady(t, f, "acme/project")
	b := make([]byte, 100)
	count, err := n.Read(t.Context(), path+"/dir/hello", b, 0)
	if err != nil || string(b[:count]) != "first revision\n" {
		t.Fatalf("nested read: %s %v", b[:count], err)
	}
}
