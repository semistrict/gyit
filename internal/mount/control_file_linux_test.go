//go:build linux

package mount

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"gat/internal/controlcli"
	"gat/internal/repo"
	"gat/internal/store"
)

func TestMountedControlFileWithoutSocket(t *testing.T) {
	if os.Getenv("GAT_FUSE_TEST") != "1" {
		t.Skip("set GAT_FUSE_TEST=1 on Linux with FUSE")
	}
	source := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", source}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git: %v: %s", err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "-b", "main")
	if err := os.Mkdir(filepath.Join(source, "sub"), 0755); err != nil {
		t.Fatal(err)
	}
	var revisions []string
	for _, body := range []string{"one\n", "two\n", "three\n"} {
		if err := os.WriteFile(filepath.Join(source, "sub", "file"), []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
		git("add", ".")
		git("commit", "-qm", strings.Repeat("message ", 3000))
		revisions = append(revisions, git("rev-parse", "HEAD"))
	}
	if err := os.WriteFile(filepath.Join(source, ".gat.control"), []byte("tracked file must never be hidden"), 0644); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "reserved path")
	conflicting := git("rev-parse", "HEAD")
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Import(t.Context(), backend, repo.ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	r, err := repo.NewDisk(backend, t.TempDir(), "mounted-fixture", 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	mp := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var mountErr error
	go func() { defer close(done); mountErr = Run(ctx, r, revisions[2], mp, "") }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("mount did not shut down")
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(mp, "sub", "file")); err == nil {
			break
		}
		select {
		case <-done:
			t.Fatalf("mount without socket failed: %v", mountErr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("mount not ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Chdir(filepath.Join(mp, "sub"))
	cli := func(args ...string) string {
		t.Helper()
		var out, stderr bytes.Buffer
		if err := controlcli.Run(t.Context(), args, &out, &stderr); err != nil {
			t.Fatalf("%v: %v: %s", args, err, &stderr)
		}
		return out.String()
	}
	if got := cli("rev-parse", "HEAD"); got != revisions[2]+"\n" {
		t.Fatal(got)
	}
	info, err := os.Stat(filepath.Join(mp, ".gat.control"))
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatalf("virtual control file: %v %v", info, err)
	}
	entries, err := os.ReadDir(mp)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "sub" {
		t.Fatalf("control file leaked into listing: %v", entries)
	}
	if got := cli("log", "-n", "3"); len(got) < 64<<10 || strings.Count(got, "commit ") != 3 {
		t.Fatal("multi-frame log was truncated")
	}
	cli("switch", revisions[0])
	if data, err := os.ReadFile("file"); err != nil || string(data) != "one\n" {
		t.Fatalf("switched file: %q %v", data, err)
	}
	cli("switch", "-")
	var rejected, rejection bytes.Buffer
	if err := controlcli.Run(t.Context(), []string{"checkout", conflicting}, &rejected, &rejection); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("checkout must reject a tracked control-path collision: %v", err)
	}
	if got := cli("rev-parse", "HEAD"); got != revisions[2]+"\n" {
		t.Fatalf("rejected checkout changed HEAD: %s", got)
	}
	mutations := map[string]func() error{
		"write":    func() error { return os.WriteFile("file", []byte("bad"), 0644) },
		"truncate": func() error { return os.Truncate("file", 0) },
		"chmod":    func() error { return os.Chmod("file", 0600) },
		"create":   func() error { return os.WriteFile("new", []byte("bad"), 0644) },
		"mkdir":    func() error { return os.Mkdir("newdir", 0755) },
		"unlink":   func() error { return os.Remove("file") },
		"rmdir":    func() error { return os.Remove(filepath.Join(mp, "sub")) },
		"rename":   func() error { return os.Rename("file", "renamed") },
		"symlink":  func() error { return os.Symlink("file", "link") },
		"link":     func() error { return os.Link("file", "hardlink") },
	}
	for name, mutate := range mutations {
		if err := mutate(); !errors.Is(err, syscall.EROFS) && !errors.Is(err, syscall.EACCES) && !errors.Is(err, syscall.EPERM) {
			t.Errorf("%s must reject modification: %v", name, err)
		}
	}
	if data, err := os.ReadFile("file"); err != nil || string(data) != "three\n" {
		t.Fatalf("repository changed: %q %v", data, err)
	}
}
