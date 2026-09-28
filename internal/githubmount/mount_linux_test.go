//go:build linux

package githubmount

import (
	"bytes"
	"context"
	"errors"
	"gyit/internal/control"
	"gyit/internal/controlcli"
	"gyit/internal/githubfs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestMountedBackgroundPublication(t *testing.T) {
	if os.Getenv("GYIT_GITHUB_FUSE_TEST") != "1" {
		t.Skip("set GYIT_GITHUB_FUSE_TEST=1 on Linux with FUSE")
	}
	root := t.TempDir()
	source := filepath.Join(root, "source")
	os.Mkdir(source, 0700)
	git := func(args ...string) string {
		t.Helper()
		c := exec.Command("git", append([]string{"-C", source}, args...)...)
		c.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
		b, err := c.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, b)
		}
		return strings.TrimSpace(string(b))
	}
	git("init", "-q", "-b", "main")
	os.WriteFile(filepath.Join(source, "hello"), []byte("ready\n"), 0600)
	os.Mkdir(filepath.Join(source, "sub"), 0700)
	os.WriteFile(filepath.Join(source, "sub", "removed"), []byte("old\n"), 0600)
	git("add", ".")
	git("commit", "-qm", "fixture")
	remote := filepath.Join(root, "remotes", "acme")
	os.MkdirAll(remote, 0700)
	git("clone", "--bare", "--quiet", source, filepath.Join(remote, "project.git"))
	// Delay the real local Git transport until after the mounted NOTICE is read.
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(root, "bin")
	os.Mkdir(bin, 0700)
	gate := filepath.Join(root, "continue")
	script := "#!/bin/sh\ncase \" $* \" in *' ls-remote '*) while [ ! -f '" + gate + "' ]; do sleep 0.01; done;; esac\nexec '" + realGit + "' \"$@\"\n"
	os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0700)
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	f, err := githubfs.New(githubfs.Options{DataDir: filepath.Join(root, "data"), CacheDir: filepath.Join(root, "cache"), CacheBytes: 16 << 20, RemoteBase: "file://" + filepath.Dir(remote)})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	mountpoint := filepath.Join(root, "mount")
	os.Mkdir(mountpoint, 0700)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, f, mountpoint) }()
	defer func() {
		os.WriteFile(gate, nil, 0600)
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("mount did not shut down")
		}
	}()
	path := filepath.Join(mountpoint, "github.com", "acme", "project")
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err = os.Stat(path); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	notice, err := os.ReadFile(filepath.Join(path, "NOTICE"))
	if err != nil || !strings.Contains(string(notice), "preparing") {
		t.Fatalf("notice %q %v", notice, err)
	}
	if err := os.Chtimes(filepath.Join(path, "NOTICE"), time.Now(), time.Now()); err != nil {
		t.Fatalf("touch setup NOTICE: %v", err)
	}
	if _, err = os.Stat(filepath.Join(path, "hello")); !os.IsNotExist(err) {
		t.Fatalf("premature file: %v", err)
	}
	if entries, err := os.ReadDir(path); err != nil || len(entries) != 1 || entries[0].Name() != "NOTICE" {
		t.Fatalf("setup directory: %v %v", entries, err)
	}
	os.WriteFile(gate, nil, 0600)
	deadline = time.Now().Add(15 * time.Second)
	for {
		b, err := os.ReadFile(filepath.Join(path, "hello"))
		if err == nil {
			if string(b) != "ready\n" {
				t.Fatal(string(b))
			}
			break
		}
		if time.Now().After(deadline) {
			b, _ := os.ReadFile(filepath.Join(path, "NOTICE"))
			t.Fatalf("setup timeout: %s", b)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err = os.Stat(filepath.Join(path, "NOTICE")); !os.IsNotExist(err) {
		t.Fatalf("NOTICE survived publication: %v", err)
	}
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(path); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(originalDir)
	endpoint, err := control.Discover(path)
	if err != nil || endpoint != "fuse:"+filepath.Join(path, control.ControlFileName) {
		t.Fatalf("mount-local command endpoint %q: %v", endpoint, err)
	}
	// An unprivileged caller may be rejected by kernel permissions before the
	// request reaches the server's EROFS response. Both must leave the file intact.
	if err := os.Remove(filepath.Join(path, "hello")); !errors.Is(err, syscall.EROFS) && !errors.Is(err, syscall.EACCES) {
		t.Fatalf("unlink must remain read-only: %v", err)
	}
	if _, err := os.Stat(filepath.Join(path, "hello")); err != nil {
		t.Fatalf("failed unlink lost the file: %v", err)
	}
	var log, logErrors bytes.Buffer
	if err := controlcli.Run(t.Context(), []string{"log", "--oneline", "-n", "1"}, &log, &logErrors); err != nil {
		t.Fatal(err)
	}
	expectedLog := git("log", "--oneline", "--no-decorate", "-n", "1")
	if strings.TrimSpace(log.String()) != expectedLog {
		t.Fatalf("mounted log: %q != %q", log.String(), expectedLog)
	}
	if _, err := os.Stat(filepath.Join(path, ".git")); !os.IsNotExist(err) {
		t.Fatalf("unexpected virtual Git directory: %v", err)
	}
	after, err := os.Stat(path)
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("repository identity changed: %v", err)
	}
	if err := os.Chtimes(filepath.Join(path, "hello"), time.Now(), time.Now()); err == nil {
		t.Fatal("touch changed a repository file")
	}
	if err = os.WriteFile(filepath.Join(path, "new"), []byte("bad"), 0600); err == nil {
		t.Fatal("mount accepted a write")
	}
	// Populate kernel directory caches before changing both root and subtree.
	for _, dir := range []string{path, filepath.Join(path, "sub")} {
		for i := 0; i < 2; i++ {
			if _, err := os.ReadDir(dir); err != nil {
				t.Fatal(err)
			}
		}
	}
	oldFile, err := os.Open(filepath.Join(path, "hello"))
	if err != nil {
		t.Fatal(err)
	}
	defer oldFile.Close()
	os.WriteFile(filepath.Join(source, "hello"), []byte("new contents\n"), 0600)
	os.WriteFile(filepath.Join(source, "added"), []byte("new\n"), 0600)
	os.Remove(filepath.Join(source, "sub", "removed"))
	os.WriteFile(filepath.Join(source, "sub", "added"), []byte("new\n"), 0600)
	git("add", "-A")
	git("commit", "-qm", "update fixture")
	git("push", "--quiet", filepath.Join(remote, "project.git"), "main")
	var updated, updateErrors bytes.Buffer
	if err := controlcli.Run(t.Context(), []string{"update"}, &updated, &updateErrors); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(path, "sub"))
	if err != nil || len(entries) != 1 || entries[0].Name() != "added" {
		t.Fatalf("cached subtree survived update: %v %v", entries, err)
	}
	if _, err := os.Stat(filepath.Join(path, "added")); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(path, "hello")); err != nil || string(b) != "new contents\n" {
		t.Fatalf("new open after update: %q %v", b, err)
	}
	b := make([]byte, 6)
	if _, err := oldFile.ReadAt(b, 0); err != nil || string(b) != "ready\n" {
		t.Fatalf("existing file handle changed snapshot: %q %v", b, err)
	}
}
