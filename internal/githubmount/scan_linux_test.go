//go:build linux

package githubmount

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"gyit/internal/githubfs"
)

// TestMountedScan uses a persistent local fixture and the same traversal on
// both filesystems. Keep both the checkout and data on the VM's local disk,
// not a host-shared filesystem. Import is outside the measured scan.
func TestMountedScan(t *testing.T) {
	source, data, script := os.Getenv("GYIT_SCAN_SOURCE"), os.Getenv("GYIT_SCAN_DATA"), os.Getenv("GYIT_SCAN_SCRIPT")
	if source == "" || data == "" || script == "" {
		t.Skip("set GYIT_SCAN_SOURCE, GYIT_SCAN_DATA, and GYIT_SCAN_SCRIPT on Linux with FUSE")
	}
	remote := filepath.Join(data, "remotes", "acme", "project.git")
	if _, err := os.Stat(remote); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(remote), 0700); err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(t.Context(), "git", "clone", "--bare", "--no-hardlinks", source, remote)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("local fixture: %v: %s", err, out)
		}
	}
	f, err := githubfs.New(githubfs.Options{DataDir: filepath.Join(data, "repositories"), CacheDir: filepath.Join(data, "cache"), CacheBytes: 4 << 30, RemoteBase: "file://" + filepath.Join(data, "remotes")})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.ReadDir(t.Context(), "acme/project", "", 128); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Minute)
	for f.Generation("acme/project") < 2 {
		if time.Now().After(deadline) {
			b := make([]byte, 512)
			n, _ := f.Read(t.Context(), "acme/project/NOTICE", b, 0)
			t.Fatalf("fixture setup: %s", b[:n])
		}
		time.Sleep(20 * time.Millisecond)
	}
	mountpoint := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, f, mountpoint) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("scan mount did not shut down")
		}
	}()
	deadline = time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(mountpoint, "github.com")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("mount did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	label := os.Getenv("GYIT_SCAN_LABEL")
	if label == "" {
		label = "fuse-fresh-mount-existing-disk-cache"
	}
	cmd := exec.CommandContext(t.Context(), "python3", script, "--mount", filepath.Join(mountpoint, "github.com", "acme", "project"), "--checkout", source, "--label", label, "--output", filepath.Join(data, label+".json"))
	out, err := cmd.CombinedOutput()
	t.Logf("%s", out)
	if err != nil {
		t.Fatalf("full scan performance or correctness gate: %v", err)
	}
}
