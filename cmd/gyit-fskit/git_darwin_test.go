//go:build darwin && cgo

package main

import (
	"os/exec"
	"strings"
	"testing"
)

func TestNativeGitAvoidsSandboxIncompatibleLauncher(t *testing.T) {
	t.Setenv("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
	if err := configureGit(); err != nil {
		t.Fatal(err)
	}
	path, err := exec.LookPath("git")
	if err != nil || path == "/usr/bin/git" {
		t.Fatalf("native Git still uses xcrun launcher: %q %v", path, err)
	}
	out, err := exec.Command("git", "--version").CombinedOutput()
	if err != nil || !strings.HasPrefix(string(out), "git version ") {
		t.Fatalf("real Git unavailable: %s %v", out, err)
	}
}
