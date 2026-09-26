//go:build darwin && cgo

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

var configureGitOnce sync.Once
var configureGitError error

// /usr/bin/git is an xcrun launcher, which refuses App Sandbox execution.
// Select the installed toolchain binary for every Git subprocess, including
// importer subprocesses, before exposing the filesystem to concurrent calls.
func configureGit() error {
	configureGitOnce.Do(func() {
		current, err := exec.LookPath("git")
		if err == nil && current != "/usr/bin/git" {
			return
		}
		var candidates []string
		if developer, err := os.Readlink("/var/db/xcode_select_link"); err == nil && filepath.IsAbs(developer) {
			candidates = append(candidates, filepath.Join(developer, "usr/bin/git"))
		}
		candidates = append(candidates, "/Library/Developer/CommandLineTools/usr/bin/git", "/Applications/Xcode.app/Contents/Developer/usr/bin/git")
		for _, candidate := range candidates {
			info, err := os.Stat(candidate)
			if err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0 {
				configureGitError = os.Setenv("PATH", filepath.Dir(candidate)+string(os.PathListSeparator)+os.Getenv("PATH"))
				return
			}
		}
		configureGitError = fmt.Errorf("gyit requires Git from Xcode or Command Line Tools; the system Git launcher cannot run inside App Sandbox")
	})
	return configureGitError
}
