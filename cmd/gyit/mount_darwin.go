package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
)

func nativeGitHubMount(ctx context.Context, mountpoint string, budget int64, cache string) (bool, error) {
	if cache != "" {
		return true, fmt.Errorf("native mounts manage the cache inside the extension container")
	}
	path, err := filepath.Abs(mountpoint)
	if err != nil {
		return true, err
	}
	c := exec.CommandContext(ctx, "/sbin/mount", nativeMountArgs(path, budget)...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err = c.Run(); err != nil {
		return true, fmt.Errorf("native mount: %w; install 🍑gyit and enable gyitfs (macos/README.md)", err)
	}
	return true, nil
}
func nativeMountArgs(mountpoint string, budget int64) []string {
	return []string{"-F", "-t", "gyit", "-o", "gyitcachemib=" + strconv.FormatInt(budget, 10), "https://github.com", mountpoint}
}
