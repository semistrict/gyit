package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"gyit/internal/githubfs"
	"gyit/internal/githubmount"
)

func mountGitHub(ctx context.Context, args []string) error {
	f := flag.NewFlagSet("mount", flag.ContinueOnError)
	data := f.String("data-dir", "", "prepared repository storage directory")
	cache := f.String("disk-cache-dir", "", "cache directory")
	mib := f.Int64("disk-cache-mib", 4096, "total cache MiB across all repositories (excludes durable storage), with 20 GiB free-space reserve")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() > 1 {
		return fmt.Errorf("usage: gyit mount [options] [mountpoint]")
	}
	if *mib < 0 || *mib > 1<<30 {
		return fmt.Errorf("invalid --disk-cache-mib")
	}
	mountpoint := "/Volumes/gyit"
	if f.NArg() == 1 {
		mountpoint = f.Arg(0)
	}
	if handled, err := nativeGitHubMount(ctx, mountpoint, *mib, *cache); handled {
		return err
	}
	if *cache == "" {
		base, err := os.UserCacheDir()
		if err != nil {
			return err
		}
		*cache = filepath.Join(base, "gyit", "github")
	}
	if *data == "" {
		*data = filepath.Join(filepath.Dir(*cache), "repositories")
	}
	token := os.Getenv("GH_TOKEN")
	if token == "" {
		token = os.Getenv("GITHUB_TOKEN")
	}
	source, err := githubfs.New(githubfs.Options{DataDir: *data, CacheDir: *cache, CacheBytes: *mib << 20, Token: token})
	if err != nil {
		return err
	}
	defer source.Close()
	return githubmount.Run(ctx, source, mountpoint)
}
