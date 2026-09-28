// Development harness for publishing a prepared progressive snapshot to Store.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"gyit/internal/repo"
	"gyit/internal/store"
)

func run() error {
	source := flag.String("source", "", "existing packed Git fixture")
	location := flag.String("store", "", "destination object store")
	sha := flag.String("sha", "", "snapshot commit")
	state := flag.String("state", "", "local staging directory")
	cache := flag.String("cache", "", "disposable decoded cache")
	timeout := flag.Duration("timeout", 3*time.Minute, "maximum import duration")
	flag.Parse()
	if *source == "" || *location == "" || *sha == "" || *state == "" || *cache == "" {
		return fmt.Errorf("source, store, sha, state and cache are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := os.MkdirAll(*state, 0700); err != nil {
		return err
	}
	backend, err := store.Open(ctx, *location, "", "")
	if err != nil {
		return err
	}
	if c, ok := backend.(io.Closer); ok {
		defer c.Close()
	}
	disk, err := store.NewDiskCache(nil, *cache, *location, 4<<30)
	if err != nil {
		return err
	}
	defer disk.Close()
	p, err := repo.NewProgressive(ctx, backend, disk, *state)
	if err != nil {
		return err
	}
	started := time.Now()
	if err = p.ImportPacks(ctx, *source); err != nil {
		return err
	}
	packs := time.Since(started).Seconds()
	started = time.Now()
	if err = p.PrepareSnapshot(ctx, *sha); err != nil {
		return err
	}
	metadata := time.Since(started).Seconds()
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"sha": *sha, "pack_publication_seconds": packs, "metadata_publication_seconds": metadata, "total_seconds": packs + metadata})
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
