// Development harness: report first-build and reader costs separately. Git is
// used only as the benchmark oracle, never by the repository query engine.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"gyit/internal/repo"
	"gyit/internal/store"
)

func run() error {
	baseLocation := flag.String("base-store", "", "read-only base; store must be an empty isolated overlay and ingest-from is required")
	ingestTimeout := flag.Duration("ingest-timeout", 2*time.Minute, "deadline for complete history ingestion")
	location := flag.String("store", "", "existing object store")
	sha := flag.String("sha", "", "full revision")
	path := flag.String("path", "README.md", "literal file path")
	cache := flag.String("cache", "", "decoded cache directory (use a fresh directory for cold reads)")
	source := flag.String("ingest-from", "", "writer acquisition Git directory; ingest complete file history before measuring first reads")
	oracle := flag.String("git-dir", "", "optional native Git directory for timing and exact output comparison")
	count := flag.Int("n", 10, "number of entries")
	runs := flag.Int("runs", 5, "reader repetitions")
	timeout := flag.Duration("timeout", 20*time.Second, "deadline per operation")
	flag.Parse()
	if *location == "" || *sha == "" || *cache == "" || *runs < 1 || *runs > 100 || *timeout <= 0 {
		return fmt.Errorf("store, sha, cache, positive timeout and runs 1..100 are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	backend, err := store.Open(ctx, *location, "", "")
	if err != nil {
		return err
	}
	if c, ok := backend.(io.Closer); ok {
		defer c.Close()
	}
	if *baseLocation != "" {
		if *source == "" {
			return fmt.Errorf("base-store requires ingest-from")
		}
		if _, _, e := backend.Get(ctx, "HEAD", 0, -1); !errors.Is(e, store.ErrNotFound) {
			return fmt.Errorf("overlay must be empty")
		}
		base, e := store.Open(ctx, *baseLocation, "", "")
		if e != nil {
			return e
		}
		if c, ok := base.(io.Closer); ok {
			defer c.Close()
		}
		backend = &benchmarkOverlay{Store: backend, base: base, keys: map[string]bool{}}
		head, _, e := base.Get(ctx, "HEAD", 0, -1)
		if e != nil {
			return e
		}
		if e = backend.Put(ctx, "HEAD", head, "*"); e != nil {
			return e
		}
	}
	disk, err := store.NewDiskCache(nil, *cache, *location, 4<<30)
	if err != nil {
		return err
	}
	defer disk.Close()
	enc := json.NewEncoder(os.Stdout)
	if *source != "" {
		ctx, cancel := context.WithTimeout(context.Background(), *ingestTimeout)
		defer cancel()
		start := time.Now()
		p, err := repo.NewProgressive(ctx, backend, disk, os.TempDir())
		if err != nil {
			return err
		}
		err = p.IngestHistory(ctx, *sha, *source)
		enc.Encode(map[string]any{"operation": "ingest", "seconds": time.Since(start).Seconds(), "error": errorText(err)})
		if err != nil {
			return err
		}
	}
	for i := 0; i < *runs; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		start := time.Now()
		p, err := repo.NewProgressive(ctx, backend, disk, os.TempDir())
		var out bytes.Buffer
		if err == nil {
			var snapshot *repo.Snapshot
			snapshot, err = p.Open(ctx, *sha)
			if err == nil {
				first := true
				err = snapshot.LogWithOptions(ctx, repo.LogOptions{Count: *count, Paths: []string{":(literal,top)" + *path}, FullCommitIDs: true}, func(e repo.LogEntry) error {
					if !first {
						out.WriteByte('\n')
					}
					first = false
					return repo.WriteLogEntry(&out, e, false)
				})
			}
		}
		elapsed := time.Since(start).Seconds()
		cancel()
		if err != nil {
			return err
		}
		row := map[string]any{"operation": "read", "run": i, "seconds": elapsed, "output_bytes": out.Len()}
		if *oracle != "" {
			ctx, cancel := context.WithTimeout(context.Background(), *timeout)
			cmd := exec.CommandContext(ctx, "git", "--no-pager", "--git-dir="+*oracle, "-c", "color.ui=false", "log", "--no-decorate", "--format=medium", fmt.Sprintf("-n%d", *count), *sha, "--", ":(literal,top)"+*path)
			cmd.Env = append(os.Environ(), "GIT_NO_LAZY_FETCH=1", "LC_ALL=C", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
			start = time.Now()
			want, err := cmd.Output()
			native := time.Since(start).Seconds()
			cancel()
			if err != nil {
				return err
			}
			if !bytes.Equal(out.Bytes(), want) {
				return fmt.Errorf("formatted output differs from Git")
			}
			row["git_seconds"] = native
			row["ratio"] = elapsed / native
			row["output_matches_git"] = true
		}
		if err = enc.Encode(row); err != nil {
			return err
		}
	}
	return nil
}
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// The overlay is single-run benchmark scratch. Keys written this run route to
// scratch; all other immutable keys route directly to the read-only base.
// It avoids both live-store mutations and extra failed GETs in measured reads.
type benchmarkOverlay struct {
	store.Store
	base store.Store
	mu   sync.RWMutex
	keys map[string]bool
}

func (s *benchmarkOverlay) Get(ctx context.Context, k string, o, n int64) ([]byte, string, error) {
	s.mu.RLock()
	own := s.keys[k]
	s.mu.RUnlock()
	if own {
		return s.Store.Get(ctx, k, o, n)
	}
	return s.base.Get(ctx, k, o, n)
}
func (s *benchmarkOverlay) Put(ctx context.Context, k string, b []byte, c string) error {
	if err := s.Store.Put(ctx, k, b, c); err != nil {
		return err
	}
	s.mu.Lock()
	s.keys[k] = true
	s.mu.Unlock()
	return nil
}
