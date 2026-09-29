//go:build !js

package repo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gyit/internal/store"
)

// Replays a real acquisition window without fetching anything. The input packs
// must include delta bases; publication goes only to a disposable local store.
// A separate complete source supplies Git's baseline, never the gyit reader.
func TestShallowDirectoryHistoryLatency(t *testing.T) {
	dir, packs, sha, baseline := os.Getenv("GYIT_SHALLOW_SOURCE"), os.Getenv("GYIT_SHALLOW_PACKS"), os.Getenv("GYIT_HISTORY_SHA"), os.Getenv("GYIT_HISTORY_SOURCE")
	if dir == "" || packs == "" || sha == "" || baseline == "" {
		t.Skip("set shallow source/packs, history SHA and baseline source")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	source := t.TempDir()
	dest := filepath.Join(source, "objects", "pack")
	if err := os.MkdirAll(dest, 0700); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, id := range strings.Split(packs, ",") {
		if !validProgressiveOID(id) {
			t.Fatal("invalid pack identity")
		}
		for _, ext := range []string{".idx", ".pack"} {
			name := "pack-" + id + ext
			from := filepath.Join(dir, "objects", "pack", name)
			info, err := os.Stat(from)
			if err != nil {
				t.Fatal(err)
			}
			total += info.Size()
			if err := os.Symlink(from, filepath.Join(dest, name)); err != nil {
				t.Fatal(err)
			}
		}
	}
	backend, _ := store.NewLocal(t.TempDir())
	p, err := NewProgressive(ctx, backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := p.ImportPacks(ctx, source); err != nil {
		t.Fatal(err)
	}
	t.Logf("acquired input=%d bytes local publication=%s", total, time.Since(start))
	// No indexing or acquisition worker is running. Both paths must be served
	// from the same already acquired repository-wide window.
	p.UsePublishedHistorySources(source)
	p.Demand = func(context.Context, []string) error { return fmt.Errorf("unexpected additional acquisition") }
	for _, path := range []string{"lib", "Documentation/"} {
		for _, n := range []int{1, 10} {
			t.Run(fmt.Sprintf("%s/n%d", path, n), func(t *testing.T) {
				s, err := p.Open(ctx, sha)
				if err != nil {
					t.Fatal(err)
				}
				start := time.Now()
				var first time.Duration
				var got []string
				err = s.LogWithOptions(ctx, LogOptions{Count: n, FullCommitIDs: true, Paths: []string{path}}, func(e LogEntry) error {
					if len(got) == 0 {
						first = time.Since(start)
					}
					got = append(got, e.SHA)
					return nil
				})
				elapsed := time.Since(start)
				t.Logf("first=%s total=%s results=%d", first, elapsed, len(got))
				if err != nil {
					t.Fatal(err)
				}
				start = time.Now()
				cmd := exec.CommandContext(ctx, "git", "-C", baseline, "log", "--format=%H", "-n", fmt.Sprint(n), sha, "--", path)
				cmd.Env = append(os.Environ(), "GIT_NO_LAZY_FETCH=1", "GIT_TERMINAL_PROMPT=0", "GIT_ALLOW_PROTOCOL=")
				want, err := cmd.Output()
				if err != nil {
					t.Fatal(err)
				}
				native := time.Since(start)
				if strings.Join(got, "\n") != strings.TrimSpace(string(want)) {
					t.Fatalf("history differs: %v vs %s", got, want)
				}
				t.Logf("native=%s ratio=%.2fx", native, float64(elapsed)/float64(native))
			})
		}
	}
}
