package repo

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gyit/internal/store"
)

// BenchmarkSnapshotScan isolates metadata serving from kernel/bridge overhead.
// The opt-in fixture is imported once and reused across runs; use a new data
// directory when changing the source revision. No file contents are read.
func BenchmarkSnapshotScan(b *testing.B) {
	source, data := os.Getenv("GYIT_SCAN_SOURCE"), os.Getenv("GYIT_SCAN_DATA")
	if source == "" || data == "" {
		b.Skip("set GYIT_SCAN_SOURCE and GYIT_SCAN_DATA to benchmark a local checkout")
	}
	ctx, cancel := context.WithTimeout(b.Context(), 90*time.Second)
	defer cancel()
	backend, err := store.NewLocal(filepath.Join(data, "store"))
	if err != nil {
		b.Fatal(err)
	}
	if _, _, err := backend.Get(ctx, "HEAD", 0, -1); err != nil {
		if _, err := Import(ctx, backend, ImportOptions{Repo: source}); err != nil {
			b.Fatal(err)
		}
	}
	r, err := NewDisk(backend, filepath.Join(data, "cache"), "scan", 4<<30)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()
	sha, err := git(ctx, source, "rev-parse", "HEAD").Output()
	if err != nil {
		b.Fatal(err)
	}
	s, err := r.Open(ctx, string(sha[:len(sha)-1]))
	if err != nil {
		b.Fatal(err)
	}
	type node struct{ path, tree string }
	scan := func(resolve, stat, verify bool) (int, error) {
		pending := []node{{tree: s.Tree}}
		count := 0
		for len(pending) > 0 {
			d := pending[len(pending)-1]
			pending = pending[:len(pending)-1]
			if resolve {
				e, err := s.Resolve(ctx, d.path)
				if err != nil {
					return 0, err
				}
				d.tree = e.OID
			}
			after := ""
			for {
				entries, err := s.ReadDir(ctx, d.tree, after, 128)
				if err != nil {
					return 0, err
				}
				for _, e := range entries {
					path := e.Name
					if d.path != "" {
						path = d.path + "/" + path
					}
					if stat {
						got, err := s.Resolve(ctx, path)
						if err != nil {
							return 0, err
						}
						if got != e {
							return 0, fmt.Errorf("stat differs for %s", path)
						}
					}
					if verify {
						info, err := os.Lstat(filepath.Join(source, path))
						if err != nil {
							return 0, err
						}
						if info.IsDir() != (e.Mode == 0040000 || e.Mode == 0160000) || (!info.IsDir() && info.Size() != e.Size) {
							return 0, fmt.Errorf("checkout differs for %s", path)
						}
					}
					count++
					if e.Mode == 0040000 {
						pending = append(pending, node{path, e.OID})
					}
				}
				if len(entries) < 128 {
					break
				}
				after = entries[len(entries)-1].Name
			}
		}
		return count, nil
	}
	want, err := scan(true, true, true)
	if err != nil {
		b.Fatal(err)
	}
	for _, mode := range []struct {
		name          string
		resolve, stat bool
	}{{"directory-handle", false, false}, {"path-list", true, false}, {"path-stat", true, true}} {
		b.Run(mode.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				count, err := scan(mode.resolve, mode.stat, false)
				if err != nil || count != want {
					b.Fatalf("scan %d/%d: %v", count, want, err)
				}
			}
			b.ReportMetric(float64(want), "entries/scan")
		})
	}
}
