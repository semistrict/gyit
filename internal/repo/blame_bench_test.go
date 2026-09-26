package repo

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"gyit/internal/store"
)

// Reuses the imported medium fixture. Compare the same snapshot with and without
// the acceleration index; no imports, network calls or fixture mutations here.
func BenchmarkBlameMedium(b *testing.B) {
	dir := os.Getenv("GYIT_BENCH_STORE")
	if dir == "" {
		dir = "../../.testdata/lima-store"
	}
	if _, err := os.Stat(filepath.Join(dir, "HEAD")); err != nil {
		b.Skip("import the medium fixture or set GYIT_BENCH_STORE")
	}
	for _, indexed := range []bool{false, true} {
		name := "legacy"
		if indexed {
			name = "indexed"
		}
		for _, warm := range []bool{false, true} {
			state := "fresh"
			if warm {
				state = "reused"
			}
			b.Run(name+"/"+state, func(b *testing.B) {
				local, err := store.NewLocal(dir)
				if err != nil {
					b.Fatal(err)
				}
				measured := &countedStore{Store: local}
				open := func() *Snapshot {
					r, _ := New(measured, 32<<20)
					s, err := r.OpenRevision(context.Background(), "595cc91e8cbb1c2ca822d0311dcf12709410c582", "")
					if err != nil {
						b.Fatal(err)
					}
					if indexed && (s.history == nil || s.history.root == (pageRef{})) {
						b.Fatal("fixture needs an import to build the history index")
					}
					if !indexed {
						s.history = nil
					}
					return s
				}
				s := open()
				blame := func() {
					lines := 0
					if err := s.Blame(context.Background(), BlameOptions{Path: "README.md", End: 40}, func(BlameLine) error { lines++; return nil }); err != nil {
						b.Fatal(err)
					}
					if lines != 40 {
						b.Fatalf("got %d lines", lines)
					}
				}
				if warm {
					blame()
				}
				measured.reset()
				totalGets, totalBytes := 0, 0
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if !warm {
						b.StopTimer()
						s = open()
						measured.reset()
						b.StartTimer()
					}
					blame()
					if !warm {
						totalGets += measured.gets
						totalBytes += measured.bytes
					}
				}
				b.StopTimer()
				if warm {
					totalGets = measured.gets
					totalBytes = measured.bytes
				}
				b.ReportMetric(float64(totalGets)/float64(b.N), "GETs/op")
				b.ReportMetric(float64(totalBytes)/float64(b.N), "fetched-B/op")
			})
		}
	}
}
