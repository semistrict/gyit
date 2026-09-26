package repo

import (
	"context"
	"gyit/internal/store"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkShowMedium(b *testing.B) {
	dir := os.Getenv("GYIT_BENCH_STORE")
	if dir == "" {
		dir = "../../.testdata/lima-store"
	}
	if _, err := os.Stat(filepath.Join(dir, "HEAD")); err != nil {
		b.Skip("import medium fixture first")
	}
	for _, warm := range []bool{false, true} {
		label := "fresh"
		if warm {
			label = "reused"
		}
		b.Run(label, func(b *testing.B) {
			local, _ := store.NewLocal(dir)
			measured := &countedStore{Store: local}
			var r *Repository
			var s *Snapshot
			open := func() {
				r, _ = New(measured, 32<<20)
				var err error
				s, err = r.OpenRevision(context.Background(), "main", "")
				if err != nil {
					b.Fatal(err)
				}
			}
			run := func() {
				if err := r.View(context.Background(), s, ViewOptions{Command: "show"}, io.Discard); err != nil {
					b.Fatal(err)
				}
			}
			open()
			if warm {
				run()
			}
			measured.reset()
			gets, read := 0, 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !warm {
					b.StopTimer()
					open()
					measured.reset()
					b.StartTimer()
				}
				start := time.Now()
				run()
				if time.Since(start) > time.Second {
					b.Logf("SLOW (>1s): show %s", time.Since(start))
				}
				if !warm {
					gets += measured.gets
					read += measured.bytes
				}
			}
			b.StopTimer()
			if warm {
				gets, read = measured.gets, measured.bytes
			}
			b.ReportMetric(float64(gets)/float64(b.N), "GETs/op")
			b.ReportMetric(float64(read)/float64(b.N), "fetched-B/op")
		})
	}
}
