package repo

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"gyit/internal/store"
)

func BenchmarkLogFileMedium(b *testing.B) {
	benchmarkLogMedium(b, LogOptions{Count: 20, Paths: []string{"README.md"}})
}
func BenchmarkLogFollowMedium(b *testing.B) {
	benchmarkLogMedium(b, LogOptions{Count: 20, Follow: true, Paths: []string{"README.md"}})
}
func BenchmarkLogPathspecMedium(b *testing.B) {
	benchmarkLogMedium(b, LogOptions{Count: 20, Paths: []string{":(glob)docs/**/*.md"}})
}
func benchmarkLogMedium(b *testing.B, options LogOptions) {
	dir := os.Getenv("GYIT_BENCH_STORE")
	if dir == "" {
		dir = "../../.testdata/lima-store"
	}
	if _, err := os.Stat(filepath.Join(dir, "HEAD")); err != nil {
		b.Skip("import the medium fixture or set GYIT_BENCH_STORE")
	}
	for _, warm := range []bool{false, true} {
		name := "fresh"
		if warm {
			name = "reused"
		}
		b.Run(name, func(b *testing.B) {
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
				return s
			}
			s := open()
			run := func() {
				n := 0
				if err := s.LogWithOptions(context.Background(), options, func(LogEntry) error { n++; return nil }); err != nil {
					b.Fatal(err)
				}
				if n != 20 {
					b.Fatalf("got %d entries", n)
				}
			}
			if warm {
				run()
			}
			measured.reset()
			gets, fetched := 0, 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !warm {
					b.StopTimer()
					s = open()
					measured.reset()
					b.StartTimer()
				}
				run()
				if !warm {
					gets += measured.gets
					fetched += measured.bytes
				}
			}
			b.StopTimer()
			if warm {
				gets, fetched = measured.gets, measured.bytes
			}
			b.ReportMetric(float64(gets)/float64(b.N), "GETs/op")
			b.ReportMetric(float64(fetched)/float64(b.N), "fetched-B/op")
		})
	}
}
