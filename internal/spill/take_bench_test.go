package spill_test

import (
	"fmt"
	"testing"

	"gyit/internal/spill"
)

func BenchmarkCombineStagedRecords(b *testing.B) {
	for _, transfer := range []bool{false, true} {
		b.Run(fmt.Sprintf("transfer-%t", transfer), func(b *testing.B) {
			parent := b.TempDir()
			const count = 65536
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				src, err := spill.New(parent, 1<<20)
				if err != nil {
					b.Fatal(err)
				}
				dst, err := spill.New(parent, 1<<20)
				if err != nil {
					src.Close()
					b.Fatal(err)
				}
				for i := count - 1; i >= 0; i-- {
					key := []byte(fmt.Sprintf("g/%040x", i))
					if err := src.Add(key, key); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				if transfer {
					err = dst.Take(src)
				} else {
					err = src.Walk(b.Context(), dst.Add)
				}
				if err != nil {
					b.Fatal(err)
				}
				n := 0
				err = dst.Walk(b.Context(), func(k, v []byte) error {
					if string(k) != fmt.Sprintf("g/%040x", n) || string(k) != string(v) {
						b.Fatalf("incorrect record %d", n)
					}
					n++
					return nil
				})
				if err != nil || n != count {
					b.Fatalf("count=%d error=%v", n, err)
				}
				if err := src.Close(); err != nil {
					b.Fatal(err)
				}
				if err := dst.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
