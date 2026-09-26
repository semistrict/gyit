package repo

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// This opt-in metadata benchmark samples existing index OIDs. It never walks
// complete history, clones a repository, or imports object contents.
func BenchmarkPrepareObjectHintsLinuxSample(b *testing.B) {
	if os.Getenv("GYIT_LINUX_METADATA_BENCH") != "1" {
		b.Skip("set GYIT_LINUX_METADATA_BENCH=1 with the cached Linux source")
	}
	source, err := filepath.Abs("../../.testdata/linux-repo.git")
	if err != nil {
		b.Fatal(err)
	}
	indexes, err := filepath.Glob(filepath.Join(source, "objects", "pack", "*.idx"))
	if err != nil || len(indexes) != 1 {
		b.Fatal("benchmark needs the existing single-pack SHA-1 Linux fixture")
	}
	index, err := os.Open(indexes[0])
	if err != nil {
		b.Fatal(err)
	}
	defer index.Close()
	var header [1032]byte
	if _, err := index.ReadAt(header[:], 0); err != nil {
		b.Fatal(err)
	}
	if binary.BigEndian.Uint32(header[:4]) != 0xff744f63 || binary.BigEndian.Uint32(header[4:8]) != 2 {
		b.Fatal("unsupported fixture pack index")
	}
	count := binary.BigEndian.Uint32(header[1028:])
	for _, order := range []string{"ordered", "permuted"} {
		for _, n := range []int{10000, 100000, 500000, 1000000} {
			b.Run(fmt.Sprintf("%s/%d", order, n), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					root := b.TempDir()
					ids, err := os.Create(filepath.Join(root, "objects"))
					if err != nil {
						b.Fatal(err)
					}
					w := bufio.NewWriter(ids)
					var oid [20]byte
					for j := 0; j < n; j++ {
						ordinal := uint64(j)
						if order == "permuted" {
							ordinal = ordinal * 8191 % uint64(n)
						}
						position := ordinal * uint64(count) / uint64(n)
						if _, err := index.ReadAt(oid[:], 1032+int64(position*20)); err != nil {
							b.Fatal(err)
						}
						if _, err := fmt.Fprintf(w, "%s sample/path-%04d\n", hex.EncodeToString(oid[:]), j%1024); err != nil {
							b.Fatal(err)
						}
					}
					if err := w.Flush(); err != nil {
						b.Fatal(err)
					}
					if _, err := ids.Seek(0, 0); err != nil {
						b.Fatal(err)
					}
					ctx, cancel := context.WithTimeout(b.Context(), 15*time.Second)
					b.StartTimer()
					started := time.Now()
					err = prepareObjectHints(ctx, ids, root, source, true, 4)
					b.StopTimer()
					if elapsed := time.Since(started); elapsed > time.Second {
						b.Logf("SLOW >1s: metadata preparation took %.3fs", elapsed.Seconds())
					}
					cancel()
					hash := sha256.New()
					if err == nil {
						if _, copyErr := io.Copy(hash, ids); copyErr != nil {
							b.Fatal(copyErr)
						}
						b.Logf("ordered IDs SHA256=%x", hash.Sum(nil))
					}
					ids.Close()
					if err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(n), "objects/op")
			})
		}
	}
}
