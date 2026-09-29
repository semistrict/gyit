//go:build !js

package repo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
	"gyit/internal/spill"
)

// Compare the ordered staging required by a history frame, including inserts
// and traversal. Values model per-parent masks for four commits per path.
func BenchmarkHistoryPathStaging(b *testing.B) {
	for _, paths := range []int{256, 4096} {
		var keys [][]byte
		for i := range paths {
			name := fmt.Sprintf("src/module-%03d/file-%05d.go", i%100, i)
			for _, ordinal := range []uint32{0, 17, 31, 63} {
				key := make([]byte, len(name)+5)
				copy(key, name)
				binary.BigEndian.PutUint32(key[len(name)+1:], ordinal)
				keys = append(keys, key)
			}
		}
		for _, mode := range []string{"btree", "sort"} {
			b.Run(fmt.Sprintf("%d/%s", paths, mode), func(b *testing.B) {
				temp := b.TempDir()
				db, err := bolt.Open(filepath.Join(temp, "stage.db"), 0600, &bolt.Options{NoSync: true})
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					var add func([]byte, []byte) error
					var walk func(func([]byte, []byte) error) error
					var close func() error
					if mode == "btree" {
						tx, err := db.Begin(true)
						if err != nil {
							b.Fatal(err)
						}
						bucket, err := tx.CreateBucket([]byte("paths"))
						if err != nil {
							b.Fatal(err)
						}
						add, walk, close = bucket.Put, bucket.ForEach, tx.Rollback
					} else {
						s, err := spill.New(temp, historyChangeMemory)
						if err != nil {
							b.Fatal(err)
						}
						add, close = s.Add, s.Close
						walk = func(emit func([]byte, []byte) error) error { return s.Walk(b.Context(), emit) }
					}
					for _, key := range keys {
						if err := add(key, []byte{1}); err != nil {
							b.Fatal(err)
						}
					}
					count := 0
					var previous []byte
					err = walk(func(key, value []byte) error {
						if count > 0 && bytes.Compare(previous, key) >= 0 {
							return fmt.Errorf("out of order")
						}
						if !bytes.Equal(value, []byte{1}) {
							return fmt.Errorf("incorrect mask")
						}
						previous = append(previous[:0], key...)
						count++
						return nil
					})
					if err != nil || count != len(keys) {
						b.Fatalf("count=%d err=%v", count, err)
					}
					if err := close(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
