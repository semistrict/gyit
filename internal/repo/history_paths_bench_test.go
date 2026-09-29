//go:build !js

package repo

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
)

func BenchmarkHistoryPathPages(b *testing.B) {
	db, err := bolt.Open(filepath.Join(b.TempDir(), "paths.db"), 0600, &bolt.Options{NoSync: true})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Rollback()
	bucket, err := tx.CreateBucket([]byte("paths"))
	if err != nil {
		b.Fatal(err)
	}
	const paths = 4096
	for i := range paths {
		name := fmt.Sprintf("src/module-%03d/file-%05d.go", i%100, i)
		for _, ordinal := range []uint32{0, 17, 31, 63} {
			key := make([]byte, len(name)+5)
			copy(key, name)
			binary.BigEndian.PutUint32(key[len(name)+1:], ordinal)
			if err := bucket.Put(key, []byte{1}); err != nil {
				b.Fatal(err)
			}
		}
	}
	filter := make([]byte, historyFilterBytes)
	var entries, encoded, pages int
	save := func(m proto.Message) (*pb.PageReference, error) {
		raw, err := proto.Marshal(m)
		entries += len(m.(*pb.HistoryPathPage).Entries)
		encoded += len(raw)
		pages++
		return &pb.PageReference{Pack: "index/benchmark", Offset: int64(encoded), Length: int64(len(raw)), Hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}, err
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		clear(filter)
		entries, encoded, pages = 0, 0, 0
		root, err := writeHistoryPaths(bucket.ForEach, filter, save)
		if err != nil || root == nil || entries != paths {
			b.Fatalf("paths=%d root=%v err=%v", entries, root, err)
		}
	}
	b.ReportMetric(float64(encoded), "encoded-bytes/op")
	b.ReportMetric(float64(pages), "pages/op")
}
