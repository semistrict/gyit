//go:build !js

package repo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/spill"
)

func TestHistorySortedPathStagingSpills(t *testing.T) {
	temp := t.TempDir()
	records, err := spill.New(temp, historyChangeMemory)
	if err != nil {
		t.Fatal(err)
	}
	defer records.Close()
	const paths = 4096
	name := func(i int) []byte { return []byte(fmt.Sprintf("dir/%05d-%s-\xff", i, strings.Repeat("long-name", 12))) }
	ordinals := []uint32{0, 17, 31, 63}
	// Reverse order and reusable input bytes exercise sorting and ownership.
	// The aggregate data exceeds the production run budget, forcing disk runs.
	for i := paths - 1; i >= 0; i-- {
		path := name(i)
		for _, ordinal := range ordinals {
			key := append(append(bytes.Clone(path), 0), 0, 0, 0, 0)
			binary.BigEndian.PutUint32(key[len(key)-4:], ordinal)
			mask := []byte{byte(ordinal + 1), 2}
			if err := records.Add(key, mask); err != nil {
				t.Fatal(err)
			}
			clear(key)
			clear(mask)
		}
	}
	filter := make([]byte, historyFilterBytes)
	pages := map[string]*pb.HistoryPathPage{}
	root, err := writeHistoryPaths(func(emit func([]byte, []byte) error) error { return records.Walk(t.Context(), emit) }, filter, func(m proto.Message) (*pb.PageReference, error) {
		key := fmt.Sprint(len(pages))
		pages[key] = proto.Clone(m).(*pb.HistoryPathPage)
		return &pb.PageReference{Pack: key}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	next := 0
	var visit func(*pb.PageReference)
	visit = func(ref *pb.PageReference) {
		t.Helper()
		if ref == nil || pages[ref.Pack] == nil {
			t.Fatal("missing path page")
		}
		page := pages[ref.Pack]
		if len(page.Children) > 32 {
			t.Fatal("unbounded child references")
		}
		for _, child := range page.Children {
			visit(child.Page)
		}
		for _, entry := range page.Entries {
			if !bytes.Equal(entry.Path, name(next)) || len(entry.Ordinals) != len(ordinals) || len(entry.DifferentParents) != len(ordinals) {
				t.Fatalf("incorrect posting %d: %v", next, entry)
			}
			if !historyFilterMatch(filter, string(entry.Path)) {
				t.Fatal("false-negative path filter")
			}
			for j, ordinal := range ordinals {
				if entry.Ordinals[j] != ordinal || !bytes.Equal(entry.DifferentParents[j], []byte{byte(ordinal + 1), 2}) {
					t.Fatal("lost commit or parent mask")
				}
			}
			next++
		}
	}
	visit(root)
	if next != paths {
		t.Fatalf("got %d paths", next)
	}
	if err := records.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(temp)
	if err != nil || len(entries) != 0 {
		t.Fatalf("staging files remain: %v %v", entries, err)
	}
}
