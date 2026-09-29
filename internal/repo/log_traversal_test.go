//go:build !js

package repo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"testing"

	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
)

func TestLogTraversalSpillOrderMembershipAndCleanup(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	walk := newLogTraversal(ctx, dir)
	defer walk.close()
	var want []logCandidate
	for i := 0; i < 120010; i++ {
		// Include negative timestamps, both integer extremes, and many date ties.
		stamp := int64(i%31 - 15)
		if i == 0 {
			stamp = math.MinInt64
		}
		if i == 1 {
			stamp = math.MaxInt64
		}
		c := logCandidate{sha: fmt.Sprintf("%040x", i+1), path: "file", time: stamp, order: i}
		if err := walk.push(c); err != nil {
			t.Fatal(err)
		}
		want = append(want, c)
		if err := walk.push(c); err != nil {
			t.Fatal(err)
		} // Duplicate queue insertion is suppressed.
	}
	if walk.disk == nil || len(walk.seen) != 0 || len(walk.queue) != 0 {
		t.Fatal("large traversal retained its queue or membership in Go memory")
	}
	sort.Slice(want, func(i, j int) bool {
		if want[i].time == want[j].time {
			return want[i].order < want[j].order
		}
		return want[i].time > want[j].time
	})
	for _, expected := range want {
		got, ok, err := walk.pop()
		if err != nil || !ok || got != expected {
			t.Fatalf("queue order: got %v %t %v want %v", got, ok, err, expected)
		}
	}
	if _, ok, err := walk.pop(); err != nil || ok {
		t.Fatalf("queue did not end: %t %v", ok, err)
	}
	// Popping does not forget ancestry and therefore cannot duplicate a shared parent.
	if err := walk.push(want[0]); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := walk.pop(); err != nil || ok {
		t.Fatalf("forgot emitted ancestor: %t %v", ok, err)
	}
	renamed := want[0]
	renamed.path = "other/\xff"
	if err := walk.push(renamed); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := walk.pop(); err != nil || !ok || got.sha != renamed.sha || got.path != renamed.path {
		t.Fatalf("different follow path was suppressed or damaged: %v %t %v", got, ok, err)
	}
	if err := walk.clearSeen(); err != nil {
		t.Fatal(err)
	}
	if err := walk.push(want[0]); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, _, err := walk.pop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if err := walk.close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("scratch survived cancellation: %v %v", entries, err)
	}
}

func TestLogTraversalSpillFailurePreservesQueue(t *testing.T) {
	// No disk is touched until the memory window fills. A missing scratch
	// directory must surface an error rather than dropping pending candidates.
	walk := newLogTraversal(t.Context(), t.TempDir()+"/missing")
	var inserted int
	for ; inserted < 10000; inserted++ {
		err := walk.push(logCandidate{sha: fmt.Sprintf("%040x", inserted+1), time: int64(inserted)})
		if err != nil {
			break
		}
	}
	if inserted == 10000 {
		t.Fatal("unusable spill directory accepted")
	}
	for i := inserted - 1; i >= 0; i-- {
		got, ok, err := walk.pop()
		if err != nil || !ok || got.sha != fmt.Sprintf("%040x", i+1) {
			t.Fatalf("failed spill lost pending data: %v %t %v", got, ok, err)
		}
	}
	if err := walk.close(); err != nil {
		t.Fatal(err)
	}
}

func TestLogTraversalPreservesDeferredHistoryLocation(t *testing.T) {
	for _, spilled := range []bool{false, true} {
		t.Run(fmt.Sprint(spilled), func(t *testing.T) {
			walk := newLogTraversal(t.Context(), t.TempDir())
			defer walk.close()
			location := &pb.HistoryBatchLocation{Ordinal: 31, Batch: &pb.PageReference{Hash: "frame-hash", Pack: "index/graph", Offset: 123, Length: 456}}
			if err := walk.push(logCandidate{sha: fmt.Sprintf("%040x", 1), time: -1, location: location}); err != nil {
				t.Fatal(err)
			}
			if spilled {
				if err := walk.spill(); err != nil {
					t.Fatal(err)
				}
			}
			if err := walk.push(logCandidate{sha: fmt.Sprintf("%040x", 2), time: 100}); err != nil {
				t.Fatal(err)
			}
			if _, ok, err := walk.pop(); err != nil || !ok {
				t.Fatalf("newer candidate: %t %v", ok, err)
			}
			got, ok, err := walk.pop()
			if err != nil || !ok || !proto.Equal(got.location, location) {
				t.Fatalf("lost deferred location: %v %t %v", got.location, ok, err)
			}
		})
	}
}
