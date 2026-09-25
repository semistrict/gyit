package repo

import (
	"reflect"
	"testing"
)

func TestParallelBlobLanesRequiresSkewAndSupportedCodec(t *testing.T) {
	for _, tc := range []struct {
		name       string
		weights    []uint64
		depth, cap int
		want       []bool
	}{
		{"balanced", []uint64{100, 100, 100, 100}, 1, 4, []bool{false, false, false, false}},
		{"overloaded", []uint64{100, 500, 100, 100}, 1, 4, []bool{false, true, false, false}},
		{"boundary", []uint64{100, 300, 200, 200}, 1, 4, []bool{false, false, false, false}},
		{"no-estimate", nil, 1, 4, nil},
		{"empty", []uint64{0, 0, 0, 0}, 1, 4, nil},
		{"deeper-deltas", []uint64{100, 500, 100, 100}, 2, 4, nil},
		{"other-candidates", []uint64{100, 500, 100, 100}, 1, 8, nil},
		{"overflow", []uint64{^uint64(0), 500, 100, 100}, 1, 4, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := parallelBlobLanes(tc.weights, 4, tc.depth, tc.cap); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("enabled=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestBlobWorkEstimateAccountsForChunkRouting(t *testing.T) {
	sizes := &blobSizes{laneBytes: make([]uint64, 4)}
	hint := "large-file"
	first := chunkWorker(hint+"/0000000000000000", 4)
	second := chunkWorker(hint+"/0000000000000001", 4)
	sizes.noteBlobWork(hint, ChunkSize+127, first)
	want := make([]uint64, 4)
	want[first] += ChunkSize
	want[second] += 127
	if !reflect.DeepEqual(sizes.laneBytes, want) {
		t.Fatalf("chunk routing weights=%v, want %v", sizes.laneBytes, want)
	}
	// Unhinted work must not be falsely pinned to the hash of an empty path.
	sizes.noteBlobWork("", 403, 0)
	for i := range want {
		want[i] += 100
		if i < 3 {
			want[i]++
		}
	}
	if !reflect.DeepEqual(sizes.laneBytes, want) {
		t.Fatalf("unhinted weights=%v, want %v", sizes.laneBytes, want)
	}
	sizes.laneBytes[first] = ^uint64(0)
	sizes.noteBlobWork(hint, 1, first)
	if sizes.laneBytes != nil {
		t.Fatal("overflowed scheduling estimate remained enabled")
	}
	sizes.noteBlobWork(hint, 1, first)
	if sizes.laneBytes != nil {
		t.Fatal("disabled estimate was reenabled")
	}
}
