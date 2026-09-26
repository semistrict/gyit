package repo

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gyit/internal/store"
)

type latencyStore struct {
	store.Store
	delay time.Duration
}

func (s latencyStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if s.delay > 0 {
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-time.After(s.delay):
		}
	}
	return s.Store.Get(ctx, key, off, n)
}

func measureMediumReads(t *testing.T, ctx context.Context, backend store.Store, snapshot *Snapshot, paths []string, files map[string]Entry) {
	t.Helper()
	for _, delay := range []time.Duration{0, 5 * time.Millisecond} {
		var cold, warm []time.Duration
		gets, packGets, transferred, deltas := 0, 0, 0, 0
		var depths [MaxDeltaDepth + 1]int
		for i := 0; i < len(paths); i += max(1, len(paths)/64) {
			e, err := snapshot.Resolve(ctx, paths[i])
			if err != nil {
				t.Fatal(err)
			}
			if e.Mode == 0160000 || e.Size == 0 {
				continue
			}
			measured := &countedStore{Store: latencyStore{Store: backend, delay: delay}}
			s := &Snapshot{idx: &index{store: measured, cache: newCache(8 << 20), root: snapshot.idx.root}, SHA: snapshot.SHA, Tree: snapshot.Tree}
			off := e.Size / 2
			data := make([]byte, min(int64(4096), e.Size-off))
			start := time.Now()
			n, err := s.ReadAt(ctx, e.OID, data, off)
			cold = append(cold, time.Since(start))
			if err != nil || n != len(data) {
				t.Fatalf("cold partial read: %d %v", n, err)
			}
			gets += measured.gets
			packGets += measured.packGets
			transferred += measured.bytes
			measured.reset()
			again := make([]byte, len(data))
			start = time.Now()
			n, err = s.ReadAt(ctx, e.OID, again, off)
			warm = append(warm, time.Since(start))
			if err != nil || n != len(again) || !bytes.Equal(data, again) || measured.gets != 0 {
				t.Fatalf("warm read used I/O or returned wrong bytes: %d %v", measured.gets, err)
			}
			var c chunk
			if err := s.idx.get(ctx, chunkKey(e.OID, off/ChunkSize), &c); err != nil {
				t.Fatal(err)
			}
			d, err := chunkLocation(c).depth()
			if err != nil {
				t.Fatal(err)
			}
			depths[d]++
			if c.Base != nil {
				deltas++
			}
		}
		slices.Sort(cold)
		slices.Sort(warm)
		t.Logf("4KiB reads delay=%s samples=%d delta_samples=%d cold_p50=%s cold_p95=%s warm_p50=%s GETs=%d payload_GETs=%d fetched_bytes=%d", delay, len(cold), deltas, cold[len(cold)/2], cold[(len(cold)-1)*95/100], warm[len(warm)/2], gets, packGets, transferred)
		t.Logf("sampled delta depths=%v", depths)
	}
	// Warm metadata version opening is independent of content encoding.
	r := &Repository{store: backend, cache: snapshot.idx.cache}
	var timings []time.Duration
	for i := 0; i < 100; i++ {
		start := time.Now()
		if _, err := r.Open(ctx, snapshot.SHA); err != nil {
			t.Fatal(err)
		}
		timings = append(timings, time.Since(start))
	}
	slices.Sort(timings)
	t.Logf("warm version open p50=%s p95=%s", timings[50], timings[95])
}

func measureStoreSize(t *testing.T, dir string) {
	t.Helper()
	sizes := map[string]int64{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.HasSuffix(path, ".lock") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		prefix, _, _ := strings.Cut(rel, string(filepath.Separator))
		sizes[prefix] += info.Size()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, n := range sizes {
		total += n
	}
	t.Log(fmt.Sprintf("stored bytes: total=%d data=%d index=%d manifests=%d", total, sizes["packs"], sizes["index"], sizes["HEAD"]+sizes["generations"]))
}
