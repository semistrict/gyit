package repo

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestCorrectnessViewsShareBoundedReaderState(t *testing.T) {
	f := archiveSnapshotFixture(t)
	globalPublishManifest(t, f.backend, f.manifests[0])
	initial, err := New(f.backend, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	current, err := initial.Open(t.Context(), f.commits[0])
	if err != nil {
		t.Fatal(err)
	}
	// This reader has not opened a snapshot yet: simultaneous first views must
	// reserve and retain exactly one table slot, shared with subsequent opens.
	r, err := New(f.backend, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errors := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			var out bytes.Buffer
			if e := r.View(t.Context(), current, ViewOptions{Command: "show", Args: []string{f.commits[0] + ":file"}}, &out); e != nil {
				errors <- e
			} else if !bytes.Equal(out.Bytes(), f.blobs[1]) {
				errors <- fmt.Errorf("view returned incorrect bytes")
			}
		})
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	r.globalMu.Lock()
	slot := r.globalSizes
	r.globalMu.Unlock()
	if slot == nil {
		t.Fatal("views did not bind the shared table slot")
	}
	for _, i := range []int{1, 0, 1} {
		globalPublishManifest(t, f.backend, f.manifests[i])
		var out bytes.Buffer
		if e := r.View(t.Context(), current, ViewOptions{Command: "show", Args: []string{f.commits[i] + ":file"}}, &out); e != nil || !bytes.Equal(out.Bytes(), f.blobs[i+1]) {
			t.Fatal("view after publication", e, out.String())
		}
		snap, e := r.Open(context.Background(), f.commits[i])
		if e != nil {
			t.Fatal(e)
		}
		if snap.idx.globalSizes != slot || snap.idx.cache != r.cache {
			t.Fatal("view and mounted reader allocated separate retained caches")
		}
		if r.globalSizes != slot || slot.charged.Load()+uint64(r.cache.used) > DefaultCacheBytes {
			t.Fatal("views exceeded the shared retained cache budget")
		}
	}
}
