//go:build !js

package repo

import (
	"bytes"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
)

// A history location occupies a small index page, even when the immutable
// container also holds megabytes of unrelated metadata. Random ancestry reads
// must not repeatedly evict their useful pages by admitting those containers.
func TestHistoryReadsOnlyRequestedIndexPages(t *testing.T) {
	backend, ids, _, _ := historyPrefetchFixture(t, 3, false)
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var items []item
	for _, id := range ids {
		var loc pb.HistoryBatchLocation
		if err := p.get(t.Context(), historyBatchKey+id, &loc); err != nil {
			t.Fatal(err)
		}
		raw, err := proto.Marshal(&loc)
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, item{Key: historyBatchKey + id, Value: raw})
	}
	state, err := proto.Marshal(&pb.HistoryIngestion{Complete: true, CoveredCommits: 3})
	if err != nil {
		t.Fatal(err)
	}
	items = append(items, item{Key: historyIngestionKey + ids[0], Value: state})
	writer := &indexWriter{ctx: t.Context(), store: backend, prefix: "history-range-test"}
	if _, err := writer.saveBytes(bytes.Repeat([]byte{42}, 2<<20)); err != nil {
		t.Fatal(err)
	}
	root, err := writer.save(page{Items: items})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.flush(); err != nil {
		t.Fatal(err)
	}
	manifest, err := proto.Marshal(&pb.ProgressiveManifest{Version: 1, Index: encodePageRef(root.ID)})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Put(t.Context(), "HEAD", manifest, p.token); err != nil {
		t.Fatal(err)
	}
	measured := &historyObjectTraceStore{Store: backend}
	reader := directoryReader(t, measured, t.TempDir(), 32<<20)
	progress, err := reader.HistoryProgress(t.Context(), ids[0])
	if err != nil || !progress.Complete {
		t.Fatalf("progress: %v %v", progress, err)
	}
	snapshot := &Snapshot{SHA: ids[0], progressive: reader, idx: reader.index()}
	var got []string
	if err := snapshot.LogWithOptions(t.Context(), LogOptions{Count: 3, Paths: []string{"file"}, FullCommitIDs: true}, func(e LogEntry) error { got = append(got, e.SHA); return nil }); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != strings.Join(ids, ",") {
		t.Fatalf("history %v, want %v", got, ids)
	}
	if n := measured.kinds[2].bytes.Load(); n > 64<<10 {
		t.Fatalf("history metadata fetched %d index bytes, want only its small page", n)
	}
	// Preparation owns an immutable coverage index too. It must not bypass the
	// point-lookup policy by reading that index directly.
	preparationReads := &historyObjectTraceStore{Store: backend}
	preparer := directoryReader(t, preparationReads, t.TempDir(), 32<<20)
	h := newHistoryPreparation(t.Context(), preparer)
	defer h.close()
	r := h.get(ids[0])
	defer r.close()
	if r.err != nil || !r.complete {
		t.Fatalf("preparation coverage: %+v", r)
	}
	if n := preparationReads.kinds[2].bytes.Load(); n > 64<<10 {
		t.Fatalf("history preparation fetched %d index bytes, want only its small page", n)
	}
}
