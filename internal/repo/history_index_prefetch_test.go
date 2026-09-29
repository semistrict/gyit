package repo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
)

// Separate leaves represent the random SHA ranges that a history walk visits.
// Both are known from the root before either leaf has finished downloading.
func TestFileLogOverlapsHistoryIndexReads(t *testing.T) {
	backend, ids, _, _ := historyPrefetchFixture(t, 2, false)
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w := &indexWriter{ctx: t.Context(), store: backend, prefix: "history-index-prefetch-test"}
	var children []edge
	for _, id := range ids {
		var location pb.HistoryBatchLocation
		if err := p.get(t.Context(), historyBatchKey+id, &location); err != nil {
			t.Fatal(err)
		}
		raw, err := proto.Marshal(&location)
		if err != nil {
			t.Fatal(err)
		}
		e, err := w.save(page{Items: []item{{Key: historyBatchKey + id, Value: raw}}})
		if err != nil {
			t.Fatal(err)
		}
		children = append(children, e)
		if err := w.flush(); err != nil {
			t.Fatal(err)
		}
	}
	root, err := w.save(page{Children: children})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	manifest, err := proto.Marshal(&pb.ProgressiveManifest{Version: 1, Index: encodePageRef(root.ID)})
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Put(t.Context(), "HEAD", manifest, p.token); err != nil {
		t.Fatal(err)
	}
	gate := &historyPrefetchGate{Store: backend, blocked: map[string]bool{children[0].ID.Pack: true, children[1].ID.Pack: true}, started: make(chan string, 4), release: make(chan struct{})}
	reader := directoryReader(t, gate, t.TempDir(), 32<<20)
	s := &Snapshot{SHA: ids[0], progressive: reader, idx: reader.index()}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	var got []string
	go func() {
		done <- s.LogWithOptions(ctx, LogOptions{Count: 2, FullCommitIDs: true, Paths: []string{"file"}}, func(e LogEntry) error { got = append(got, e.SHA); return nil })
	}()
	defer cancel()
	defer close(gate.release)
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case key := <-gate.started:
			seen[key] = true
		case err := <-done:
			t.Fatalf("query finished before index release: %v", err)
		case <-time.After(time.Second):
			cancel()
			<-done
			t.Fatal("next history index container waited for the first")
		}
	}
	gate.release <- struct{}{}
	gate.release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != fmt.Sprint(ids) {
		t.Fatalf("history order %v, want %v", got, ids)
	}
}

func TestHistoryIndexReadAheadBoundsAndCancellation(t *testing.T) {
	backend, _, _, _ := historyPrefetchFixture(t, 1, false)
	w := &indexWriter{ctx: t.Context(), store: backend, prefix: "history-index-budget-test"}
	var children []edge
	blocked := map[string]bool{}
	for i := range 24 {
		e, err := w.save(page{Items: []item{{Key: fmt.Sprintf("%s%040x", historyBatchKey, i), Value: []byte{1}}}})
		if err != nil {
			t.Fatal(err)
		}
		children = append(children, e)
		blocked[e.ID.Pack] = true
		if err := w.flush(); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := marshal(page{Children: children})
	if err != nil {
		t.Fatal(err)
	}
	gate := &historyPrefetchGate{Store: backend, blocked: blocked, started: make(chan string, 24), release: make(chan struct{})}
	p := directoryReader(t, gate, t.TempDir(), 32<<20)
	r := newHistoryPrefetch(t.Context(), p)
	defer r.close()
	for i := range 100 {
		// Changing the branch identity must not reset the per-query limit.
		r.indexSiblings(pageRef{Hash: fmt.Sprint(i)}, raw)
	}
	if len(r.indexPacks) != historyReadAhead {
		t.Fatalf("admitted %d index containers, want %d", len(r.indexPacks), historyReadAhead)
	}
	for range cacheReadAheadLimit {
		select {
		case <-gate.started:
		case <-time.After(time.Second):
			t.Fatal("index read-ahead did not start")
		}
	}
	select {
	case <-gate.started:
		t.Fatal("index read-ahead exceeded the shared six-request limit")
	case <-time.After(25 * time.Millisecond):
	}
	r.close()
	deadline := time.Now().Add(time.Second)
	for {
		gate.mu.Lock()
		active := gate.active
		gate.mu.Unlock()
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("speculative index reads survived query cancellation")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHistoryIndexReadAheadPrioritizesIndependentContainers(t *testing.T) {
	backend, _, _, _ := historyPrefetchFixture(t, 1, false)
	w := &indexWriter{ctx: t.Context(), store: backend, prefix: "history-index-independent"}
	var children []edge
	for i := range 2*historyReadAhead + 1 {
		if i == 2*historyReadAhead {
			if err := w.flush(); err != nil {
				t.Fatal(err)
			}
		}
		e, err := w.save(page{Items: []item{{Key: fmt.Sprintf("%s%040x", historyBatchKey, i), Value: []byte{1}}}})
		if err != nil {
			t.Fatal(err)
		}
		children = append(children, e)
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	raw, err := marshal(page{Children: children})
	if err != nil {
		t.Fatal(err)
	}
	first, last := children[0].ID.Pack, children[len(children)-1].ID.Pack
	gate := &historyPrefetchGate{Store: backend, blocked: map[string]bool{first: true, last: true}, started: make(chan string, 2), release: make(chan struct{})}
	p := directoryReader(t, gate, t.TempDir(), 32<<20)
	r := newHistoryPrefetch(t.Context(), p)
	defer r.close()
	r.indexSiblings(pageRef{Hash: "root"}, raw)
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case key := <-gate.started:
			seen[key] = true
		case <-time.After(time.Second):
			t.Fatal("aliases of one blocked container starved the independent container")
		}
	}
}

// A sibling leaf can reveal the shared graph before the selected SHA's leaf
// arrives. Traversal must still wait for that selected authoritative location.
func TestFileLogPrefetchesGraphFromAvailableSibling(t *testing.T) {
	for _, levels := range []int{1, 2} {
		t.Run(fmt.Sprint(levels), func(t *testing.T) { testFileLogGraphSibling(t, levels) })
	}
}
func testFileLogGraphSibling(t *testing.T, levels int) {
	backend, ids, _, _ := historyPrefetchFixture(t, 2, false)
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	writer := &indexWriter{ctx: t.Context(), store: backend, prefix: "graph-sibling-prefetch"}
	var children []edge
	graph := ""
	for _, id := range ids {
		var location pb.HistoryBatchLocation
		if err := p.get(t.Context(), historyBatchKey+id, &location); err != nil {
			t.Fatal(err)
		}
		graph = location.Batch.Pack
		raw, err := proto.Marshal(&location)
		if err != nil {
			t.Fatal(err)
		}
		child, err := writer.save(page{Items: []item{{Key: historyBatchKey + id, Value: raw}}})
		if err != nil {
			t.Fatal(err)
		}
		children = append(children, child)
		if err := writer.flush(); err != nil {
			t.Fatal(err)
		}
	}
	rootChildren := children
	if levels == 2 {
		rootChildren = nil
		for _, child := range children {
			branch, err := writer.save(page{Children: []edge{child}})
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.flush(); err != nil {
				t.Fatal(err)
			}
			rootChildren = append(rootChildren, branch)
		}
	}
	root, err := writer.save(page{Children: rootChildren})
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
	gate := &historyPrefetchGate{Store: backend, blocked: map[string]bool{children[0].ID.Pack: true, graph: true}, started: make(chan string, 4), release: make(chan struct{})}
	reader := directoryReader(t, gate, t.TempDir(), 32<<20)
	snapshot := &Snapshot{SHA: ids[0], progressive: reader, idx: reader.index()}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	done := make(chan error, 1)
	finished := make(chan struct{})
	emitted := make(chan string, 2)
	go func() {
		defer close(finished)
		done <- snapshot.indexedFileLog(ctx, "file", LogOptions{Count: 2, FullCommitIDs: true}, func(e LogEntry) error { emitted <- e.SHA; return nil })
	}()
	defer func() { cancel(); close(gate.release); <-finished }()
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case key := <-gate.started:
			seen[key] = true
		case <-ctx.Done():
			t.Fatal("graph waited for selected index leaf")
		}
	}
	select {
	case <-emitted:
		t.Fatal("sibling hint emitted an unproven result")
	default:
	}
	for range 2 {
		gate.release <- struct{}{}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if got := <-emitted; got != id {
			t.Fatalf("got %s want %s", got, id)
		}
	}
}

func TestHistoryGraphReadAheadBoundsAndCancellation(t *testing.T) {
	backend, _, _, _ := historyPrefetchFixture(t, 1, false)
	blocked := map[string]bool{}
	var items []item
	for i := range 24 {
		pack := fmt.Sprintf("index/progressive-history-v2-graph-budget/%08d", i)
		blocked[pack] = true
		value, err := proto.Marshal(&pb.HistoryBatchLocation{Batch: &pb.PageReference{Pack: pack, Length: 1, Hash: fmt.Sprintf("%064d", i)}})
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, item{Key: fmt.Sprintf("%s%040d", historyBatchKey, i), Value: value})
	}
	raw, err := marshal(page{Items: items})
	if err != nil {
		t.Fatal(err)
	}
	gate := &historyPrefetchGate{Store: backend, blocked: blocked, started: make(chan string, 24), release: make(chan struct{})}
	p := directoryReader(t, gate, t.TempDir(), 32<<20)
	prefetch := newHistoryPrefetch(t.Context(), p)
	defer prefetch.close()
	for range 100 {
		prefetch.indexGraphs(raw)
	}
	prefetch.mu.Lock()
	count := len(prefetch.graphPacks)
	prefetch.mu.Unlock()
	if count != historyReadAhead {
		t.Fatalf("admitted %d graph containers, want %d", count, historyReadAhead)
	}
	for range cacheReadAheadLimit {
		select {
		case <-gate.started:
		case <-time.After(time.Second):
			t.Fatal("graph reads did not start")
		}
	}
	select {
	case <-gate.started:
		t.Fatal("graph hints exceeded the shared request limit")
	case <-time.After(25 * time.Millisecond):
	}
	prefetch.close()
	deadline := time.Now().Add(time.Second)
	for {
		gate.mu.Lock()
		active := gate.active
		gate.mu.Unlock()
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("graph reads survived cancellation")
		}
		time.Sleep(time.Millisecond)
	}
}
