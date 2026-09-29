package repo

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
)

// Each batch has its own data container but shares a graph container. This
// isolates the request dependency without network timing or a large fixture.
func historyPrefetchFixture(t testing.TB, count int, separateDisplay bool, unchangedTip ...bool) (store.Store, []string, []string, []string) {
	t.Helper()
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	enc, err := newCompressor()
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	graph := &indexWriter{ctx: t.Context(), store: backend, prefix: "progressive-history-v2-graph-prefetch", packLimit: progressiveContainerBytes}
	ids, keys := make([]string, count), make([]string, count)
	displays := make([]string, count)
	refs := make([]pageRef, count)
	for i := range ids {
		ids[i] = fmt.Sprintf("%040x", i+1)
	}
	save := func(w *indexWriter, m proto.Message) *pb.PageReference {
		raw, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		ref, err := w.saveBytes(enc.EncodeAll(raw, nil))
		if err != nil {
			t.Fatal(err)
		}
		return encodePageRef(ref)
	}
	for i := range ids {
		w := &indexWriter{ctx: t.Context(), store: backend, prefix: fmt.Sprintf("progressive-history-v2-prefetch-%d", i), packLimit: progressiveContainerBytes}
		changed := i != 0 || len(unchangedTip) == 0 || !unchangedTip[0]
		pathPage := &pb.HistoryPathPage{}
		if changed {
			pathPage.Entries = []*pb.HistoryPathPosting{{Path: []byte("file"), Ordinals: []uint32{0}, DifferentParents: [][]byte{{1}}}}
		}
		paths := save(w, pathPage)
		if separateDisplay {
			if err := w.flush(); err != nil {
				t.Fatal(err)
			}
		}
		display := save(w, &pb.HistoryDisplay{Commits: []*pb.CommitRecord{{Message: []byte(fmt.Sprint(i))}}})
		displays[i] = display.Pack
		if err := w.flush(); err != nil {
			t.Fatal(err)
		}
		keys[i] = paths.Pack
		oid, _ := hex.DecodeString(ids[i])
		c := &pb.HistoryBatchCommit{Oid: oid}
		if i+1 < count {
			parent, _ := hex.DecodeString(ids[i+1])
			c.Parents, c.ParentTimes = [][]byte{parent}, []int64{int64(count - i - 1)}
		}
		filter := make([]byte, historyFilterBytes)
		if changed {
			historyFilterAdd(filter, "file")
		}
		refs[i] = decodePageRef(save(graph, &pb.HistoryBatch{Version: historyBatchVersion, Commits: []*pb.HistoryBatchCommit{c}, Paths: paths, Display: display, PathFilter: filter}))
	}
	if err := graph.flush(); err != nil {
		t.Fatal(err)
	}
	if err := p.stage(func(b *bolt.Bucket) error {
		for i, id := range ids {
			if err := progressivePut(b, historyBatchKey+id, &pb.HistoryBatchLocation{Batch: encodePageRef(refs[i])}); err != nil {
				return err
			}
		}
		if err := progressivePut(b, historyIngestionKey+ids[0], &pb.HistoryIngestion{Complete: true, CoveredCommits: uint64(count)}); err != nil {
			return err
		}
		return p.publish(t.Context(), b)
	}); err != nil {
		t.Fatal(err)
	}
	return backend, ids, keys, displays
}

type historyPrefetchGate struct {
	store.Store
	blocked      map[string]bool
	started      chan string
	release      chan struct{}
	mu           sync.Mutex
	active, peak int
}

func (s *historyPrefetchGate) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if s.blocked[key] {
		s.mu.Lock()
		s.active++
		s.peak = max(s.peak, s.active)
		s.mu.Unlock()
		defer func() { s.mu.Lock(); s.active--; s.mu.Unlock() }()
		select {
		case s.started <- key:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	return s.Store.Get(ctx, key, off, n)
}

func TestFileLogReservesReadAheadForIndependentContainers(t *testing.T) {
	backend, ids, keys, _ := historyPrefetchFixture(t, cacheReadAheadLimit+1, false)
	blocked := make(map[string]bool, len(keys))
	for _, key := range keys {
		blocked[key] = true
	}
	gate := &historyPrefetchGate{Store: backend, blocked: blocked, started: make(chan string, len(keys)), release: make(chan struct{})}
	p := directoryReader(t, gate, t.TempDir(), 32<<20)
	s := &Snapshot{SHA: ids[0], progressive: p, idx: p.index()}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	done := make(chan error, 1)
	finished := make(chan struct{})
	var got []string
	go func() {
		defer close(finished)
		done <- s.indexedFileLog(ctx, "file", LogOptions{Count: len(ids), FullCommitIDs: true}, func(e LogEntry) error {
			got = append(got, e.SHA)
			return nil
		})
	}()
	defer func() { cancel(); close(gate.release); <-finished }()
	seen := map[string]bool{}
	for len(seen) < len(keys) {
		select {
		case key := <-gate.started:
			seen[key] = true
		case err := <-done:
			t.Fatalf("history ended before independent reads started: %v (%d/%d)", err, len(seen), len(keys))
		case <-ctx.Done():
			t.Fatalf("only %d/%d independent containers started; foreground work occupied speculative capacity", len(seen), len(keys))
		}
	}
	for range keys {
		gate.release <- struct{}{}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != fmt.Sprint(ids) {
		t.Fatalf("history order %v, want %v", got, ids)
	}
}

func TestFileLogOverlapsIndependentHistoryDataReads(t *testing.T) {
	for _, display := range []bool{false, true} {
		t.Run(fmt.Sprintf("separate-display=%v", display), func(t *testing.T) {
			backend, ids, keys, displays := historyPrefetchFixture(t, 2, display)
			second := keys[1]
			if display {
				second = displays[1]
			}
			gate := &historyPrefetchGate{Store: backend, blocked: map[string]bool{keys[0]: true, second: true}, started: make(chan string, 4), release: make(chan struct{})}
			p := directoryReader(t, gate, t.TempDir(), 32<<20)
			s := &Snapshot{SHA: ids[0], progressive: p, idx: p.index()}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			finished := make(chan struct{})
			defer func() { cancel(); <-finished }()
			var got []string
			go func() {
				defer close(finished)
				done <- s.indexedFileLog(ctx, "file", LogOptions{Count: 2, FullCommitIDs: true}, func(e LogEntry) error { got = append(got, e.SHA); return nil })
			}()
			defer close(gate.release)
			seen := map[string]bool{}
			for len(seen) < 2 {
				select {
				case key := <-gate.started:
					seen[key] = true
				case err := <-done:
					t.Fatalf("query finished before data was released: %v", err)
				case <-time.After(time.Second):
					t.Fatal("second independent data container waited for the first request")
				}
			}
			gate.release <- struct{}{}
			gate.release <- struct{}{}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(got) != fmt.Sprint(ids) {
				t.Fatalf("wrong history order: %v want %v", got, ids)
			}
		})
	}
}

func TestFileLogReadAheadDoesNotDelayFirstResultOrOutlivePager(t *testing.T) {
	backend, ids, keys, _ := historyPrefetchFixture(t, 20, false)
	blocked := make(map[string]bool)
	for _, key := range keys[1:] {
		blocked[key] = true
	}
	gate := &historyPrefetchGate{Store: backend, blocked: blocked, started: make(chan string, 32), release: make(chan struct{})}
	p := directoryReader(t, gate, t.TempDir(), 32<<20)
	s := &Snapshot{SHA: ids[0], progressive: p, idx: p.index()}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	emitted, quit := make(chan struct{}), make(chan struct{})
	pagerClosed := errors.New("pager closed")
	done := make(chan error, 1)
	finished := make(chan struct{})
	defer func() { cancel(); <-finished }()
	go func() {
		defer close(finished)
		done <- s.indexedFileLog(ctx, "file", LogOptions{Unlimited: true, FullCommitIDs: true}, func(e LogEntry) error {
			close(emitted)
			select {
			case <-quit:
				return pagerClosed
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-emitted:
	case <-time.After(time.Second):
		t.Fatal("first result waited for speculative reads")
	}
	for range cacheReadAheadLimit {
		select {
		case <-gate.started:
		case <-time.After(time.Second):
			t.Fatal("read ahead did not fill bounded request window")
		}
	}
	select {
	case <-gate.started:
		t.Fatal("read ahead exceeded six requests while pager was blocked")
	case <-time.After(25 * time.Millisecond):
	}
	close(quit)
	select {
	case err := <-done:
		if !errors.Is(err, pagerClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pager quit left read ahead blocked")
	}
	deadline := time.Now().Add(time.Second)
	for {
		gate.mu.Lock()
		active, peak := gate.active, gate.peak
		gate.mu.Unlock()
		if peak > cacheReadAheadLimit {
			t.Fatalf("read ahead had %d simultaneous requests", peak)
		}
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d requests survived pager quit", active)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestFileLogReadAheadBudgetSharedAcrossReaders(t *testing.T) {
	backend, ids, keys, _ := historyPrefetchFixture(t, 24, false)
	blocked := make(map[string]bool)
	for i, key := range keys {
		if i != 0 && i != 12 {
			blocked[key] = true
		}
	}
	gate := &historyPrefetchGate{Store: backend, blocked: blocked, started: make(chan string, 32), release: make(chan struct{})}
	p := directoryReader(t, gate, t.TempDir(), 32<<20)
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	emitted := make(chan struct{}, 2)
	start := func(sha string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := &Snapshot{SHA: sha, progressive: p, idx: p.index()}
			_ = s.indexedFileLog(ctx, "file", LogOptions{Unlimited: true, FullCommitIDs: true}, func(LogEntry) error { emitted <- struct{}{}; <-ctx.Done(); return ctx.Err() })
		}()
	}
	start(ids[0])
	for range cacheReadAheadLimit {
		select {
		case <-gate.started:
		case <-time.After(time.Second):
			t.Fatal("first reader did not start read ahead")
		}
	}
	start(ids[12])
	for range 2 {
		select {
		case <-emitted:
		case <-time.After(time.Second):
			t.Fatal("speculation starved foreground reader")
		}
	}
	select {
	case <-gate.started:
		t.Fatal("second reader exceeded the shared read-ahead budget")
	case <-time.After(25 * time.Millisecond):
	}
}

// A sparse path may have no foreground data request in the first frame. Its
// independent next five containers should fit in one bounded read-ahead wave.
func TestFileLogSparseReadAheadStartsFiveIndependentContainers(t *testing.T) {
	backend, ids, keys, _ := historyPrefetchFixture(t, 6, false, true)
	blocked := map[string]bool{}
	for _, key := range keys[1:] {
		blocked[key] = true
	}
	gate := &historyPrefetchGate{Store: backend, blocked: blocked, started: make(chan string, 6), release: make(chan struct{})}
	p := directoryReader(t, gate, t.TempDir(), 32<<20)
	s := &Snapshot{SHA: ids[0], progressive: p, idx: p.index()}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	done := make(chan error, 1)
	finished := make(chan struct{})
	var got []string
	go func() {
		defer close(finished)
		done <- s.indexedFileLog(ctx, "file", LogOptions{Count: 5, FullCommitIDs: true}, func(e LogEntry) error { got = append(got, e.SHA); return nil })
	}()
	defer func() { cancel(); close(gate.release); <-finished }()
	seen := map[string]bool{}
	for len(seen) < 5 {
		select {
		case key := <-gate.started:
			seen[key] = true
		case err := <-done:
			t.Fatalf("stopped before five independent reads: %v", err)
		case <-ctx.Done():
			t.Fatalf("only %d/5 independent reads started", len(seen))
		}
	}
	for range 5 {
		gate.release <- struct{}{}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != fmt.Sprint(ids[1:]) {
		t.Fatalf("got %v want %v", got, ids[1:])
	}
}
