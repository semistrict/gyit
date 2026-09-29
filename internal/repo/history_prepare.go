//go:build !js

package repo

import (
	"context"
	"encoding/hex"
	"errors"
	"runtime"
	"sync"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
)

type preparedHistoryCommit struct {
	commit   *historyIngestCommit
	paths    *historyChanges
	complete bool
	err      error
}

func (r *preparedHistoryCommit) close() {
	if r.paths != nil {
		r.paths.close()
	}
}

type historyFuture struct {
	sha    string
	done   chan struct{}
	result preparedHistoryCommit
	commit *historyIngestCommit
	tree   string
}

// Up to seven workers look ahead while the caller consumes the original
// disk-backed frontier. At most sixteen futures remain pending; a miss
// computes synchronously rather than waiting for speculative capacity. Only the
// caller mutates staging or publishes coverage. Each result spills at 1 MiB.
type historyPreparation struct {
	p           *Progressive
	coverage    *index
	hasCoverage bool
	coverageErr error
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closed      bool
	pending     map[string]*historyFuture
	jobs        chan *historyFuture
	wg          sync.WaitGroup
}

func newHistoryPreparation(ctx context.Context, p *Progressive) *historyPreparation {
	ctx, cancel := context.WithCancel(ctx)
	// Previously covered records stay valid: historyBuild serializes ingestion
	// and its disk-backed seen set tracks newly processed commits.
	// Snapshot/pack publication may still advance HEAD, but must not make each
	// preparation reload a newer index just to discover the same old coverage.
	// Object lookups and final CAS publication continue using the latest root.
	workers := min(7, max(1, runtime.GOMAXPROCS(0)-1))
	h := &historyPreparation{p: p, coverage: p.index(), ctx: ctx, cancel: cancel, pending: make(map[string]*historyFuture), jobs: make(chan *historyFuture, 2*(workers+1))}
	// Coverage probes are sparse point lookups, even during a large import.
	h.coverage.pageRanges = true
	// A fresh immutable view has no earlier history to reuse. Prove that
	// once, rather than searching the same empty namespace for every commit.
	known, err := h.coverage.scan(ctx, "history/v2/", "", 1)
	h.hasCoverage, h.coverageErr = len(known) != 0, err
	for range workers {
		h.wg.Add(1)
		go func() {
			defer h.wg.Done()
			for f := range h.jobs {
				f.result = h.prepareCommit(f.sha, f.commit, f.tree)
				f.commit = nil
				close(f.done)
			}
		}()
	}
	return h
}

func (h *historyPreparation) offer(sha string) {
	h.offerCommit(sha, nil, "")
}

// Transfer already parsed parent metadata into the bounded look-ahead queue.
// The producer only needs its timestamp; the worker owns the record afterwards.
// The queue bound and commit parser limits also bound these retained records.
func (h *historyPreparation) offerCommit(sha string, commit *historyIngestCommit, tree string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.ctx.Err() != nil || len(h.pending) >= cap(h.jobs) || h.pending[sha] != nil {
		return
	}
	f := &historyFuture{sha: sha, done: make(chan struct{}), commit: commit, tree: tree}
	h.pending[sha] = f
	select {
	case h.jobs <- f:
	default:
		// Speculation must never block while holding the scheduler lock.
		delete(h.pending, sha)
	}
}

func (h *historyPreparation) take(sha string) *historyFuture {
	h.mu.Lock()
	defer h.mu.Unlock()
	f := h.pending[sha]
	delete(h.pending, sha)
	return f
}

func (h *historyPreparation) get(sha string) preparedHistoryCommit {
	if f := h.take(sha); f != nil {
		<-f.done
		return f.result
	}
	return h.prepare(sha)
}

func (h *historyPreparation) forget(sha string) {
	if f := h.take(sha); f != nil {
		<-f.done
		f.result.close()
	}
}

func (h *historyPreparation) close() {
	h.cancel()
	h.mu.Lock()
	h.closed = true
	close(h.jobs)
	h.mu.Unlock()
	h.wg.Wait()
	for _, f := range h.pending {
		f.result.close()
	}
}

func (h *historyPreparation) prepare(sha string) (r preparedHistoryCommit) {
	return h.prepareCommit(sha, nil, "")
}

func (h *historyPreparation) prepareCommit(sha string, commit *historyIngestCommit, tree string) (r preparedHistoryCommit) {
	ctx, p := h.ctx, h.p
	if r.err = ctx.Err(); r.err != nil {
		return
	}
	if h.coverageErr != nil {
		r.err = h.coverageErr
		return
	}
	if h.hasCoverage {
		state := &pb.HistoryIngestion{}
		err := h.coverage.get(ctx, historyIngestionKey+sha, state)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			r.err = err
			return
		}
		if state.Complete {
			r.complete = true
			return
		}
		var existing pb.HistoryBatchLocation
		err = h.coverage.get(ctx, historyBatchKey+sha, &existing)
		if err == nil {
			b, e := p.readHistoryBatch(ctx, &existing)
			if e != nil {
				r.err = e
				return
			}
			r.commit = &historyIngestCommit{HistoryBatchCommit: b.Commits[existing.Ordinal]}
			for _, oid := range r.commit.Parents {
				h.offer(hex.EncodeToString(oid))
			}
			return
		}
		if !errors.Is(err, store.ErrNotFound) {
			r.err = err
			return
		}
	}
	r.commit = commit
	if r.commit == nil {
		r.commit, tree, r.err = p.historyBatchCommit(ctx, sha)
	}
	if r.err != nil {
		return
	}
	parents := make([]string, 0, len(r.commit.Parents))
	for _, oid := range r.commit.Parents {
		c, t, e := p.historyBatchCommit(ctx, hex.EncodeToString(oid))
		if e != nil {
			r.err = e
			return
		}
		parents = append(parents, t)
		r.commit.ParentTimes = append(r.commit.ParentTimes, c.metadata.CommitTime)
		h.offerCommit(hex.EncodeToString(oid), c, t)
	}
	if len(parents) == 0 {
		parents = append(parents, "")
	}
	r.paths = newHistoryChanges(p.temp, len(parents))
	for i, parent := range parents {
		var include func(string) bool
		if i > 0 && r.paths.sorted == nil {
			// Default path history follows the first TREESAME parent. A path
			// unchanged from parent zero can never need another comparison.
			// First-parent differences already contain every ancestor directory,
			// so this also prunes unrelated subtrees before fetching them.
			// Large spilled changes retain the ordinary bounded traversal.
			include = func(path string) bool {
				mask := r.paths.paths[path]
				return len(mask) != 0 && mask[0]&1 != 0
			}
		}
		r.err = p.historyTreeDeltaPaths(ctx, tree, parent, include, func(path string) error { return r.paths.add(path, i) }, true)
		if r.err != nil {
			r.close()
			r.paths = nil
			return
		}
	}
	return
}
