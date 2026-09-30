package githubfs

import (
	"context"
	"fmt"
	"time"
)

// An early transfer belongs to the filesystem lifetime, not a reader or pager.
// Its result is reusable by the background importer, but it does not publish
// objects or coverage. Keep at most one speculative transfer per repository.
type ancestryAcquisition struct {
	sha  string
	done chan struct{}
	err  error // Read only after done closes.
}

func (f *FS) startEarlyAncestry(p *progressiveRepository, sha string) {
	p.earlyMu.Lock()
	defer p.earlyMu.Unlock()
	if previous := p.earlyAncestry; previous != nil {
		select {
		case <-previous.done:
			if previous.sha == sha && previous.err == nil {
				return
			}
		default:
			return
		}
	}
	// Never queue extra workers behind an existing fetch. Normal ingestion
	// handles a different selected revision once its acquisition lane is free.
	select {
	case p.ancestryOperations <- struct{}{}:
	default:
		return
	}
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		p.ancestryOperations.release()
		return
	}
	f.wg.Add(1)
	f.mu.Unlock()
	a := &ancestryAcquisition{sha: sha, done: make(chan struct{})}
	p.earlyAncestry = a
	go func() {
		defer f.wg.Done()
		defer close(a.done)
		defer p.ancestryOperations.release()
		a.err = f.fetchHistory(f.ctx, p.ancestrySource, sha, 0)
	}()
}

func (p *progressiveRepository) takeEarlyAncestry(sha string) *ancestryAcquisition {
	p.earlyMu.Lock()
	defer p.earlyMu.Unlock()
	if p.earlyAncestry == nil || p.earlyAncestry.sha != sha {
		return nil
	}
	a := p.earlyAncestry
	p.earlyAncestry = nil
	return a
}

// Prioritize a missing commit without deepening every existing shallow boundary.
// The result includes trees for every path, is published to the shared store,
// and remains available after this reader closes its pager. Bulk ingestion
// continues independently and can index the same objects later.
func (f *FS) acquireHistoryWindow(ctx context.Context, p *progressiveRepository, sha string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !fullSHA(sha) {
		return fmt.Errorf("invalid history window commit")
	}
	result := p.historyDemands.DoChan(sha, func() (any, error) {
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			return nil, context.Canceled
		}
		f.wg.Add(1)
		f.mu.Unlock()
		defer f.wg.Done()
		acquire, cancel := context.WithTimeout(f.ctx, 30*time.Second)
		defer cancel()
		if err := p.historyOperations.acquire(acquire); err != nil {
			return nil, err
		}
		defer p.historyOperations.release()
		// Explicit wants must include promised trees even if a commit-only request
		// previously acquired the commit. This is not a query-specific path index.
		err := f.runHistoryFetch(acquire, p.historySource, "-c", "fetch.negotiationAlgorithm=noop", "fetch", "--quiet", "--keep", "--no-tags", "--no-auto-maintenance", "--no-write-fetch-head", "--filter=blob:none", "--depth=64", "origin", sha)
		if err != nil {
			return nil, err
		}
		return nil, p.reader.ImportPacks(acquire, p.historySource)
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case result := <-result:
		return result.Err
	}
}
