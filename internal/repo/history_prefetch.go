package repo

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
)

const historyReadAhead = 8

type historyPrefetchRequest struct {
	ref   pageRef
	index bool
}

func (r historyPrefetchRequest) key() string {
	if r.index {
		return fmt.Sprintf("index/%s/%d", r.ref.Pack, r.ref.Offset)
	}
	return r.ref.Pack
}

// Read-ahead only warms the existing globally bounded decoded cache. It never
// supplies traversal decisions: foreground readers validate every referenced
// frame normally. Six workers and eight queued containers bound extra work,
// including while emit is blocked behind the terminal pager.
type historyPrefetch struct {
	p       *Progressive
	ctx     context.Context
	cancel  context.CancelFunc
	queue   chan historyPrefetchRequest
	wg      sync.WaitGroup
	mu      sync.Mutex
	pending map[string]bool
	// Graph hints are admitted by workers and bounded across the whole query.
	graphPacks map[string]bool
	// Shared by the foreground walker and workers under mu. Bound both page
	// inspection and admitted requests over the lifetime of this query.
	indexPages    map[string]bool
	indexPacks    map[string]bool
	indexRequests map[string]bool
}

func newHistoryPrefetch(ctx context.Context, p *Progressive) *historyPrefetch {
	ctx = context.WithValue(ctx, cacheReadAheadKey{}, true)
	ctx, cancel := context.WithCancel(ctx)
	r := &historyPrefetch{p: p, ctx: ctx, cancel: cancel, queue: make(chan historyPrefetchRequest, historyReadAhead), pending: make(map[string]bool), graphPacks: make(map[string]bool), indexPages: make(map[string]bool), indexPacks: make(map[string]bool), indexRequests: make(map[string]bool)}
	for range cacheReadAheadLimit {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case request := <-r.queue:
					if ctx.Err() != nil {
						return
					}
					// Failure is speculative. The ordinary read retries if needed
					// and reports the error only if traversal actually needs it.
					if request.index {
						idx := p.index()
						idx.pageRanges = true
						raw, release, err := idx.borrowPage(ctx, request.ref)
						if err == nil {
							r.indexSiblings(request.ref, raw)
							r.indexGraphs(raw)
						}
						release()
					} else {
						_, _ = p.historyBytes(ctx, request.ref)
					}
					r.mu.Lock()
					delete(r.pending, request.key())
					r.mu.Unlock()
				}
			}
		}()
	}
	return r
}
func (r *historyPrefetch) close() { r.cancel(); r.wg.Wait() }

func (r *historyPrefetch) enqueue(ref pageRef, index bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.enqueueLocked(historyPrefetchRequest{ref: ref, index: index})
}
func (r *historyPrefetch) enqueueLocked(request historyPrefetchRequest) bool {
	key := request.key()
	if r.pending[key] {
		return true
	}
	select {
	case r.queue <- request:
		r.pending[key] = true
		return true
	default:
		return false
	}
}

// SHA order scatters consecutive ancestry commits across index containers.
// Follow only history-intersecting branches, with per-query caps of sixteen
// page requests and eight containers. Cached siblings may expose graph locations
// before the foreground reaches them; all loaders share the six-request budget.
func (r *historyPrefetch) indexSiblings(ref pageRef, raw []byte) {
	r.mu.Lock()
	if r.ctx.Err() != nil || len(r.indexPages) >= 2*historyReadAhead || r.indexPages[ref.Hash] {
		r.mu.Unlock()
		return
	}
	r.indexPages[ref.Hash] = true
	r.mu.Unlock()
	var page pb.IndexPage
	if proto.Unmarshal(raw, &page) != nil || len(page.Children) > fanout {
		return
	}
	// A container holds many pages. Admit independent containers first;
	// otherwise aliases can consume every slot waiting for the same GET.
	// A second pass still explores cached branches and graph hints within
	// those containers, without increasing either lifetime budget.
	r.mu.Lock()
	defer r.mu.Unlock()
	for pass := range 2 {
		previous := ""
		for _, child := range page.Children {
			if previous >= historyBatchKey+"\xff" {
				break
			}
			previous = child.MaxKey
			if child.MaxKey < historyBatchKey || child.Page == nil {
				continue
			}
			ref := decodePageRef(child.Page)
			request := historyPrefetchRequest{ref: ref, index: true}
			key := request.key()
			if len(r.indexRequests) >= 2*historyReadAhead {
				return
			}
			if (pass == 0 && r.indexPacks[ref.Pack]) || r.indexRequests[key] || r.indexPages[ref.Hash] || (!r.indexPacks[ref.Pack] && len(r.indexPacks) >= historyReadAhead) {
				continue
			}
			if r.enqueueLocked(request) {
				r.indexRequests[key] = true
				r.indexPacks[ref.Pack] = true
			}
		}
	}
}

// The already-fetched graph container often covers thousands of commits.
// Inspect its following frame headers for matching path data, without another
// Store request and without decoding/copying their commit arrays.
func (r *historyPrefetch) after(ref *pb.PageReference, path string) {
	if ref == nil || r.ctx.Err() != nil {
		return
	}
	raw, release, err := r.p.cache.borrowCached(r.ctx, "history-container/"+ref.Pack)
	defer release()
	if err != nil {
		return
	}
	seen := make(map[string]bool, historyReadAhead)
	foreground := ""
	_ = visitHistoryFrames(raw, func(offset, length int64, hash, payload []byte) (bool, error) {
		if offset < ref.Offset {
			return true, nil
		}
		hints := historyBatchHints(payload, path)
		if offset == ref.Offset && hints[0] != nil {
			foreground = hints[0].Pack
		}
		for _, hint := range hints {
			// Reserve speculative workers for independent containers. Display
			// or later path frames may share the foreground path's container;
			// enqueuing any such alias wastes a worker on its existing load.
			if hint != nil && hint.Pack == foreground {
				continue
			}
			if hint != nil && !seen[hint.Pack] {
				seen[hint.Pack] = true
				r.enqueue(decodePageRef(hint), false)
			}
			if len(seen) == historyReadAhead {
				return false, nil
			}
		}
		return len(seen) < historyReadAhead, nil
	})
}

// These are hints from neighboring frames, not trusted index references. Bad
// hints are ignored. historyBytes restricts their namespace/range and verifies
// their checksums; foreground references remain the authority for all results.
func historyBatchHints(raw []byte, path string) (hints [2]*pb.PageReference) {
	var version uint64
	var references [2][]byte
	var filter []byte
	for len(raw) > 0 {
		num, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			return hints
		}
		raw = raw[n:]
		if num == 1 && typ == protowire.VarintType {
			version, n = protowire.ConsumeVarint(raw)
		} else if (num == 3 || num == 4 || num == 5) && typ == protowire.BytesType {
			var value []byte
			value, n = protowire.ConsumeBytes(raw)
			if num == 5 {
				filter = value
			} else {
				references[num-3] = value
			}
		} else {
			n = protowire.ConsumeFieldValue(num, typ, raw)
		}
		if n < 0 {
			return hints
		}
		raw = raw[n:]
	}
	if !knownHistoryBatchVersion(version) || len(filter) != historyFilterBytes || !historyFilterMatch(filter, path) || len(references[0]) == 0 {
		return hints
	}
	for i, raw := range references {
		if len(raw) == 0 {
			continue
		}
		var ref pb.PageReference
		if proto.Unmarshal(raw, &ref) == nil {
			hints[i] = &ref
		}
	}
	return hints
}

// A fetched history leaf exposes graph containers independently of which SHA
// the foreground currently needs. Warm at most eight, without recursively
// traversing index pages or using hints as proof of coverage or traversal order.
func (r *historyPrefetch) indexGraphs(raw []byte) {
	for count := 0; len(raw) > 0 && count < fanout; count++ {
		field, typ, n := protowire.ConsumeTag(raw)
		if n < 0 || field != 1 || typ != protowire.BytesType {
			return
		}
		raw = raw[n:]
		record, n := protowire.ConsumeBytes(raw)
		if n < 0 {
			return
		}
		raw = raw[n:]
		var item pb.IndexItem
		if proto.Unmarshal(record, &item) != nil {
			return
		}
		if !strings.HasPrefix(item.Key, historyBatchKey) {
			continue
		}
		var location pb.HistoryBatchLocation
		if proto.Unmarshal(item.Value, &location) != nil || location.Batch == nil {
			continue
		}
		ref := decodePageRef(location.Batch)
		if !strings.HasPrefix(ref.Pack, "index/progressive-history-v2-graph-") {
			continue
		}
		r.mu.Lock()
		if len(r.graphPacks) >= historyReadAhead {
			r.mu.Unlock()
			return
		}
		if r.graphPacks[ref.Pack] {
			r.mu.Unlock()
			continue
		}
		r.graphPacks[ref.Pack] = true
		r.mu.Unlock()
		if !r.enqueue(ref, false) {
			r.mu.Lock()
			delete(r.graphPacks, ref.Pack)
			r.mu.Unlock()
		}
	}
}
