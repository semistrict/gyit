package repo

import (
	"bytes"
	"context"
	"sync"
)

// Import-only work sharing. Results commit in original worker order.
// A changed candidate set invalidates speculative work and forces recomputation.
type speculativeJob struct {
	slot    encodeSlot
	choices []*deltaBase
	done    chan struct{}
}
type speculativePool struct {
	jobs chan *speculativeJob
	wg   sync.WaitGroup
}

func newSpeculativePool(n int) (*speculativePool, error) {
	p := &speculativePool{jobs: make(chan *speculativeJob, 128)}
	for i := 0; i < n; i++ {
		encoder, e := newCompressor()
		if e != nil {
			close(p.jobs)
			p.wg.Wait()
			return nil, e
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer encoder.Close()
			c := chunkCodec{encoder: encoder, depth: 1}
			for job := range p.jobs {
				c.bases = newBaseCache(4)
				for i := len(job.choices) - 1; i >= 0; i-- {
					c.bases.put(job.slot.hint, job.choices[i])
				}
				c.encode(&job.slot)
				close(job.done)
			}
		}()
	}
	return p, nil
}
func (p *speculativePool) close() { close(p.jobs); p.wg.Wait() }
func sameBases(a, b []*deltaBase) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type speculativeBatch struct {
	pool    *speculativePool
	codec   *chunkCodec
	pending []*speculativeJob
	stable  int
	last    string
}

func (b *speculativeBatch) observe(s *encodeSlot) {
	if s.hint == b.last && s.base != nil && s.anchor == nil {
		b.stable++
	} else {
		b.stable = 0
	}
	b.last = s.hint
}
func (b *speculativeBatch) flush(ctx context.Context, sink func(*encodeSlot) error) error {
	if len(b.pending) == 0 {
		return nil
	}
	pending := b.pending
	b.pending = nil
	if err := ctx.Err(); err != nil {
		return err
	}
	choices := b.codec.bases.candidates(pending[0].slot.hint)
	submitted := 0
	// Always join submitted jobs, including on cancellation or sink failure.
	defer func() {
		for _, job := range pending[:submitted] {
			<-job.done
		}
	}()
	for _, job := range pending {
		job.choices = choices
		select {
		case b.pool.jobs <- job:
			submitted++
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, job := range pending {
		select {
		case <-job.done:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		s := &job.slot
		if !sameBases(choices, b.codec.bases.candidates(s.hint)) {
			b.codec.encode(s)
		} else if s.anchor != nil {
			// The private cache does not own the authoritative inter-path eviction order.
			b.codec.bases.put(s.hint, s.anchor)
			s.anchors = b.codec.bases.candidates(s.hint)
		}
		if s.err != nil {
			return s.err
		}
		b.observe(s)
		if e := sink(s); e != nil {
			return e
		}
	}
	return nil
}
func (b *speculativeBatch) add(ctx context.Context, slot *encodeSlot, sink func(*encodeSlot) error) error {
	eligible := b.pool != nil && b.codec.seed == nil && b.codec.depth == 1 && b.codec.bases.limit == 4 && len(slot.raw) >= 64<<10 && slot.hint != "" && slot.hint == b.last && b.stable >= 8
	if !eligible || (len(b.pending) > 0 && b.pending[0].slot.hint != slot.hint) {
		if e := b.flush(ctx, sink); e != nil {
			return e
		}
	}
	if eligible {
		b.pending = append(b.pending, &speculativeJob{slot: encodeSlot{key: slot.key, hint: slot.hint, raw: bytes.Clone(slot.raw)}, done: make(chan struct{})})
		if len(b.pending) == 16 {
			return b.flush(ctx, sink)
		}
		return nil
	}
	b.codec.encode(slot)
	if slot.err != nil {
		return slot.err
	}
	b.observe(slot)
	return sink(slot)
}
