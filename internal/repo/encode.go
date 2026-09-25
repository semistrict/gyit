package repo

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/klauspost/compress/zstd"
)

type encodeSlot struct {
	key, hash, hint string
	base, anchor    *deltaBase
	anchors         []*deltaBase
	raw, compressed []byte
	ready           chan struct{}
	err             error
}

// chunkEncoder overlaps compression with Git reads and durable pack writes.
// A fixed ring owns all outstanding buffers; only the caller invokes the sink,
// in submission order, so staging transactions remain confined to one goroutine.
type chunkEncoder struct {
	ctx           context.Context
	sink          func(*encodeSlot) error
	slots         []encodeSlot
	head, pending int
	jobs          []chan *encodeSlot
	workers       sync.WaitGroup
	failed        chan error
}

func newChunkEncoder(ctx context.Context, workers, depth, candidates int, seed func(string) ([]*deltaBase, error), sink func(*encodeSlot) error) (*chunkEncoder, error) {
	if workers < 1 {
		return nil, fmt.Errorf("compression workers must be positive")
	}
	encoders := make([]*zstd.Encoder, 0, workers)
	for i := 0; i < workers; i++ {
		encoder, err := newCompressor()
		if err != nil {
			for _, e := range encoders {
				e.Close()
			}
			return nil, err
		}
		encoders = append(encoders, encoder)
	}
	// Let source reads advance past short compression stalls without changing
	// worker ownership or sink order. Only the importer owns these buffers.
	const slotsPerWorker = 16
	c := &chunkEncoder{ctx: ctx, sink: sink, slots: make([]encodeSlot, slotsPerWorker*workers), jobs: make([]chan *encodeSlot, workers), failed: make(chan error, 1)}
	for i := range c.slots {
		c.slots[i].ready = make(chan struct{}, 1)
	}
	for worker, encoder := range encoders {
		jobs := make(chan *encodeSlot, slotsPerWorker*workers)
		c.jobs[worker] = jobs
		c.workers.Go(func() {
			defer encoder.Close()
			codec := &chunkCodec{encoder: encoder, bases: newBaseCache(candidates), depth: depth, seed: seed}
			for slot := range jobs {
				codec.encode(slot)
				if slot.err != nil {
					select {
					case c.failed <- slot.err:
					default:
					}
				}
				slot.ready <- struct{}{}
			}
		})
	}
	return c, nil
}

func (c *chunkEncoder) add(key string, raw []byte) error { return c.addHint(key, "", raw) }

func (c *chunkEncoder) addHint(key, hint string, raw []byte) error {
	if err := c.err(); err != nil {
		return err
	}
	if len(raw) > ChunkSize {
		return fmt.Errorf("chunk exceeds size limit")
	}
	if c.pending == len(c.slots) {
		if err := c.drain(); err != nil {
			return err
		}
	}
	slot := &c.slots[(c.head+c.pending)%len(c.slots)]
	slot.key, slot.hint = key, hint
	slot.raw = append(slot.raw[:0], raw...)
	c.pending++
	worker := chunkWorker(hint, len(c.jobs))
	if hint == "" {
		worker = (c.head + c.pending - 1) % len(c.jobs)
	}
	c.jobs[worker] <- slot
	return nil
}

func (c *chunkEncoder) drain() error {
	if err := c.err(); err != nil {
		return err
	}
	slot := &c.slots[c.head]
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case err := <-c.failed:
		return err
	case <-slot.ready:
	}
	if err := c.ctx.Err(); err != nil {
		return err
	}
	if slot.err != nil {
		return slot.err
	}
	if err := c.sink(slot); err != nil {
		return err
	}
	c.head = (c.head + 1) % len(c.slots)
	c.pending--
	return nil
}

func (c *chunkEncoder) err() error {
	select {
	case err := <-c.failed:
		return err
	default:
		return c.ctx.Err()
	}
}

func (c *chunkEncoder) finish() error {
	for c.pending > 0 {
		if err := c.drain(); err != nil {
			return err
		}
	}
	return c.ctx.Err()
}

// close also joins workers when a source or sink fails with queued work. It does
// not call the sink; abandoned chunks can never publish partial staged results.
func (c *chunkEncoder) close() {
	for _, jobs := range c.jobs {
		close(jobs)
	}
	c.workers.Wait()
}

func chunkWorker(hint string, workers int) int {
	h := fnv.New32a()
	h.Write([]byte(hint))
	return int(h.Sum32()) % workers
}

// A codec is owned by one worker. Keeping delta selection here makes buffered
// compression and direct source ingestion use identical candidates and costs.
type chunkCodec struct {
	encoder                   *zstd.Encoder
	bases                     *baseCache
	depth                     int
	seed                      func(string) ([]*deltaBase, error)
	deltaBuffer, deltaScratch []byte
}

func (c *chunkCodec) encodeFull(slot *encodeSlot) {
	slot.base, slot.anchor, slot.err, slot.anchors = nil, nil, nil, nil
	slot.compressed = c.encoder.EncodeAll(slot.raw, slot.compressed[:0])
	slot.hash = fmt.Sprintf("%x", sha256.Sum256(slot.raw))
	if slot.hint != "" && len(slot.compressed) >= 256 {
		choices := c.bases.candidates(slot.hint)
		if len(choices) == 0 && c.seed != nil {
			choices, slot.err = c.seed(slot.hint)
			if slot.err != nil {
				return
			}
			for i := len(choices) - 1; i >= 0; i-- {
				c.bases.put(slot.hint, choices[i])
			}
			choices = c.bases.candidates(slot.hint)
		}
		// Compare the real compressed deltas, including a conservative allowance
		// for every embedded dependency reference. Ties favor a shallower chain.
		bestCost := len(slot.compressed) * 4 / 5
		for _, base := range choices {
			if base.depth >= c.depth {
				continue
			}
			c.deltaScratch = base.deltaInto(c.deltaScratch[:0], slot.raw)
			c.deltaBuffer = c.encoder.EncodeAll(c.deltaScratch, c.deltaBuffer[:0])
			cost := len(c.deltaBuffer) + 200*(base.depth+1)
			if cost < bestCost || (cost == bestCost && slot.base != nil && base.depth < slot.base.depth) {
				bestCost = cost
				slot.compressed, c.deltaBuffer = c.deltaBuffer, slot.compressed
				slot.base = base
			}
		}
		resultDepth := 0
		if slot.base != nil {
			resultDepth = slot.base.depth + 1
		}
		if resultDepth < c.depth {
			anchor := newDeltaBase(slot.raw)
			anchor.depth = resultDepth
			c.bases.put(slot.hint, anchor)
			slot.anchor = anchor
			slot.anchors = c.bases.candidates(slot.hint)
		}
	}
}

func writeEncodedChunk(slot *encodeSlot, packs *packWriter, st *stage, stats *Stats) error {
	c, err := packs.add(slot.compressed, slot.hash)
	if err != nil {
		return err
	}
	if slot.base != nil {
		loc := slot.base.location
		if !validChunkRange(loc) {
			return fmt.Errorf("delta base has not been written")
		}
		c.Base = &loc
		stats.DeltaChunks++
		stats.MaxDepth = max(stats.MaxDepth, slot.base.depth+1)
	}
	if slot.anchor != nil {
		slot.anchor.location = chunkLocation(c)
		record := anchorRecord{}
		for _, candidate := range slot.anchors {
			if !validChunkRange(candidate.location) {
				return fmt.Errorf("anchor has not been written")
			}
			record.Candidates = append(record.Candidates, candidate.location)
		}
		if err := st.put(anchorKey(slot.hint), record); err != nil {
			return err
		}
	}
	stats.Chunks++
	return st.put(slot.key, c)
}

// Evaluate the existing delta candidates first. Stop the full-frame encoder
// only once its conservative lower bound proves that the same delta wins.
// This preserves payload bytes, dependency depth, tie breaking, and anchor order.
func (c *chunkCodec) encode(slot *encodeSlot) {
	if len(slot.raw) < 4096 || slot.hint == "" || c.seed != nil {
		c.encodeFull(slot)
		return
	}
	choices := c.bases.candidates(slot.hint)
	if len(choices) == 0 {
		c.encodeFull(slot)
		return
	}
	slot.base, slot.anchor, slot.err, slot.anchors = nil, nil, nil, nil
	slot.hash = fmt.Sprintf("%x", sha256.Sum256(slot.raw))
	bestCost := int(^uint(0) >> 1)
	for _, base := range choices {
		if base.depth >= c.depth {
			continue
		}
		c.deltaScratch = base.deltaInto(c.deltaScratch[:0], slot.raw)
		c.deltaBuffer = c.encoder.EncodeAll(c.deltaScratch, c.deltaBuffer[:0])
		cost := len(c.deltaBuffer) + 200*(base.depth+1)
		if cost < bestCost || (cost == bestCost && slot.base != nil && base.depth < slot.base.depth) {
			bestCost = cost
			slot.compressed, c.deltaBuffer = c.deltaBuffer, slot.compressed
			slot.base = base
		}
	}
	if slot.base == nil {
		c.encodeFull(slot)
		return
	}
	// Original admission requires cost < floor(4*fullLength/5) and a
	// full frame of at least 256 bytes. The ceiling below is the first
	// length that satisfies both, including exact integer boundaries.
	limit := max(256, (5*(bestCost+1)+3)/4)
	var stopped bool
	c.deltaBuffer, stopped = c.encoder.EncodeAllBelow(slot.raw, c.deltaBuffer[:0], limit)
	if !stopped && (len(c.deltaBuffer) < 256 || bestCost >= len(c.deltaBuffer)*4/5) {
		slot.base = nil
		slot.compressed, c.deltaBuffer = c.deltaBuffer, slot.compressed
	}
	eligible := stopped || slot.base != nil || len(slot.compressed) >= 256
	if eligible {
		resultDepth := 0
		if slot.base != nil {
			resultDepth = slot.base.depth + 1
		}
		if resultDepth < c.depth {
			anchor := newDeltaBase(slot.raw)
			anchor.depth = resultDepth
			c.bases.put(slot.hint, anchor)
			slot.anchor = anchor
			slot.anchors = c.bases.candidates(slot.hint)
		}
	}
}
