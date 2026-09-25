package repo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/klauspost/compress/zstd"
)

type treeJob struct {
	oid string
	raw []byte
}

// Each worker owns its parser, encoder, pack buffer, and wide-directory spill
// directory. At most 32 bodies of at most ChunkSize bytes are in flight; larger
// trees remain on the foreground streaming path. Publication waits for finish.
type treeImporter struct {
	ctx     context.Context
	jobs    chan treeJob
	buffers chan []byte
	once    sync.Once
	wg      sync.WaitGroup
	mu      sync.Mutex
	err     error
}

func startTreeImporter(ctx context.Context, cancel context.CancelFunc, workers int, base *directoryWriter, oidBytes int) *treeImporter {
	p := &treeImporter{ctx: ctx, jobs: make(chan treeJob, 32), buffers: make(chan []byte, 32)}
	// Keep one prefix for compact directory routing pages, with disjoint
	// pack-number sequences for each worker and the foreground large-tree path.
	base.pages.step = workers + 1

	for i := 0; i < 32; i++ {
		p.buffers <- nil
	}
	for i := 0; i < workers; i++ {
		p.wg.Go(func() {
			err := func() error {
				enc, e := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithZeroFrames(true))
				if e != nil {
					return e
				}
				defer enc.Close()
				tmp, e := os.MkdirTemp(base.tmp, "tree-worker-*")
				if e != nil {
					return e
				}
				defer os.RemoveAll(tmp)
				pages := &indexWriter{ctx: ctx, store: base.pages.store, prefix: base.pages.prefix, number: i + 1, step: workers + 1}
				w := &directoryWriter{pages: pages, encoder: enc, stage: base.stage, sizes: base.sizes, old: base.old, tmp: tmp}
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case j, ok := <-p.jobs:
						if !ok {
							return pages.flush()
						}
						ref, e := w.readTree(bytes.NewReader(j.raw), oidBytes)
						if e == nil {
							e = base.stage.put("o/"+j.oid, object{Kind: "tree", Size: int64(len(j.raw)), Directory: ref})
						}
						p.buffers <- j.raw
						if e != nil {
							return e
						}
					}
				}
			}()
			if err != nil && ctx.Err() == nil {
				p.mu.Lock()
				if p.err == nil {
					p.err = err
				}
				p.mu.Unlock()
				cancel()
			}
		})
	}
	return p
}
func (p *treeImporter) add(oid string, size int64, r io.Reader) error {
	if size < 0 || size > ChunkSize {
		return fmt.Errorf("tree size limit")
	}
	var b []byte
	select {
	case b = <-p.buffers:
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
	if int64(cap(b)) < size {
		b = make([]byte, size)
	} else {
		b = b[:size]
	}
	if _, e := io.ReadFull(r, b); e != nil {
		p.buffers <- b
		return e
	}
	select {
	case p.jobs <- treeJob{oid, b}:
		return nil
	case <-p.ctx.Done():
		p.buffers <- b
		return p.ctx.Err()
	}
}

// finish is idempotent and joins all workers, including their pending uploads.
func (p *treeImporter) finish() error {
	p.once.Do(func() { close(p.jobs) })
	p.wg.Wait()
	if p.err != nil {
		return p.err
	}
	return p.ctx.Err()
}
