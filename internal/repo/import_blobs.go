package repo

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"gyit/internal/gitdelta"
	"gyit/internal/packfile"
	"hash"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Each worker owns its Git stream, codec, and pack writer. Single-chunk blobs
// go directly from source to compression; larger blobs retain the foreground
// stream and send bounded chunks to the same lanes used by chunkEncoder. This
// preserves candidate-cache order and every encoded dependency decision.
type blobImportJob struct {
	oid, hint string
	size      int64
	raw       []byte
	part      int64
	group     string
	waitRaw   bool
}
type blobImportWorker struct {
	skipNative bool
	buffers    chan []byte
	jobs       chan blobImportJob
	stats      Stats
	spool      *blobJobSpool
	err        error
}
type blobImporter struct {
	uploaded          atomic.Int64
	mu                sync.Mutex
	failed            error
	buffers           chan []byte
	next              int
	ctx               context.Context
	workers           []*blobImportWorker
	wg                sync.WaitGroup
	once              sync.Once
	dispose           sync.Once
	sealErr, closeErr error
	cancel            context.CancelFunc
}

func startBlobImporter(ctx context.Context, cancel context.CancelFunc, source, format string, workers, depth, candidates int, st *stage, packs *packWriter) (*blobImporter, error) {
	return startBlobImporterMode(ctx, cancel, source, format, workers, depth, candidates, st, packs, false)
}
func startBlobImporterMode(ctx context.Context, cancel context.CancelFunc, source, format string, workers, depth, candidates int, st *stage, packs *packWriter, skipNative bool) (*blobImporter, error) {
	p := &blobImporter{ctx: ctx, cancel: cancel, buffers: make(chan []byte, 64)}
	for i := 0; i < 64; i++ {
		p.buffers <- nil
	}
	// Acquire every queue before starting any Git process or worker. A partial
	// constructor failure has no asynchronous cleanup or error ownership.
	for i := 0; i < workers; i++ {
		q, err := newBlobJobSpool(st.tmp)
		if err != nil {
			for _, w := range p.workers {
				err = errors.Join(err, w.spool.close())
			}
			return nil, err
		}
		p.workers = append(p.workers, &blobImportWorker{skipNative: skipNative, jobs: make(chan blobImportJob, 128), buffers: p.buffers, spool: q})
	}
	for i, w := range p.workers {
		digest := sha256.Sum256([]byte(fmt.Sprintf("%s-f%d", packs.prefix, i)))
		writer := &packWriter{ctx: ctx, store: packs.store, prefix: fmt.Sprintf("%x", digest[:16]), stats: &w.stats, data: make([]byte, 0, PackSize), onFlush: func(n int64) { p.uploaded.Add(n) }}
		p.wg.Go(func() {
			w.err = w.run(ctx, source, format, depth, candidates, st, writer)
			if w.err != nil && ctx.Err() == nil {
				p.mu.Lock()
				if p.failed == nil {
					p.failed = w.err
				}
				p.mu.Unlock()
				cancel()
			}
		})
	}
	return p, nil
}

func (p *blobImporter) route(group string) int {
	i := chunkWorker(group, len(p.workers))
	if group == "" {
		i = p.next % len(p.workers)
	}
	p.next = (p.next + 1) % len(p.workers)
	return i
}

func (p *blobImporter) add(oid, hint string, size int64) error {
	group := ""
	if hint != "" {
		group = hint + "/0000000000000000"
	}
	i := 0
	if size > 0 {
		i = p.route(group)
	}
	if err := p.ctx.Err(); err != nil {
		return err
	}
	return p.workers[i].spool.add(blobImportJob{oid: oid, hint: hint, size: size}, false)
}

func (p *blobImporter) addRaw(oid, group string, part int64, raw []byte) error {
	var buf []byte
	select {
	case buf = <-p.buffers:
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
	buf = append(buf[:0], raw...)
	i := p.route(group)
	if err := p.workers[i].spool.add(blobImportJob{oid: oid, part: part, group: group}, true); err != nil {
		p.buffers <- buf
		return err
	}
	select {
	case p.workers[i].jobs <- blobImportJob{oid: oid, part: part, group: group, raw: buf}:
		return nil
	case <-p.ctx.Done():
		p.buffers <- buf
		return p.ctx.Err()
	}
}

// finish is also called during error cleanup; it always joins every writer
// before staging files can be closed or a manifest can be published.
func (p *blobImporter) finish(stats *Stats) error {
	p.once.Do(func() {
		for _, w := range p.workers {
			if err := w.spool.finish(); err != nil {
				p.sealErr = errors.Join(p.sealErr, err)
				p.cancel()
			}
			close(w.jobs)
		}
	})
	p.wg.Wait()
	p.dispose.Do(func() {
		for _, w := range p.workers {
			p.closeErr = errors.Join(p.closeErr, w.spool.close())
		}
	})
	if p.failed != nil {
		return p.failed
	}
	if p.sealErr != nil {
		return p.sealErr
	}
	for _, w := range p.workers {
		if w.err != nil {
			return w.err
		}
	}
	if p.closeErr != nil {
		return p.closeErr
	}
	if stats != nil {
		for _, w := range p.workers {
			stats.Chunks += w.stats.Chunks
			stats.DeltaChunks += w.stats.DeltaChunks
			stats.UploadedBytes += w.stats.UploadedBytes
			stats.MaxDepth = max(stats.MaxDepth, w.stats.MaxDepth)
		}
		// Progress used the atomic total while writers were running. Ownership
		// transfers to the final Stats only after every worker has stopped.
		p.uploaded.Store(0)
	}
	return p.ctx.Err()
}

func (w *blobImportWorker) run(ctx context.Context, source, format string, depth, candidates int, st *stage, packs *packWriter) error {
	encoder, e := newCompressor()
	if e != nil {
		return e
	}
	defer encoder.Close()
	var nr *packrecipe.Reader
	if !w.skipNative && format == "sha1" && nativeImportPack(ctx) != "" {
		nr, e = packrecipe.OpenDeferred(ctx, nativeImportPack(ctx), source)
		if e != nil {
			return e
		}
		defer nr.Close()
	}
	np := &packWriter{ctx: ctx, store: packs.store, prefix: "nativechain-" + packs.prefix, stats: &w.stats, data: make([]byte, 0, PackSize), onFlush: packs.onFlush}
	sharedRoots := map[string]chunkBase{}
	saveNative := func(job blobImportJob) (bool, error) {
		if nr == nil || job.waitRaw || job.size == 0 || !nr.Has(job.oid) {
			return false, nil
		}
		out, err := nr.Convert(job.oid, nil)
		if errors.Is(err, gitdelta.ErrLimit) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if int64(out.Size) != job.size {
			return false, fmt.Errorf("native size mismatch")
		}
		length := len(out.Data)
		if out.Base != nil {
			length += len(out.Base.Packed)
		}
		if len(np.data)+length > PackSize {
			if err = np.flush(); err != nil {
				return false, err
			}
		}
		var base *chunkBase
		if out.Base != nil {
			if cached, ok := sharedRoots[out.Base.Hash]; ok {
				base = &cached
			} else {
				c, err := np.add(out.Base.Packed, out.Base.Hash)
				if err != nil {
					return false, err
				}
				b := chunkLocation(c)
				base = &b
				if len(sharedRoots) >= 4096 {
					clear(sharedRoots)
				}
				sharedRoots[out.Base.Hash] = b
			}
		}

		c, err := np.add(out.Data, out.Hash)
		if err != nil {
			return false, err
		}
		c.Base = base
		if base == nil {
			if len(sharedRoots) >= 4096 {
				clear(sharedRoots)
			}
			sharedRoots[c.Hash] = chunkLocation(c)
		}
		if err = st.put(chunkKey(job.oid, 0), c); err != nil {
			return false, err
		}
		w.stats.Chunks++
		if base != nil {
			w.stats.DeltaChunks++
			w.stats.MaxDepth = max(w.stats.MaxDepth, 1)
		}
		return true, nil
	}
	codec := &chunkCodec{encoder: encoder, bases: newBaseCache(candidates), depth: depth}
	var slot encodeSlot
	raw := make([]byte, ChunkSize)
	var stderr bytes.Buffer
	cmd := git(ctx, source, "cat-file", "--batch-command", "--buffer")
	cmd.Stderr = &stderr
	input, e := cmd.StdinPipe()
	if e != nil {
		return e
	}
	defer input.Close()
	output, e := cmd.StdoutPipe()
	if e != nil {
		return e
	}
	if e = cmd.Start(); e != nil {
		return e
	}
	waited := false
	defer func() {
		if !waited {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	reader := bufio.NewReaderSize(output, 64<<10)
	requests := bufio.NewWriterSize(input, 32<<10)
	save := func(job blobImportJob, part int64, body []byte) error {
		hint := job.group
		if job.hint != "" {
			hint = fmt.Sprintf("%s/%016x", job.hint, part)
		}
		slot.key, slot.hint, slot.raw = chunkKey(job.oid, part), hint, body
		codec.encode(&slot)
		if slot.err != nil {
			return slot.err
		}
		return writeEncodedChunk(&slot, packs, st, &w.stats)
	}
	for {
		batch, e := w.spool.batch(ctx)
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		done := make([]bool, len(batch))
		for i, job := range batch {
			var err error
			done[i], err = saveNative(job)
			if err != nil {
				return err
			}
			if !job.waitRaw && !done[i] {
				fmt.Fprintf(requests, "contents %s\n", job.oid)
			}
		}
		fmt.Fprintln(requests, "flush")
		if e = requests.Flush(); e != nil {
			return e
		}
		for i, job := range batch {
			if done[i] {
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if job.waitRaw {
				// Consume raw bodies only as their markers are executed. Waiting
				// while assembling a batch could exhaust all shared buffers.
				var got blobImportJob
				select {
				case <-ctx.Done():
					return ctx.Err()
				case v, ok := <-w.jobs:
					if !ok {
						return fmt.Errorf("missing queued raw chunk")
					}
					got = v
				}
				if got.oid != job.oid || got.part != job.part || got.group != job.group {
					return fmt.Errorf("queued raw chunk order mismatch")
				}
				job = got
				err := save(job, job.part, job.raw)
				w.buffers <- job.raw
				if err != nil {
					return err
				}
				continue
			}
			header, e := reader.ReadString('\n')
			if e != nil {
				return e
			}
			f := strings.Fields(header)
			if len(f) != 3 || f[0] != job.oid || f[1] != "blob" {
				return fmt.Errorf("blob import source header %q", header)
			}
			size, e := strconv.ParseInt(f[2], 10, 64)
			if e != nil || size != job.size {
				return fmt.Errorf("blob import source size")
			}
			var hash hash.Hash = sha1.New()
			if format == "sha256" {
				hash = sha256.New()
			}
			fmt.Fprintf(hash, "blob %d\x00", size)
			limited := &io.LimitedReader{R: reader, N: size}
			body := io.TeeReader(limited, hash)
			for part := int64(0); ; part++ {
				n, e := io.ReadFull(body, raw)
				if e != nil && e != io.EOF && e != io.ErrUnexpectedEOF {
					return e
				}
				if n > 0 {
					if err := save(job, part, raw[:n]); err != nil {
						return err
					}
				}
				if e != nil {
					break
				}
			}
			if limited.N != 0 || hex.EncodeToString(hash.Sum(nil)) != job.oid {
				return fmt.Errorf("blob import source checksum")
			}
			sep, e := reader.ReadByte()
			if e != nil || sep != '\n' {
				return fmt.Errorf("blob import source separator")
			}
		}
	}
	input.Close()
	e = cmd.Wait()
	waited = true
	if e != nil {
		return fmt.Errorf("blob import source: %w: %s", e, stderr.String())
	}
	if err := np.flush(); err != nil {
		return err
	}
	return packs.flush()
}
