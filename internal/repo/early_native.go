package repo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"gat/internal/gitdelta"
	"gat/internal/packfile"
	"gat/internal/store"
)

type earlyNativeReader interface {
	Has(string) bool
	Convert(string, []byte) (packrecipe.Output, error)
	Close()
}

type earlyNativeWorker struct {
	reader          earlyNativeReader
	queue, rejected *blobJobSpool
	stats           Stats
	err             error
}

// Admission is asynchronous: the foreground checks only inventory size/OID and
// pack membership. Convert runs once in its owning worker. Unsupported targets
// retain their original hints in a bounded disk spool; the foreground must join
// and replay those rows before consuming its ordinary sorted fallback stream.
type earlyNative struct {
	ctx                                                                context.Context
	cancel                                                             context.CancelFunc
	workers                                                            []*earlyNativeWorker
	wg                                                                 sync.WaitGroup
	mu                                                                 sync.Mutex
	failed                                                             error
	sealOnce, disposeOnce, countOnce, replayOnce, bodyOnce, joinedOnce sync.Once
	sealErr, closeErr, replayErr                                       error
	disposed, sealed, replayed                                         atomic.Bool
	queued, accepted, rejected                                         atomic.Int64
	uploaded                                                           atomic.Int64
	rootFrames, rootBytes, targetFrames, targetBytes                   atomic.Int64
	queuedBytes, acceptedBytes, rejectedBytes                          atomic.Int64
}

func startEarlyNative(ctx context.Context, cancel context.CancelFunc, source, format, tmp string, workers int, st *stage, backend store.Store) (*earlyNative, error) {
	pack := nativeImportPack(ctx)
	if format != "sha1" || pack == "" {
		return &earlyNative{ctx: ctx, cancel: cancel}, nil
	}
	return startEarlyNativeReaders(ctx, cancel, tmp, workers, st, backend, func(int) (earlyNativeReader, error) {
		r, err := packrecipe.OpenDeferred(ctx, pack, source)
		if err != nil {
			return nil, err
		}
		return r, nil
	})
}

func startEarlyNativeReaders(ctx context.Context, cancel context.CancelFunc, tmp string, workers int, st *stage, backend store.Store, open func(int) (earlyNativeReader, error)) (_ *earlyNative, retErr error) {
	if workers < 1 || workers > 256 {
		return nil, fmt.Errorf("invalid early native worker count")
	}
	p := &earlyNative{ctx: ctx, cancel: cancel}
	// Acquire every reader and both queues before launching any worker. A failed
	// constructor has no running work and cannot leave a source mapping behind.
	defer func() {
		if retErr != nil {
			for _, w := range p.workers {
				if w.reader != nil {
					w.reader.Close()
				}
				if w.queue != nil {
					retErr = errors.Join(retErr, w.queue.close())
				}
				if w.rejected != nil {
					retErr = errors.Join(retErr, w.rejected.close())
				}
			}
		}
	}()
	for i := 0; i < workers; i++ {
		w := &earlyNativeWorker{}
		p.workers = append(p.workers, w)
		var err error
		w.queue, err = newBlobJobSpool(tmp)
		if err != nil {
			return nil, err
		}
		w.rejected, err = newBlobJobSpool(tmp)
		if err != nil {
			return nil, err
		}
		reader, err := open(i)
		if err != nil {
			return nil, err
		}
		w.reader = reader
	}
	prefix := rand.Text()
	nativePrefix := "nativechain-stream"
	if archiveImportEnabled(ctx) {
		nativePrefix = "nativechain-auth-stream"
	}
	var pending atomic.Int32
	pending.Store(int32(len(p.workers)))
	for i, w := range p.workers {
		writer := &packWriter{ctx: ctx, store: backend, prefix: fmt.Sprintf("%s-%s-%d", nativePrefix, prefix, i), stats: &w.stats, data: make([]byte, 0, PackSize), onFlush: func(n int64) { p.uploaded.Add(n) }}
		p.wg.Go(func() {
			defer func() {
				if pending.Add(-1) == 0 {
					streamingTrace("native_workers_exited", p.accepted.Load())
				}
			}()
			w.err = w.runNative(p, st, writer)
			if w.err != nil && p.ctx.Err() == nil {
				p.mu.Lock()
				if p.failed == nil {
					p.failed = w.err
				}
				p.mu.Unlock()
				p.cancel()
			}
		})
	}
	return p, nil
}

func (p *earlyNative) tryAdd(oid, hint string, size int64) (bool, error) {
	if err := p.ctx.Err(); err != nil {
		return false, err
	}
	if p.sealed.Load() {
		return false, os.ErrClosed
	}
	if len(p.workers) == 0 || len(oid) != 40 || size <= 0 || size > ChunkSize {
		return false, nil
	}
	id, err := hex.DecodeString(oid)
	if err != nil {
		return false, fmt.Errorf("invalid early native OID")
	}
	// The hint is already present in the streaming Git walk. Keep the existing
	// family lane assignment without waiting for the complete path sort.
	group := hint + "/0000000000000000"
	if hint == "" {
		group = string(id)
	}
	w := p.workers[chunkWorker(group, len(p.workers))]
	// Has only reads immutable pack-index bytes; the worker does not mutate them.
	if !w.reader.Has(oid) {
		return false, nil
	}
	if err := w.queue.add(blobImportJob{oid: oid, hint: hint, size: size}, false); err != nil {
		return false, err
	}
	p.queued.Add(1)
	p.queuedBytes.Add(size)
	return true, nil
}

func (w *earlyNativeWorker) runNative(p *earlyNative, st *stage, packs *packWriter) (retErr error) {
	defer func() { retErr = errors.Join(retErr, w.rejected.finish()) }()
	sharedRoots := make(map[string]chunkBase)
	for {
		batch, err := w.queue.batch(p.ctx)
		if err == io.EOF {
			return packs.flush()
		}
		if err != nil {
			return err
		}
		for _, job := range batch {
			if err := p.ctx.Err(); err != nil {
				return err
			}
			p.bodyOnce.Do(func() { streamingTrace("native_first_body", p.queued.Load()) })
			if r, ok := w.reader.(*archiveNativeReader); ok {
				owned, err := r.stage(job, st, &w.stats)
				if err != nil {
					return err
				}
				if owned {
					p.accepted.Add(1)
					p.acceptedBytes.Add(job.size)
					continue
				}
			}
			out, err := w.reader.Convert(job.oid, nil)
			if errors.Is(err, gitdelta.ErrLimit) {
				if err := w.rejected.add(job, false); err != nil {
					return err
				}
				p.rejected.Add(1)
				p.rejectedBytes.Add(job.size)
				continue
			}
			if err != nil {
				return err
			}
			if int64(out.Size) != job.size {
				return fmt.Errorf("early native size mismatch")
			}
			length := len(out.Data)
			if out.Base != nil {
				length += len(out.Base.Packed)
			}
			if len(packs.data)+length > PackSize {
				if err := packs.flush(); err != nil {
					return err
				}
			}
			var base *chunkBase
			if out.Base != nil {
				if cached, ok := sharedRoots[out.Base.Hash]; ok {
					base = &cached
				} else {
					c, err := packs.add(out.Base.Packed, out.Base.Hash)
					if err != nil {
						return err
					}
					p.rootFrames.Add(1)
					p.rootBytes.Add(int64(len(out.Base.Packed)))
					b := chunkLocation(c)
					base = &b
					if len(sharedRoots) >= 4096 {
						clear(sharedRoots)
					}
					sharedRoots[out.Base.Hash] = b
				}
			}
			c, err := packs.add(out.Data, out.Hash)
			if err != nil {
				return err
			}
			p.targetFrames.Add(1)
			p.targetBytes.Add(int64(len(out.Data)))
			c.Base = base
			if base == nil {
				if len(sharedRoots) >= 4096 {
					clear(sharedRoots)
				}
				sharedRoots[c.Hash] = chunkLocation(c)
			}
			if err := st.put(chunkKey(job.oid, 0), c); err != nil {
				return err
			}
			if err := st.put("o/"+job.oid, object{Kind: "blob", Size: job.size}); err != nil {
				return err
			}
			w.stats.Objects++
			w.stats.Blobs++
			w.stats.Bytes += job.size
			w.stats.Chunks++
			if base != nil {
				w.stats.DeltaChunks++
				w.stats.MaxDepth = max(w.stats.MaxDepth, 1)
			}
			p.accepted.Add(1)
			p.acceptedBytes.Add(job.size)
		}
	}
}

func (p *earlyNative) seal() error {
	p.sealOnce.Do(func() {
		p.sealed.Store(true)
		for _, w := range p.workers {
			p.sealErr = errors.Join(p.sealErr, w.queue.finish())
		}
		if p.sealErr != nil {
			p.cancel()
		}
	})
	return p.sealErr
}

func (p *earlyNative) join() error {
	p.seal()
	p.wg.Wait()
	p.joinedOnce.Do(func() {
		streamingTrace("native_workers_joined", p.accepted.Load())
		streamingTrace("native_queued", p.queued.Load())
		streamingTrace("native_rejected", p.rejected.Load())
		streamingTrace("native_queued_raw_bytes", p.queuedBytes.Load())
		streamingTrace("native_accepted_raw_bytes", p.acceptedBytes.Load())
		streamingTrace("native_rejected_raw_bytes", p.rejectedBytes.Load())
		streamingTrace("native_root_frames", p.rootFrames.Load())
		streamingTrace("native_root_bytes", p.rootBytes.Load())
		streamingTrace("native_target_frames", p.targetFrames.Load())
		streamingTrace("native_target_bytes", p.targetBytes.Load())
		traceArchiveBlobs(p.workers)
		var totals packrecipe.ConversionCounters
		for _, w := range p.workers {
			if r := archiveFallbackReader(w.reader); r != nil {
				c := r.SnapshotCounters()
				totals.FastObjects += c.FastObjects
				totals.FastRawBytes += c.FastRawBytes
				totals.LegacyObjects += c.LegacyObjects
				totals.LegacyRawBytes += c.LegacyRawBytes
				totals.EagerObjects += c.EagerObjects
				totals.EagerRawBytes += c.EagerRawBytes
				totals.PrefixInflations += c.PrefixInflations
				totals.PrefixBytes += c.PrefixBytes
				totals.FramePlans += c.FramePlans
				totals.MetadataHits += c.MetadataHits
				totals.BundledFrameBytes += c.BundledFrameBytes
				totals.ReturnedRootBytes += c.ReturnedRootBytes
				totals.FullInflations += c.FullInflations
				totals.FullInflatedBytes += c.FullInflatedBytes
				totals.Reconstructions += c.Reconstructions
				totals.ReconstructedBytes += c.ReconstructedBytes
				totals.GetCalls += c.GetCalls
				totals.FastFullInflations += c.FastFullInflations
				totals.FastFullInflatedBytes += c.FastFullInflatedBytes
				totals.FastReconstructions += c.FastReconstructions
				totals.FastReconstructedBytes += c.FastReconstructedBytes
				totals.FastGetCalls += c.FastGetCalls
			}
		}
		streamingTrace("conversion_fast_objects", totals.FastObjects)
		streamingTrace("conversion_fast_raw_bytes", totals.FastRawBytes)
		streamingTrace("conversion_legacy_objects", totals.LegacyObjects)
		streamingTrace("conversion_legacy_raw_bytes", totals.LegacyRawBytes)
		streamingTrace("conversion_eager_objects", totals.EagerObjects)
		streamingTrace("conversion_eager_raw_bytes", totals.EagerRawBytes)
		streamingTrace("conversion_prefix_inflations", totals.PrefixInflations)
		streamingTrace("conversion_prefix_bytes", totals.PrefixBytes)
		streamingTrace("conversion_frame_plans", totals.FramePlans)
		streamingTrace("conversion_metadata_hits", totals.MetadataHits)
		streamingTrace("conversion_bundled_frame_bytes", totals.BundledFrameBytes)
		streamingTrace("conversion_returned_root_bytes", totals.ReturnedRootBytes)
		streamingTrace("conversion_full_inflations", totals.FullInflations)
		streamingTrace("conversion_full_inflated_bytes", totals.FullInflatedBytes)
		streamingTrace("conversion_reconstructions", totals.Reconstructions)
		streamingTrace("conversion_reconstructed_bytes", totals.ReconstructedBytes)
		streamingTrace("conversion_get_calls", totals.GetCalls)
		streamingTrace("conversion_fast_full_inflations", totals.FastFullInflations)
		streamingTrace("conversion_fast_full_inflated_bytes", totals.FastFullInflatedBytes)
		streamingTrace("conversion_fast_reconstructions", totals.FastReconstructions)
		streamingTrace("conversion_fast_reconstructed_bytes", totals.FastReconstructedBytes)
		streamingTrace("conversion_fast_get_calls", totals.FastGetCalls)

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
	return p.ctx.Err()
}

func (p *earlyNative) replayRejected(emit func(oid, hint string, size int64) error) error {
	p.replayOnce.Do(func() {
		if p.disposed.Load() {
			p.replayErr = os.ErrClosed
			return
		}
		if err := p.join(); err != nil {
			p.replayErr = err
			return
		}
		for _, w := range p.workers {
			for {
				jobs, err := w.rejected.batch(p.ctx)
				if err == io.EOF {
					break
				}
				if err != nil {
					p.replayErr = err
					return
				}
				for _, job := range jobs {
					if err := emit(job.oid, job.hint, job.size); err != nil {
						p.replayErr = err
						return
					}
				}
			}
		}
		p.replayed.Store(true)
	})
	return p.replayErr
}

// finish always joins and disposes. Stats are counted once, only after every
// worker has stopped; rejected objects are counted by the ordinary fallback.
func (p *earlyNative) finish(stats *Stats) error {
	err := p.join()
	p.disposeOnce.Do(func() {
		for _, w := range p.workers {
			w.reader.Close()
			p.closeErr = errors.Join(p.closeErr, w.queue.close(), w.rejected.close())
		}
		p.disposed.Store(true)
	})
	if err != nil {
		return err
	}
	if p.closeErr != nil {
		return p.closeErr
	}
	if p.replayErr != nil {
		return p.replayErr
	}
	if p.rejected.Load() > 0 && stats != nil {
		// Finishing for publication without foreground replay would lose objects.
		if !p.replayed.Load() {
			return fmt.Errorf("early native rejections were not replayed")
		}
	}
	if stats != nil {
		p.countOnce.Do(func() {
			for _, w := range p.workers {
				stats.Objects += w.stats.Objects
				stats.Blobs += w.stats.Blobs
				stats.Bytes += w.stats.Bytes
				stats.Chunks += w.stats.Chunks
				stats.DeltaChunks += w.stats.DeltaChunks
				stats.UploadedBytes += w.stats.UploadedBytes
				stats.MaxDepth = max(stats.MaxDepth, w.stats.MaxDepth)
			}
			p.uploaded.Store(0)
		})
	}
	return nil
}
