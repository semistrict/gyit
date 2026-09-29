//go:build !js

package repo

import (
	"context"
	"runtime/trace"

	bolt "go.etcd.io/bbolt"
	"gyit/internal/spill"
)

// One publication can run while ingestion prepares the next batch. The frozen
// changes own their bytes and spill after 1 MiB; no goroutine borrows an active
// bbolt transaction. Completion and failure are observed before launching the
// next publication, keeping CAS and coverage strictly ordered.
type historyPublication struct {
	cancel  context.CancelFunc
	uploads *publicationUploads
	done    chan struct{}
	err     error
}

func (p *Progressive) publishHistoryAsync(ctx context.Context, bucket *bolt.Bucket, uploads *publicationUploads, include func(context.Context, *spill.Sorter) error) (*historyPublication, error) {
	records, err := spill.New(p.temp, 1<<20)
	if err != nil {
		return nil, err
	}
	err = bucket.ForEach(func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return records.Add(key, value)
	})
	if err != nil {
		records.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	job := &historyPublication{cancel: cancel, uploads: uploads, done: make(chan struct{})}
	go func() {
		ctx, task := trace.NewTask(ctx, "history-publication")
		defer task.End()
		defer close(job.done)
		defer cancel()
		defer records.Close()
		defer uploads.close()
		// Slow payload uploads must not hold the shared writer lock. Pack
		// acquisition and snapshot metadata can keep publishing independently.
		trace.WithRegion(ctx, "history-payload-wait", func() { job.err = uploads.wait() })
		if job.err != nil {
			return
		}
		if include != nil {
			// Join immutable pack uploads outside the writer lock and include
			// their recipes in the first history CAS. Coverage cannot reference
			// objects that a separate publication has not yet made visible.
			trace.WithRegion(ctx, "history-pack-staging-wait", func() { job.err = include(ctx, records) })
			if job.err != nil {
				return
			}
		}
		metadata := newPublicationUploads(ctx, p.store)
		defer metadata.close()
		trace.WithRegion(ctx, "history-writer-lock", func() { p.writer.Lock() })
		defer p.writer.Unlock()
		if job.err = ctx.Err(); job.err != nil {
			return
		}
		job.err = p.publishIndex(ctx, metadata, func(idx *index) (pageRef, error) { return idx.updateSorted(ctx, records) })
	}()
	return job, nil
}

func (j *historyPublication) wait() error { <-j.done; return j.err }
func (j *historyPublication) abort()      { j.cancel(); j.uploads.cancel() }
