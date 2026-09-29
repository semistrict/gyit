package repo

import (
	"context"
	"fmt"

	"gyit/internal/store"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// publicationUploads overlaps immutable writes. HEAD must wait for all of them.
// Small metadata objects can overlap without reserving an eight-MiB buffer
// each. Both request count and total copied bytes stay bounded.
const publicationUploadRequests = 12
const publicationUploadBytes = 3 * indexPackSize

type publicationUploads struct {
	store.Store
	ctx     context.Context
	cancel  context.CancelFunc
	group   *errgroup.Group
	buffers *semaphore.Weighted
}

func newPublicationUploads(ctx context.Context, backend store.Store) *publicationUploads {
	ctx, cancel := context.WithCancel(ctx)
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(publicationUploadRequests)
	return &publicationUploads{Store: backend, ctx: ctx, cancel: cancel, group: group, buffers: semaphore.NewWeighted(publicationUploadBytes)}
}
func (u *publicationUploads) Put(_ context.Context, key string, data []byte, condition string) error {
	if condition != "" || key == "HEAD" {
		return fmt.Errorf("publication queue only accepts immutable objects")
	}
	if len(data) > indexPackSize {
		return fmt.Errorf("publication object exceeds upload buffer bound")
	}
	// errgroup records the storage failure as its cancellation cause. Returning
	// Err alone would hide that failure behind internal context cancellation.
	if err := context.Cause(u.ctx); err != nil {
		return err
	}
	if err := u.buffers.Acquire(u.ctx, int64(len(data))); err != nil {
		return context.Cause(u.ctx)
	}
	// Go acquires a concurrency slot before this copy, keeping memory bounded.
	// The caller's source remains valid until Go has started the worker; use a
	// handshake so Put never returns while the worker still borrows that source.
	copied := make(chan struct{})
	u.group.Go(func() error {
		defer u.buffers.Release(int64(len(data)))
		b := append([]byte(nil), data...)
		close(copied)
		return u.Store.Put(u.ctx, key, b, "")
	})
	<-copied
	return context.Cause(u.ctx)
}

func (u *publicationUploads) wait() error { return u.group.Wait() }
func (u *publicationUploads) close()      { u.cancel(); _ = u.group.Wait() }
