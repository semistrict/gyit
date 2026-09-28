package repo

import (
	"context"
	"fmt"

	"gyit/internal/store"

	"golang.org/x/sync/errgroup"
)

// publicationUploads overlaps immutable writes. HEAD must wait for all of them.
// At most three eight-MiB buffers are retained, independent of repository size.
type publicationUploads struct {
	store.Store
	ctx    context.Context
	cancel context.CancelFunc
	group  *errgroup.Group
}

func newPublicationUploads(ctx context.Context, backend store.Store) *publicationUploads {
	ctx, cancel := context.WithCancel(ctx)
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(3)
	return &publicationUploads{Store: backend, ctx: ctx, cancel: cancel, group: group}
}
func (u *publicationUploads) Put(_ context.Context, key string, data []byte, condition string) error {
	if condition != "" || key == "HEAD" {
		return fmt.Errorf("publication queue only accepts immutable objects")
	}
	if len(data) > indexPackSize {
		return fmt.Errorf("publication object exceeds upload buffer bound")
	}
	if err := u.ctx.Err(); err != nil {
		return err
	}
	// Go acquires a concurrency slot before this copy, keeping memory bounded.
	// The caller's source remains valid until Go has started the worker; use a
	// handshake so Put never returns while the worker still borrows that source.
	copied := make(chan struct{})
	u.group.Go(func() error {
		b := append([]byte(nil), data...)
		close(copied)
		return u.Store.Put(u.ctx, key, b, "")
	})
	<-copied
	return u.ctx.Err()
}
func (u *publicationUploads) wait() error { return u.group.Wait() }
func (u *publicationUploads) close()      { u.cancel(); _ = u.group.Wait() }
