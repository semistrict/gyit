//go:build !js

package repo

import (
	"context"
	"errors"
	"testing"

	"gyit/internal/store"
)

type failingPublicationStore struct {
	store.Store
	err error
}

func (s failingPublicationStore) Put(context.Context, string, []byte, string) error { return s.err }

func TestPublicationUploadsPreserveStorageFailure(t *testing.T) {
	failure := errors.New("storage upload failed")
	u := newPublicationUploads(t.Context(), failingPublicationStore{err: failure})
	defer u.close()
	if err := u.Put(t.Context(), "index/first", []byte("first"), ""); err != nil && !errors.Is(err, failure) {
		t.Fatalf("first failure replaced: %v", err)
	}
	// The worker's failure cancels its group. Later producers must see that
	// storage error, rather than mistaking internal cancellation for user input.
	<-u.ctx.Done()
	if err := u.Put(t.Context(), "index/next", []byte("next"), ""); !errors.Is(err, failure) {
		t.Fatalf("subsequent upload hid storage failure: %v", err)
	}
	if err := u.wait(); !errors.Is(err, failure) {
		t.Fatal(err)
	}
}

func TestPublicationUploadsPreserveCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	u := newPublicationUploads(ctx, failingPublicationStore{err: errors.New("should not upload")})
	defer u.close()
	cancel()
	if err := u.Put(ctx, "index/first", nil, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation: %v", err)
	}
}
