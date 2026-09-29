package repo

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestCacheCanceledOwnerDoesNotCancelWaitingReader(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, errReadAheadBusy} {
		t.Run(cause.Error(), func(t *testing.T) {
			c := newCache(1024)
			release := make(chan struct{})
			owner := c.flight.DoChan("object", func() (any, error) { <-release; return nil, cause })
			done := make(chan error, 1)
			go func() {
				b, err := c.load(t.Context(), "object", func() ([]byte, error) { return []byte("data"), nil })
				if err == nil && string(b) != "data" {
					err = errors.New("wrong cache data")
				}
				done <- err
			}()
			// Allow the reader to join the deliberately blocked flight. Shared below
			// verifies that this test actually exercised a canceled foreign loader.
			time.Sleep(20 * time.Millisecond)
			close(release)
			if !(<-owner).Shared {
				t.Fatal("reader did not join the blocked flight")
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("other reader inherited owner's cancellation: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("reader did not retry canceled shared fetch")
			}
		})
	}
}

func TestCanceledReadAheadKeepsBudgetUntilLoaderExits(t *testing.T) {
	c := newCache(1024)
	ctx, cancel := context.WithCancel(context.WithValue(t.Context(), cacheReadAheadKey{}, true))
	defer cancel()
	started, finished := make(chan struct{}, cacheReadAheadLimit), make(chan struct{}, cacheReadAheadLimit)
	release := make(chan struct{})
	defer close(release)
	for i := range cacheReadAheadLimit {
		go func() {
			_, _ = c.load(ctx, fmt.Sprint("old", i), func() ([]byte, error) { started <- struct{}{}; <-release; return nil, context.Canceled })
			finished <- struct{}{}
		}()
	}
	for range cacheReadAheadLimit {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("old read ahead did not start")
		}
	}
	cancel()
	for range cacheReadAheadLimit {
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatal("waiter did not cancel")
		}
	}
	nextCtx, nextCancel := context.WithCancel(context.WithValue(t.Context(), cacheReadAheadKey{}, true))
	defer nextCancel()
	nextStarted, nextDone := make(chan struct{}, 1), make(chan struct{}, 1)
	go func() {
		_, _ = c.load(nextCtx, "next", func() ([]byte, error) { nextStarted <- struct{}{}; return []byte("data"), nil })
		nextDone <- struct{}{}
	}()
	select {
	case <-nextStarted:
		t.Fatal("new read ahead exceeded the budget while old loaders were still running")
	case <-time.After(25 * time.Millisecond):
	}
	foreground, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	foregroundStarted := make(chan struct{}, 2)
	foregroundDone := make(chan error, 2)
	foregroundRelease := make(chan struct{})
	defer func() {
		close(foregroundRelease)
		for range 2 {
			<-foregroundDone
		}
	}()
	for i := range 2 {
		go func() {
			_, err := c.load(foreground, fmt.Sprint("foreground", i), func() ([]byte, error) {
				foregroundStarted <- struct{}{}
				<-foregroundRelease
				return []byte("data"), nil
			})
			foregroundDone <- err
		}()
	}
	for range 2 {
		select {
		case <-foregroundStarted:
		case <-foreground.Done():
			t.Fatal("speculation did not leave two foreground loads available")
		}
	}
	nextCancel()
	<-nextDone
}

func TestCacheDoesNotRetryOwnLoaderCancellation(t *testing.T) {
	c := newCache(1024)
	calls := 0
	_, err := c.load(t.Context(), "object", func() ([]byte, error) { calls++; return nil, context.Canceled })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("unexpected retry: err=%v calls=%d", err, calls)
	}
}
