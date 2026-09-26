package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// The real cache runs with a simulated, slow dependency. Explicit admission
// gates make the shared request and cancellation ordering reproducible;
// synctest supplies virtual time and proves all goroutines have settled.
func TestCacheSimulationSharedFlight(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("failed-read=%t", fail), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cache := newCache(4)
				cause := errors.New("simulated read outage")
				var calls atomic.Int32
				fetch := func() ([]byte, error) {
					attempt := calls.Add(1)
					time.Sleep(time.Hour)
					if fail && attempt == 1 {
						return nil, cause
					}
					return []byte("data"), nil
				}
				type result struct {
					body []byte
					err  error
				}
				start := func(ctx context.Context) <-chan result {
					done := make(chan result, 1)
					go func() {
						b, err := cache.load(ctx, "shared", fetch)
						done <- result{b, err}
					}()
					// Wait without advancing the clock: the next caller is
					// admitted only after this one joins the pending flight.
					synctest.Wait()
					return done
				}
				started := time.Now()
				leader := start(t.Context())
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				canceled := start(ctx)
				follower := start(t.Context())
				if calls.Load() != 1 {
					t.Fatalf("shared request started %d fetches", calls.Load())
				}
				cancel()
				synctest.Wait()
				select {
				case got := <-canceled:
					if !errors.Is(got.err, context.Canceled) || len(got.body) != 0 {
						t.Fatalf("canceled waiter: %+v", got)
					}
				default:
					t.Fatal("canceled waiter is still blocked")
				}
				for _, done := range []<-chan result{leader, follower} {
					select {
					case <-done:
						t.Fatal("canceling one waiter completed another before the read")
					default:
					}
				}
				time.Sleep(time.Hour)
				synctest.Wait()
				for _, done := range []<-chan result{leader, follower} {
					got := <-done
					if fail {
						if !errors.Is(got.err, cause) || len(got.body) != 0 {
							t.Fatalf("outage reply: %+v", got)
						}
					} else if got.err != nil || !bytes.Equal(got.body, []byte("data")) {
						t.Fatalf("successful reply: %+v", got)
					}
				}
				if elapsed := time.Since(started); elapsed != time.Hour {
					t.Fatalf("shared flight took %s of virtual time", elapsed)
				}
				// A failed flight must be retryable; a successful one is cached.
				got, err := cache.load(t.Context(), "shared", fetch)
				wantCalls := int32(1)
				if fail {
					wantCalls++
				}
				if err != nil || string(got) != "data" || calls.Load() != wantCalls {
					t.Fatalf("retry/cache hit: %q err=%v fetches=%d want=%d", got, err, calls.Load(), wantCalls)
				}
				synctest.Wait()
				if cache.used != 4 || len(cache.slots) != 0 {
					t.Fatalf("unsettled cache: retained=%d occupied slots=%d", cache.used, len(cache.slots))
				}
			})
		})
	}
}
