package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"
)

func TestSpeculativeEncodingPreservesTransitions(t *testing.T) {
	for _, depth := range []int{1, 2} {
		for _, candidates := range []int{1, 4} {
			t.Run(fmt.Sprintf("depth%d-candidates%d", depth, candidates), func(t *testing.T) {
				pool, e := newSpeculativePool(4)
				if e != nil {
					t.Fatal(e)
				}
				defer pool.close()
				a, e := newCompressor()
				if e != nil {
					t.Fatal(e)
				}
				defer a.Close()
				b, e := newCompressor()
				if e != nil {
					t.Fatal(e)
				}
				defer b.Close()
				reference := &chunkCodec{encoder: a, bases: newBaseCache(candidates), depth: depth}
				actual := &chunkCodec{encoder: b, bases: newBaseCache(candidates), depth: depth}
				batch := &speculativeBatch{pool: pool, codec: actual}
				describe := func(s *encodeSlot) string {
					if s.err != nil {
						t.Fatal(s.err)
					}
					v := fmt.Sprintf("%s:%s:%x", s.key, s.hash, sha256.Sum256(s.compressed))
					if s.base != nil {
						v += fmt.Sprintf(":base%d:%x", s.base.depth, sha256.Sum256(s.base.raw))
					}
					if s.anchor != nil {
						v += fmt.Sprintf(":anchor%d:%x", s.anchor.depth, sha256.Sum256(s.anchor.raw))
					}
					for _, a := range s.anchors {
						v += fmt.Sprintf(":choice%d:%x", a.depth, sha256.Sum256(a.raw))
					}
					return v
				}
				var want, got []string
				sink := func(s *encodeSlot) error { got = append(got, describe(s)); return nil }
				rng := rand.New(rand.NewPCG(331, 519))
				raw := make([]byte, 128<<10)
				for i := 0; i < 80; i++ {
					// Stable deltas activate the pool, then unrelated bodies introduce a new
					// anchor in the middle of a speculative batch and invalidate later jobs.
					if i == 0 || i == 14 || i == 39 || i == 62 {
						for j := range raw {
							raw[j] = byte(rng.Uint64())
						}
					} else {
						raw[(i*103)%len(raw)] ^= byte(i)
					}
					hint := "same-path"
					if i == 52 {
						hint = "other-path"
					}
					key := fmt.Sprint(i)
					expected := encodeSlot{key: key, hint: hint, raw: raw}
					reference.encode(&expected)
					want = append(want, describe(&expected))
					input := bytes.Clone(raw)
					slot := encodeSlot{key: key, hint: hint, raw: input}
					if e = batch.add(t.Context(), &slot, sink); e != nil {
						t.Fatal(e)
					}
					// Queued jobs must own their source bytes after add returns.
					clear(input)
				}
				if e = batch.flush(t.Context(), sink); e != nil {
					t.Fatal(e)
				}
				if !reflect.DeepEqual(want, got) {
					for i := range want {
						if i >= len(got) || want[i] != got[i] {
							t.Fatalf("different encoding/state at job%d", i)
						}
					}
				}
			})
		}
	}
}

func TestSpeculativePreCanceledBatchDoesNotReachSink(t *testing.T) {
	pool, e := newSpeculativePool(2)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.close()
	encoder, e := newCompressor()
	if e != nil {
		t.Fatal(e)
	}
	defer encoder.Close()
	codec := &chunkCodec{encoder: encoder, bases: newBaseCache(4), depth: 1}
	batch := &speculativeBatch{pool: pool, codec: codec}
	raw := make([]byte, 128<<10)
	rng := rand.New(rand.NewPCG(733, 881))
	for i := range raw {
		raw[i] = byte(rng.Uint64())
	}
	sink := func(*encodeSlot) error { return nil }
	for i := 0; i < 12; i++ {
		raw[i] ^= byte(i)
		s := encodeSlot{hint: "path", raw: raw}
		if e = batch.add(t.Context(), &s, sink); e != nil {
			t.Fatal(e)
		}
	}
	if len(batch.pending) == 0 {
		t.Fatal("fixture did not queue speculative work")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if e = batch.flush(ctx, func(*encodeSlot) error { t.Fatal("canceled batch reached sink"); return nil }); e != context.Canceled {
		t.Fatalf("cancellation: %v", e)
	}
}

// Both jobs are submitted before the first result reaches the sink. Hold the
// second result until we have observed the first sink failure/cancellation.
func TestSpeculativeFailureJoinsInFlightJobs(t *testing.T) {
	for _, cancellation := range []bool{false, true} {
		t.Run(fmt.Sprint(cancellation), func(t *testing.T) {
			encoder, err := newCompressor()
			if err != nil {
				t.Fatal(err)
			}
			defer encoder.Close()
			codec := &chunkCodec{encoder: encoder, bases: newBaseCache(4), depth: 1}
			jobs := []*speculativeJob{
				{slot: encodeSlot{hint: "path"}, done: make(chan struct{})},
				{slot: encodeSlot{hint: "path"}, done: make(chan struct{})},
			}
			pool := &speculativePool{jobs: make(chan *speculativeJob, 2)}
			batch := &speculativeBatch{pool: pool, codec: codec, pending: jobs}
			release := make(chan struct{})
			workerDone := make(chan struct{})
			go func() {
				defer close(workerDone)
				first := <-pool.jobs
				close(first.done)
				second := <-pool.jobs
				<-release
				close(second.done)
			}()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			marker := errors.New("sink failure")
			sinkEntered := make(chan struct{})
			result := make(chan error, 1)
			count := 0
			go func() {
				result <- batch.flush(ctx, func(*encodeSlot) error {
					count++
					close(sinkEntered)
					if cancellation {
						cancel()
						return nil
					}
					return marker
				})
			}()
			select {
			case <-sinkEntered:
			case <-time.After(5 * time.Second):
				close(release)
				t.Fatal("sink never reached")
			}
			// Returning before the outstanding job is joined would permit caller-owned
			// staging/cache state to be destroyed while a worker still references it.
			premature := false
			select {
			case <-result:
				premature = true
			case <-time.After(20 * time.Millisecond):
			}
			close(release)
			<-workerDone
			if premature {
				t.Fatal("flush returned before outstanding compression joined")
			}
			select {
			case err = <-result:
			case <-time.After(5 * time.Second):
				t.Fatal("flush did not join released work")
			}
			want := marker
			if cancellation {
				want = context.Canceled
			}
			if !errors.Is(err, want) || count != 1 {
				t.Fatalf("error=%v sink calls=%d", err, count)
			}
			for _, j := range jobs {
				select {
				case <-j.done:
				default:
					t.Fatal("unjoined job")
				}
			}
		})
	}
}
