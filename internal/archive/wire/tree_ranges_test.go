package wire

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func rangeTreeChain(t testing.TB, count int) testSource {
	t.Helper()
	s := testChain(t, count, false)
	for i, raw := range s.raw {
		h := sha1.New()
		fmt.Fprintf(h, "tree %d\x00", len(raw))
		h.Write(raw)
		copy(s.r.Frames[i].OID[:], h.Sum(nil))
	}
	s.r.TargetOID = s.r.Frames[count-1].OID
	return s
}

func TestTreeRangesAdmission16And17(t *testing.T) {
	if RangeLimit != 16 || FetchConcurrency != 4 || FrameLimit != 64 || PackedLimit != 3<<20 || WorkLimit != 4<<20 || ObjectLimit != 1<<20 || ProgramLimit != 2<<20 || WireLimit != 8<<10 {
		t.Fatal("prototype changed an unrelated cap")
	}
	s := rangeTreeChain(t, 16)
	plan, err := Plan(s.r, -1)
	if err != nil || len(plan) != 16 {
		t.Fatalf("sixteen separated ranges must be admitted: %d %v", len(plan), err)
	}
	got, m, err := ReadObject(t.Context(), "tree", s.r, s.r.TargetOID, s.fetch, nil, nil)
	if err != nil || !bytes.Equal(got, s.raw[15]) || m.Fetches != 16 || m.VerifiedFrames != 16 {
		t.Fatalf("sixteen-range tree read: %+v %v", m, err)
	}
	s = rangeTreeChain(t, 17)
	if _, err := Plan(s.r, -1); !errors.Is(err, ErrLimit) {
		t.Fatalf("seventeen ranges must still fall back: %v", err)
	}
}

type rangeReadResult struct {
	got []byte
	m   Metrics
	err error
}

func waitStarted(t *testing.T, started <-chan int, wave int) {
	t.Helper()
	for range FetchConcurrency {
		select {
		case index := <-started:
			if index/FetchConcurrency != wave {
				t.Fatalf("started index%d during gated wave%d", index, wave)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("wave%d did not fill its four fetch workers", wave)
		}
	}
}

func TestTreeRangesFourFetchWorkersAndFullVerification(t *testing.T) {
	s := rangeTreeChain(t, 16)
	gates := [4]chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})}
	started := make(chan int, 16)
	var active, peak, exited, callbacks atomic.Int32
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan rangeReadResult, 1)
	go func() {
		got, m, err := ReadObject(ctx, "tree", s.r, s.r.TargetOID, func(ctx context.Context, segment uint64, offset, length uint32) ([]byte, error) {
			index := -1
			for i, f := range s.r.Frames {
				if f.Offset == segment*SegmentSize+uint64(offset) {
					index = i
					break
				}
			}
			if index < 0 {
				return nil, fmt.Errorf("unexpected range")
			}
			n := active.Add(1)
			for old := peak.Load(); old < n && !peak.CompareAndSwap(old, n); old = peak.Load() {
			}
			defer func() { active.Add(-1); exited.Add(1) }()
			started <- index
			select {
			case <-gates[index/FetchConcurrency]:
				return s.fetch(ctx, segment, offset, length)
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}, nil, func(int, []byte) { callbacks.Add(1) })
		result <- rangeReadResult{got, m, err}
	}()
	for wave := range gates {
		waitStarted(t, started, wave)
		if active.Load() != 4 || peak.Load() != 4 || callbacks.Load() != 0 {
			t.Fatalf("unbounded/premature activity: active%d peak%d callbacks%d", active.Load(), peak.Load(), callbacks.Load())
		}
		close(gates[wave])
	}
	select {
	case r := <-result:
		if r.err != nil || !bytes.Equal(r.got, s.raw[15]) || r.m.Fetches != 16 || peak.Load() != 4 || active.Load() != 0 || exited.Load() != 16 || callbacks.Load() != 16 {
			t.Fatalf("result=%+v active%d peak%d exits%d callbacks%d", r, active.Load(), peak.Load(), exited.Load(), callbacks.Load())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read did not join")
	}
}

func TestTreeRangesLaterWaveCancellationJoins(t *testing.T) {
	s := rangeTreeChain(t, 16)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{}, 4)
	var active, exited, calls, callbacks atomic.Int32
	result := make(chan rangeReadResult, 1)
	go func() {
		got, m, err := ReadObject(ctx, "tree", s.r, s.r.TargetOID, func(ctx context.Context, segment uint64, offset, length uint32) ([]byte, error) {
			n := calls.Add(1)
			active.Add(1)
			defer func() { active.Add(-1); exited.Add(1) }()
			if n <= 4 {
				return s.fetch(ctx, segment, offset, length)
			}
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}, nil, func(int, []byte) { callbacks.Add(1) })
		result <- rangeReadResult{got, m, err}
	}()
	for range 4 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("later wave did not start")
		}
	}
	cancel()
	select {
	case r := <-result:
		if !errors.Is(r.err, context.Canceled) || r.got != nil || callbacks.Load() != 0 || active.Load() != 0 || calls.Load() != 8 || exited.Load() != 8 || r.m.Fetches != 8 {
			t.Fatalf("failed read leaked bytes/workers/cache writes: result=%+v calls%d exits%d active%d callbacks%d", r, calls.Load(), exited.Load(), active.Load(), callbacks.Load())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled fetches were not joined")
	}
}

func TestTreeRangesFetchFailureStopsQueuedWork(t *testing.T) {
	s := rangeTreeChain(t, 16)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{}, 4)
	fail := make(chan struct{})
	cause := errors.New("first wave fetch failure")
	var calls, exited, callbacks atomic.Int32
	result := make(chan rangeReadResult, 1)
	go func() {
		got, m, err := ReadObject(ctx, "tree", s.r, s.r.TargetOID, func(ctx context.Context, segment uint64, offset, length uint32) ([]byte, error) {
			calls.Add(1)
			defer exited.Add(1)
			started <- struct{}{}
			if segment*SegmentSize+uint64(offset) == s.r.Frames[0].Offset {
				select {
				case <-fail:
					return nil, cause
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}, nil, func(int, []byte) { callbacks.Add(1) })
		result <- rangeReadResult{got, m, err}
	}()
	for range 4 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("first wave did not start")
		}
	}
	close(fail)
	select {
	case r := <-result:
		var wantedBytes uint64
		for _, f := range s.r.Frames[:4] {
			wantedBytes += uint64(f.Length)
		}
		if !errors.Is(r.err, cause) || r.got != nil || callbacks.Load() != 0 || calls.Load() != 4 || exited.Load() != 4 || r.m.Fetches != 4 || r.m.FetchedBytes != wantedBytes {
			t.Fatalf("queued work started or metrics were planned rather than actual: result=%+v calls%d exits%d callbacks%d", r, calls.Load(), exited.Load(), callbacks.Load())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("failed fetch workers were not joined")
	}
}

func TestTreeRangesLateFailureKeepsCacheEmpty(t *testing.T) {
	for _, mode := range []string{"short", "fetch_error", "corrupt", "late_identity"} {
		t.Run(mode, func(t *testing.T) {
			s := rangeTreeChain(t, 16)
			last := s.r.Frames[15]
			cause := errors.New("late fetch failure")
			var active, callbacks atomic.Int32
			if mode == "corrupt" {
				s.packed[15] = bytes.Clone(s.packed[15])
				s.packed[15][len(s.packed[15])/2] ^= 128
			}
			if mode == "late_identity" {
				s.r.Frames[15].OID[0] ^= 1
				s.r.TargetOID = s.r.Frames[15].OID
			}
			got, _, err := ReadObject(t.Context(), "tree", s.r, s.r.TargetOID, func(ctx context.Context, segment uint64, offset, length uint32) ([]byte, error) {
				active.Add(1)
				defer active.Add(-1)
				if segment*SegmentSize+uint64(offset) == last.Offset {
					if mode == "short" {
						return make([]byte, length-1), nil
					}
					if mode == "fetch_error" {
						return nil, cause
					}
				}
				return s.fetch(ctx, segment, offset, length)
			}, nil, func(int, []byte) { callbacks.Add(1) })
			if err == nil || got != nil || active.Load() != 0 || callbacks.Load() != 0 {
				t.Fatalf("failed read exposed bytes/cache updates: got%d active%d callbacks%d err%v", len(got), active.Load(), callbacks.Load(), err)
			}
			if mode == "fetch_error" && !errors.Is(err, cause) {
				t.Fatalf("causal fetch failure lost: %v", err)
			}
		})
	}
}

// BenchmarkTreeRangesModeled20ms is a deterministic injected-latency exercise,
// not an object-store, mounted-filesystem, or historical-repository benchmark.
// Data creation is excluded; each iteration starts without a verified base.
func BenchmarkTreeRangesModeled20ms(b *testing.B) {
	for _, ranges := range []int{4, 8, 16} {
		b.Run(fmt.Sprintf("ranges%d", ranges), func(b *testing.B) {
			s := rangeTreeChain(b, ranges)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, m, err := ReadObject(b.Context(), "tree", s.r, s.r.TargetOID, func(ctx context.Context, segment uint64, offset, length uint32) ([]byte, error) {
					timer := time.NewTimer(20 * time.Millisecond)
					defer timer.Stop()
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-timer.C:
						return s.fetch(ctx, segment, offset, length)
					}
				}, nil, nil)
				if err != nil || m.Fetches != ranges {
					b.Fatalf("modeled read: %+v %v", m, err)
				}
			}
			b.ReportMetric(float64(ranges), "GETs/op")
		})
	}
}
