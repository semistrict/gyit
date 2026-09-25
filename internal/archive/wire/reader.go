package wire

import (
	"context"
	"crypto/sha1"
	"fmt"
	"sync"
	"sync/atomic"

	wirecodec "gat/internal/packcodec"
)

// Fetch must honor cancellation and return exactly length bounded bytes.
// Calls for an individual Read are concurrent, with at most four calls.
type Fetch func(ctx context.Context, segment uint64, offset, length uint32) ([]byte, error)
type VerifiedBase struct {
	Ordinal int
	Raw     []byte
}
type Metrics struct {
	Fetches, VerifiedFrames                     int
	FetchedBytes, DecodedWork, CopiedFrameBytes uint64
}

// Read fetches the complete known suffix in parallel, then applies its deltas
// locally. Cached bases are rechecked against the expected frame identity.
// onVerified runs only after the final target passed authentication; its byte
// slices and the return value are immutable and may share storage.
func Read(ctx context.Context, r Recipe, expectedTarget [20]byte, fetch Fetch, base *VerifiedBase, onVerified func(int, []byte)) ([]byte, Metrics, error) {
	return ReadObject(ctx, "blob", r, expectedTarget, fetch, base, onVerified)
}

// ReadObject uses the object kind supplied by the authenticated containing
// catalog record. The same bytes must never authenticate across kind namespaces.
func ReadObject(ctx context.Context, kind string, r Recipe, expectedTarget [20]byte, fetch Fetch, base *VerifiedBase, onVerified func(int, []byte)) ([]byte, Metrics, error) {
	var stats Metrics
	if kind != "blob" && kind != "tree" {
		return nil, stats, ErrMalformed
	}
	if err := Validate(r); err != nil {
		return nil, stats, err
	}
	if r.TargetOID != expectedTarget || fetch == nil {
		return nil, stats, ErrMalformed
	}
	if err := ctx.Err(); err != nil {
		return nil, stats, err
	}
	after := -1
	var raw []byte
	if base != nil {
		if base.Ordinal < 0 || base.Ordinal >= len(r.Frames) {
			return nil, stats, ErrMalformed
		}
		after = base.Ordinal
		if err := verifyObject(kind, base.Raw, r.Frames[after]); err != nil {
			return nil, stats, err
		}
		raw = base.Raw
	}
	ranges, err := Plan(r, after)
	if err != nil {
		return nil, stats, err
	}
	packed := make([][]byte, len(ranges))
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var once sync.Once
	var fetchErr error

	// Recipe fanout and active HTTP work are separate bounds. Fixed workers
	// consume the known plan; at most four Fetch calls run for this decode.
	// Cancellation stops dequeueing and Wait joins every in-flight call.
	var next, attempted atomic.Int32
	var attemptedBytes atomic.Uint64
	for range min(FetchConcurrency, len(ranges)) {
		wg.Go(func() {
			for {
				if fetchCtx.Err() != nil {
					return
				}
				i := int(next.Add(1)) - 1
				if i >= len(ranges) {
					return
				}
				span := ranges[i]
				attempted.Add(1)
				attemptedBytes.Add(uint64(span.Length))
				b, err := fetch(fetchCtx, span.Segment, span.Offset, span.Length)
				if err == nil && len(b) != int(span.Length) {
					err = fmt.Errorf("archive short range: %w", ErrMalformed)
				}
				if err != nil {
					once.Do(func() { fetchErr = err; cancel() })
					return
				}
				packed[i] = b
			}
		})
	}
	wg.Wait()
	stats.Fetches = int(attempted.Load())
	stats.FetchedBytes = attemptedBytes.Load()
	if fetchErr != nil {
		return nil, stats, fetchErr
	}
	if err := ctx.Err(); err != nil {
		return nil, stats, err
	}
	verified := make([][]byte, len(r.Frames))
	for i := after + 1; i < len(r.Frames); i++ {
		if err := ctx.Err(); err != nil {
			return nil, stats, err
		}
		f := r.Frames[i]
		compressed, copied, err := frameBytes(f, ranges, packed)
		if err != nil {
			return nil, stats, err
		}
		stats.CopiedFrameBytes += copied
		decoded := make([]byte, int(f.RawSize))
		if err := wirecodec.Inflate(decoded, compressed); err != nil {
			return nil, stats, fmt.Errorf("archive zlib frame: %w", err)
		}
		stats.DecodedWork += uint64(f.RawSize)
		if i > 0 {
			decoded, err = applyRaw(ctx, raw, decoded, f.Size)
			if err != nil {
				return nil, stats, err
			}
			stats.DecodedWork += uint64(f.Size)
		}
		if err := verifyObject(kind, decoded, f); err != nil {
			return nil, stats, err
		}
		verified[i] = decoded
		stats.VerifiedFrames++
		raw = decoded
	}
	if err := ctx.Err(); err != nil {
		return nil, stats, err
	}
	if onVerified != nil {
		for i := after + 1; i < len(verified); i++ {
			onVerified(i, verified[i])
		}
	}
	return raw, stats, nil
}

func verifyObject(kind string, raw []byte, f Frame) error {
	if len(raw) != int(f.Size) {
		return fmt.Errorf("archive blob size: %w", ErrMalformed)
	}
	h := sha1.New()
	fmt.Fprintf(h, "%s %d\x00", kind, len(raw))
	h.Write(raw)
	var oid [20]byte
	copy(oid[:], h.Sum(nil))
	if oid != f.OID {
		return fmt.Errorf("archive blob identity: %w", ErrMalformed)
	}
	return nil
}

func frameBytes(f Frame, ranges []Range, packed [][]byte) ([]byte, uint64, error) {
	start, end := f.Offset, f.Offset+uint64(f.Length)
	var pieces [][]byte
	for i, r := range ranges {
		a := r.Segment*SegmentSize + uint64(r.Offset)
		b := a + uint64(r.Length)
		if start >= b || end <= a {
			continue
		}
		lo, hi := max(start, a), min(end, b)
		pieces = append(pieces, packed[i][lo-a:hi-a])
	}
	if len(pieces) == 1 && len(pieces[0]) == int(f.Length) {
		return pieces[0], 0, nil
	}
	if len(pieces) == 0 || len(pieces) > 2 {
		return nil, 0, ErrMalformed
	}
	result := make([]byte, 0, int(f.Length))
	for _, p := range pieces {
		if len(result)+len(p) > int(f.Length) {
			return nil, 0, ErrMalformed
		}
		result = append(result, p...)
	}
	if len(result) != int(f.Length) {
		return nil, 0, ErrMalformed
	}
	return result, uint64(len(result)), nil
}
