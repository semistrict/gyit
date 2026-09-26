package repo

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"gyit/internal/packfile"
	"gyit/internal/gitdelta"
	"gyit/internal/spill"
	"gyit/internal/store"
)

type earlyNativeCase struct {
	oid, hint string
	size      int64
	out       packrecipe.Output
	err       error
	calls     atomic.Int64
}
type earlyNativeFake struct {
	ctx     context.Context
	cases   map[string]*earlyNativeCase
	closed  atomic.Int64
	started chan struct{}
	block   bool
}

func (f *earlyNativeFake) Has(oid string) bool { return f.cases[oid] != nil }
func (f *earlyNativeFake) Convert(oid string, _ []byte) (packrecipe.Output, error) {
	c := f.cases[oid]
	c.calls.Add(1)
	if f.started != nil {
		select {
		case f.started <- struct{}{}:
		default:
		}
	}
	if f.block {
		<-f.ctx.Done()
		return packrecipe.Output{}, f.ctx.Err()
	}
	return c.out, c.err
}
func (f *earlyNativeFake) Close() { f.closed.Add(1) }

func makeEarlyNativeCase(t *testing.T, n int) *earlyNativeCase {
	t.Helper()
	body := []byte(fmt.Sprintf("synthetic body %04d\n", n))
	h := sha1.New()
	fmt.Fprintf(h, "blob %d\x00", len(body))
	h.Write(body)
	var packed bytes.Buffer
	z := zlib.NewWriter(&packed)
	if _, err := z.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	return &earlyNativeCase{oid: fmt.Sprintf("%x", h.Sum(nil)), hint: fmt.Sprintf("path/%d\nwith-newline", n), size: int64(len(body)), out: packrecipe.Output{Data: packed.Bytes(), Hash: fmt.Sprintf("%x", sha256.Sum256(body)), Size: len(body), Full: true}}
}

func earlyNativeTestStage(t *testing.T) (*stage, store.Store, string) {
	t.Helper()
	tmp := t.TempDir()
	records, err := spill.New(tmp, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { records.Close() })
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &stage{tmp: tmp, records: records}, backend, tmp
}

func TestEarlyNativeOwnershipReplayAndJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	st, backend, tmp := earlyNativeTestStage(t)
	cases := make(map[string]*earlyNativeCase)
	var jobs []*earlyNativeCase
	for i := 0; i < 260; i++ {
		c := makeEarlyNativeCase(t, i)
		cases[c.oid] = c
		jobs = append(jobs, c)
	}
	reject := jobs[17]
	reject.err = gitdelta.ErrLimit
	fake := &earlyNativeFake{ctx: ctx, cases: cases, started: make(chan struct{}, 1)}
	p, err := startEarlyNativeReaders(ctx, cancel, tmp, 1, st, backend, func(int) (earlyNativeReader, error) { return fake, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer p.finish(nil)
	for _, c := range jobs {
		owned, err := p.tryAdd(c.oid, c.hint, c.size)
		if err != nil || !owned {
			t.Fatalf("ownership %v %v", owned, err)
		}
	}
	// A full queue batch starts conversion while the producer is still open.
	select {
	case <-fake.started:
	case <-ctx.Done():
		t.Fatal("worker did not overlap producer")
	}
	if p.sealed.Load() {
		t.Fatal("test expected live producer")
	}
	var rejected []string
	if err := p.replayRejected(func(oid, hint string, size int64) error {
		if oid != reject.oid || hint != reject.hint || size != reject.size {
			t.Fatal("fallback metadata changed")
		}
		rejected = append(rejected, oid)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(rejected) != 1 {
		t.Fatalf("rejection count %d", len(rejected))
	}
	if err := p.replayRejected(func(string, string, int64) error { t.Fatal("replayed twice"); return nil }); err != nil {
		t.Fatal(err)
	}
	var stats Stats
	if err := p.finish(&stats); err != nil {
		t.Fatal(err)
	}
	once := stats
	if err := p.finish(&stats); err != nil {
		t.Fatal(err)
	}
	if stats != once || stats.Objects != 259 || stats.Blobs != 259 || stats.Chunks != 259 {
		t.Fatalf("stats double/missing count: %+v", stats)
	}
	if fake.closed.Load() != 1 {
		t.Fatalf("reader closed %d times", fake.closed.Load())
	}
	for _, c := range jobs {
		if c.calls.Load() != 1 {
			t.Fatalf("conversion count=%d", c.calls.Load())
		}
	}
	idx := &index{store: backend, cache: newCache(1 << 20)}
	idx.root, err = idx.updateSorted(ctx, st.records)
	if err != nil {
		t.Fatal(err)
	}
	var obj object
	if err := idx.get(ctx, "o/"+reject.oid, &obj); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rejected object staged: %v", err)
	}
	if err := idx.get(ctx, "o/"+jobs[0].oid, &obj); err != nil || obj.Size != jobs[0].size {
		t.Fatalf("accepted object %v %+v", err, obj)
	}
	if p.rootFrames.Load() != 0 || p.targetFrames.Load() != 259 || p.targetBytes.Load() == 0 {
		t.Fatal("payload accounting")
	}
	entries, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			t.Fatalf("queue file remained after finish: %s", e.Name())
		}
	}
}

func TestEarlyNativeFailureCancelsAndCannotReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	st, backend, tmp := earlyNativeTestStage(t)
	c := makeEarlyNativeCase(t, 0)
	want := errors.New("native target checksum mismatch")
	c.err = want
	fake := &earlyNativeFake{ctx: ctx, cases: map[string]*earlyNativeCase{c.oid: c}}
	p, err := startEarlyNativeReaders(ctx, cancel, tmp, 1, st, backend, func(int) (earlyNativeReader, error) { return fake, nil })
	if err != nil {
		t.Fatal(err)
	}
	if owned, err := p.tryAdd(c.oid, c.hint, c.size); err != nil || !owned {
		t.Fatal(err)
	}
	if err := p.replayRejected(func(string, string, int64) error { t.Fatal("corruption treated as unsupported"); return nil }); !errors.Is(err, want) {
		t.Fatalf("causal error %v", err)
	}
	if err := p.finish(nil); !errors.Is(err, want) {
		t.Fatalf("finish causal error %v", err)
	}
	if fake.closed.Load() != 1 || ctx.Err() == nil {
		t.Fatal("worker was not canceled/joined/disposed")
	}
}

func TestEarlyNativeCancellationAndConstructorCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	st, backend, tmp := earlyNativeTestStage(t)
	c := makeEarlyNativeCase(t, 1)
	fake := &earlyNativeFake{ctx: ctx, cases: map[string]*earlyNativeCase{c.oid: c}, started: make(chan struct{}, 1), block: true}
	p, err := startEarlyNativeReaders(ctx, cancel, tmp, 1, st, backend, func(int) (earlyNativeReader, error) { return fake, nil })
	if err != nil {
		t.Fatal(err)
	}
	if owned, err := p.tryAdd(c.oid, c.hint, c.size); err != nil || !owned {
		t.Fatal(err)
	}
	if err := p.seal(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-fake.started:
	case <-time.After(time.Second):
		cancel()
		p.finish(nil)
		t.Fatal("worker did not start")
	}
	cancel()
	if err := p.finish(nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error %v", err)
	}
	if fake.closed.Load() != 1 {
		t.Fatal("canceled reader not closed")
	}
	want := errors.New("second reader unavailable")
	first := &earlyNativeFake{ctx: t.Context()}
	if _, err := startEarlyNativeReaders(t.Context(), func() {}, tmp, 2, st, backend, func(n int) (earlyNativeReader, error) {
		if n == 1 {
			// A failed concrete constructor can become a nonnil interface holding
			// a nil pointer; cleanup must not call Close on that failed value.
			return (*packrecipe.Reader)(nil), want
		}
		return first, nil
	}); !errors.Is(err, want) {
		t.Fatalf("constructor error %v", err)
	}
	if first.closed.Load() != 1 {
		t.Fatal("partial constructor leaked reader")
	}
}

func TestEarlyNativeDisabledAndUnreplayedGuard(t *testing.T) {
	st, backend, tmp := earlyNativeTestStage(t)
	p, err := startEarlyNative(t.Context(), func() {}, "unused", "sha1", tmp, 1, st, backend)
	if err != nil {
		t.Fatal(err)
	}
	if owned, err := p.tryAdd("bad", "file", 5); owned || err != nil {
		t.Fatalf("disabled pool %v %v", owned, err)
	}
	if err := p.finish(&Stats{}); err != nil {
		t.Fatal(err)
	}
	c := makeEarlyNativeCase(t, 2)
	c.err = gitdelta.ErrLimit
	fake := &earlyNativeFake{ctx: t.Context(), cases: map[string]*earlyNativeCase{c.oid: c}}
	p, err = startEarlyNativeReaders(t.Context(), func() {}, tmp, 1, st, backend, func(int) (earlyNativeReader, error) { return fake, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.tryAdd(c.oid, c.hint, c.size); err != nil {
		t.Fatal(err)
	}
	if err := p.finish(&Stats{}); err == nil {
		t.Fatal("publication could lose unreplayed fallback")
	}
}
