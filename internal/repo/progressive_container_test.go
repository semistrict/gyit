package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/klauspost/compress/zstd"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"

	"google.golang.org/protobuf/proto"
)

type directoryReadStore struct {
	store.Store
	gets atomic.Int64
}

func (s *directoryReadStore) Get(ctx context.Context, k string, o, n int64) ([]byte, string, error) {
	if len(k) >= 18 && k[:18] == "index/progressive-" {
		s.gets.Add(1)
	}
	return s.Store.Get(ctx, k, o, n)
}
func directoryContainerFixture(t testing.TB, count int) (*directoryReadStore, []pageRef) {
	t.Helper()
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	enc, err := newCompressor()
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	w := &indexWriter{ctx: context.Background(), store: local, prefix: "progressive-test", packLimit: 256 << 10}
	refs := make([]pageRef, count)
	for i := range refs {
		raw, err := proto.Marshal(&pb.ProgressiveDirectory{Entries: []*pb.ProgressiveEntry{{Name: []byte(fmt.Sprintf("file%04d", i)), Oid: fmt.Sprintf("%040x", i+1), Mode: 0100644, Size: int64(i)}}})
		if err != nil {
			t.Fatal(err)
		}
		refs[i], err = w.saveBytes(enc.EncodeAll(raw, nil))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = w.flush(); err != nil {
		t.Fatal(err)
	}
	return &directoryReadStore{Store: local}, refs
}
func directoryReader(t testing.TB, s store.Store, dir string, budget int64) *Progressive {
	t.Helper()
	disk, err := store.NewDiskCache(nil, dir, "directory-test", budget)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { disk.Close() })
	p, err := NewProgressive(context.Background(), s, disk, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func TestProgressiveDirectoryContainerReads(t *testing.T) {
	backend, refs := directoryContainerFixture(t, 40)
	cacheDir := filepath.Join(t.TempDir(), "cache")
	p := directoryReader(t, backend, cacheDir, 1<<20)
	for i, r := range refs {
		page, err := p.directoryPage(t.Context(), r)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Entries) != 1 || page.Entries[0].Size != int64(i) {
			t.Fatalf("wrong page %d: %v", i, page)
		}
	}
	if got := backend.gets.Load(); got != 1 {
		t.Fatalf("cold scan fetched %d ranges for one container; want one fetch", got)
	}
	if err := p.cache.disk.Close(); err != nil {
		t.Fatal(err)
	}
	p = directoryReader(t, backend, cacheDir, 1<<20)
	for _, r := range refs {
		if _, err := p.directoryPage(t.Context(), r); err != nil {
			t.Fatal(err)
		}
	}
	if got := backend.gets.Load(); got != 1 {
		t.Fatalf("reopened decoded cache fetched %d times", got)
	}
}
func TestProgressiveDirectoryContainerConcurrent(t *testing.T) {
	backend, refs := directoryContainerFixture(t, 80)
	p := directoryReader(t, backend, t.TempDir(), 1<<20)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, r := range refs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			page, err := p.directoryPage(t.Context(), r)
			if err != nil {
				t.Error(err)
				return
			}
			if page.Entries[0].Size != int64(i) {
				t.Errorf("wrong page %d", i)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := backend.gets.Load(); got != 1 {
		t.Fatalf("concurrent readers fetched container %d times; want one", got)
	}
}
func TestProgressiveDirectoryContainerTinyCache(t *testing.T) {
	for _, budget := range []int64{0, 4096} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			backend, refs := directoryContainerFixture(t, 10)
			p := directoryReader(t, backend, t.TempDir(), budget)
			for pass := 0; pass < 2; pass++ {
				for i, r := range refs {
					page, err := p.directoryPage(t.Context(), r)
					if err != nil {
						t.Fatal(err)
					}
					if page.Entries[0].Size != int64(i) {
						t.Fatal("eviction returned wrong page")
					}
				}
			}
		})
	}
}
func TestProgressiveDirectoryContainerCorruption(t *testing.T) {
	for _, damage := range []string{"hash", "truncated", "frame-header", "offset"} {
		t.Run(damage, func(t *testing.T) {
			backend, refs := directoryContainerFixture(t, 3)
			r := refs[1]
			b, _, err := backend.Store.Get(t.Context(), r.Pack, 0, -1)
			if err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "hash":
				r.Hash = fmt.Sprintf("%064x", 0)
			case "truncated":
				b = b[:int(r.Offset+r.Length)-1]
			case "frame-header":
				b[r.Offset] = 0
			case "offset":
				r.Offset++
			}
			if err = backend.Put(t.Context(), r.Pack, b, ""); err != nil {
				t.Fatal(err)
			}
			p := directoryReader(t, backend, t.TempDir(), 1<<20)
			if _, err = p.directoryPage(t.Context(), r); err == nil {
				t.Fatal("damaged page accepted")
			}
		})
	}
}
func TestProgressiveDirectoryContainerBadFrameDoesNotPoisonCache(t *testing.T) {
	backend, refs := directoryContainerFixture(t, 3)
	r := refs[0]
	b, _, err := backend.Store.Get(t.Context(), r.Pack, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	// A well-framed but corrupt page must never be installed under another page's hash.
	bad := append([]byte(nil), b...)
	bad[int(r.Length)-1] ^= 1
	if err = backend.Put(t.Context(), r.Pack, bad, ""); err != nil {
		t.Fatal(err)
	}
	p := directoryReader(t, backend, t.TempDir(), 1<<20)
	if _, err = p.directoryPage(t.Context(), r); err == nil {
		t.Fatal("checksum mismatch accepted")
	}
	if err = backend.Put(t.Context(), r.Pack, b, ""); err != nil {
		t.Fatal(err)
	}
	page, err := p.directoryPage(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	if page.Entries[0].Size != 0 {
		t.Fatal("incorrect recovered page")
	}
}

func TestProgressiveDirectoryFrameBoundaries(t *testing.T) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(true), zstd.WithZeroFrames(true))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	random := make([]byte, 256<<10)
	rand.New(rand.NewSource(1)).Read(random)
	for _, raw := range [][]byte{nil, []byte("hello"), bytes.Repeat([]byte("a"), 60000), random} {
		frame := enc.EncodeAll(raw, nil)
		n, err := directoryFrameLength(append(append([]byte(nil), frame...), frame...))
		if err != nil || n != len(frame) {
			t.Fatalf("frame length %d != %d: %v", n, len(frame), err)
		}
		for _, end := range []int{0, 1, 4, len(frame) - 1} {
			if _, err := directoryFrameLength(frame[:end]); err == nil {
				t.Fatalf("truncated frame accepted at %d", end)
			}
		}
	}
	// A reserved block type must be rejected before decoding.
	frame := enc.EncodeAll([]byte("hello"), nil)
	var h zstd.Header
	if err = h.Decode(frame); err != nil {
		t.Fatal(err)
	}
	frame[h.HeaderSize] |= 6
	if _, err = directoryFrameLength(frame); err == nil {
		t.Fatal("reserved block accepted")
	}
}

func TestProgressiveDirectoryContainerExpansionFallback(t *testing.T) {
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	enc, err := newCompressor()
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	w := &indexWriter{ctx: t.Context(), store: local, prefix: "progressive-expanded", packLimit: progressiveContainerBytes}
	var first pageRef
	for i := 0; i < 80; i++ {
		raw, err := proto.Marshal(&pb.ProgressiveDirectory{Entries: []*pb.ProgressiveEntry{{Name: bytes.Repeat([]byte("a"), 60000), Oid: fmt.Sprintf("%040x", i+1), Mode: 0100644, Size: int64(i)}}})
		if err != nil {
			t.Fatal(err)
		}
		ref, err := w.saveBytes(enc.EncodeAll(raw, nil))
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = ref
		}
	}
	if err = w.flush(); err != nil {
		t.Fatal(err)
	}
	p := directoryReader(t, local, t.TempDir(), 1<<20)
	page, err := p.directoryPage(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	if page.Entries[0].Oid != fmt.Sprintf("%040x", 1) {
		t.Fatal("fallback returned wrong page")
	}
}

func TestProgressiveDirectoryContainerCanceled(t *testing.T) {
	backend, refs := directoryContainerFixture(t, 2)
	p := directoryReader(t, backend, t.TempDir(), 1<<20)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := p.directoryPage(ctx, refs[0]); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if backend.gets.Load() != 0 {
		t.Fatal("canceled reader fetched container")
	}
}

func BenchmarkProgressiveColdDirectoryScan(b *testing.B) {
	backend, refs := directoryContainerFixture(b, 512)
	for b.Loop() {
		b.StopTimer()
		disk, err := store.NewDiskCache(nil, filepath.Join(b.TempDir(), "cache"), "benchmark", 8<<20)
		if err != nil {
			b.Fatal(err)
		}
		p, err := NewProgressive(context.Background(), backend, disk, b.TempDir())
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for _, ref := range refs {
			if _, err := p.directoryPage(context.Background(), ref); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		disk.Close()
		b.StartTimer()
	}
	b.ReportMetric(float64(backend.gets.Load())/float64(b.N), "container-gets/op")
}
