package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	storagev1 "gat/internal/gen/gat/storage/v1"
	"gat/internal/store"
	"google.golang.org/protobuf/proto"
)

func TestDeltaReconstructsEditsAndRejectsInvalidCopies(t *testing.T) {
	raw := make([]byte, ChunkSize)
	rand.New(rand.NewSource(72)).Read(raw)
	base := newDeltaBase(raw)
	variants := [][]byte{raw, append([]byte("inserted header"), raw[:len(raw)-len("inserted header")]...), append(bytes.Clone(raw[:500]), raw[1200:]...), []byte("unrelated"), bytes.Repeat([]byte("x"), ChunkSize)}
	for i, v := range variants {
		wire := base.delta(v)
		var message storagev1.ChunkDelta
		if err := proto.Unmarshal(wire, &message); err != nil {
			t.Fatal(err)
		}
		canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(&message)
		if err != nil || !bytes.Equal(wire, canonical) {
			t.Fatalf("variant %d differs from canonical protobuf", i)
		}
		decoded, err := applyDelta(raw, wire)
		if err != nil || !bytes.Equal(decoded, v) {
			t.Fatalf("variant %d: %v", i, err)
		}
	}
	for _, d := range []*storagev1.ChunkDelta{
		{Size: ChunkSize + 1},
		{Size: 1, Operations: []*storagev1.DeltaOperation{{Offset: ChunkSize, Length: 1}}},
		{Size: 1, Operations: []*storagev1.DeltaOperation{{Length: 2}}},
		{Size: 1, Operations: []*storagev1.DeltaOperation{{Length: 1, Literal: []byte("a")}}},
		{Size: 2, Operations: []*storagev1.DeltaOperation{{Literal: []byte("a")}}},
	} {
		wire, _ := proto.Marshal(d)
		if _, err := applyDelta(raw, wire); err == nil {
			t.Fatal("accepted invalid delta")
		}
	}
}

func TestDeltaImportsReuseFullBasesAcrossPublications(t *testing.T) {
	ctx := t.Context()
	source := t.TempDir()
	command(t, source, "init", "-q")
	content := make([]byte, 2*ChunkSize+8192)
	rand.New(rand.NewSource(91)).Read(content)
	write(t, source, "nested/file", content)
	first := commit(t, source)
	backend, _ := store.NewLocal(t.TempDir())
	if _, err := Import(ctx, backend, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	measured := &countedStore{Store: backend}
	r, _ := New(measured, 2*ChunkSize)
	old, err := r.Open(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	original, err := old.Resolve(ctx, "nested/file")
	if err != nil {
		t.Fatal(err)
	}
	for version := 0; version < 3; version++ {
		content[ChunkSize-10+version] = 'Z'
		content[ChunkSize+10+version] = 'Q'
		write(t, source, "nested/file", content)
		sha := commit(t, source)
		stats, err := Import(ctx, backend, ImportOptions{Repo: source})
		if err != nil {
			t.Fatal(err)
		}
		if stats.DeltaChunks != 3 || stats.UploadedBytes > 2048 {
			t.Fatalf("incremental update lost delta savings: %+v", stats)
		}
		snapshot, err := r.Open(ctx, sha)
		if err != nil {
			t.Fatal(err)
		}
		e, err := snapshot.Resolve(ctx, "nested/file")
		if err != nil {
			t.Fatal(err)
		}
		snapshot.idx.cache = newCache(2 * ChunkSize)
		measured.reset()
		got := make([]byte, 64)
		off := int64(ChunkSize - 32)
		n, err := snapshot.ReadAt(ctx, e.OID, got, off)
		if err != nil || n != len(got) || !bytes.Equal(got, content[off:off+64]) {
			t.Fatalf("partial read across chunk boundary: %d %v", n, err)
		}
		if measured.packGets != 4 {
			t.Fatalf("two cold chunks need four payload GETs: %d", measured.packGets)
		}
		var c chunk
		if err := snapshot.idx.get(ctx, chunkKey(e.OID, 0), &c); err != nil {
			t.Fatal(err)
		}
		var originalChunk chunk
		if err := old.idx.get(ctx, chunkKey(original.OID, 0), &originalChunk); err != nil {
			t.Fatal(err)
		}
		if c.Base == nil || c.Base.Hash != originalChunk.Hash {
			t.Fatal("delta chain or base rotation despite nearly identical versions")
		}
		snapshot.idx.cache.mu.Lock()
		used := snapshot.idx.cache.used
		snapshot.idx.cache.mu.Unlock()
		if used > 2*ChunkSize {
			t.Fatal("cache exceeds budget")
		}
	}
	got := make([]byte, 64)
	if _, err := old.ReadAt(ctx, original.OID, got, ChunkSize-32); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(got, content[ChunkSize-32:ChunkSize+32]) {
		t.Fatal("old snapshot followed new content")
	}
}

func TestDeltaConcurrentColdReadsAndCorruption(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	local, _ := store.NewLocal(t.TempDir())
	raw := make([]byte, 64<<10)
	rand.New(rand.NewSource(6)).Read(raw)
	encoder, _ := newCompressor()
	defer encoder.Close()
	packed := encoder.EncodeAll(raw, nil)
	base := chunkBase{Pack: "packs/base", Length: int64(len(packed)), Hash: fmt.Sprintf("%x", sha256.Sum256(raw))}
	if err := local.Put(ctx, base.Pack, packed, ""); err != nil {
		t.Fatal(err)
	}
	var chunks []chunk
	var contents [][]byte
	for i := 0; i < 16; i++ {
		v := bytes.Clone(raw)
		v[i] ^= 255
		payload := encoder.EncodeAll(newDeltaBase(raw).delta(v), nil)
		key := fmt.Sprintf("packs/delta%d", i)
		if err := local.Put(ctx, key, payload, ""); err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, chunk{Pack: key, Length: int64(len(payload)), Hash: fmt.Sprintf("%x", sha256.Sum256(v)), Base: &base})
		contents = append(contents, v)
	}
	measured := &countedStore{Store: local}
	snapshot := &Snapshot{idx: &index{store: measured, cache: newCache(0)}}
	var wg sync.WaitGroup
	for i, c := range chunks {
		wg.Go(func() {
			got, err := snapshot.readChunk(ctx, c)
			if err != nil || !bytes.Equal(got, contents[i]) {
				t.Errorf("concurrent cold read: %v", err)
			}
		})
	}
	wg.Wait()
	if measured.packGets != 32 {
		t.Fatalf("expected exactly two payload ranges per chunk: %d", measured.packGets)
	}
	if err := local.Put(ctx, base.Pack, bytes.Repeat([]byte{0}, len(packed)), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.readChunk(ctx, chunks[0]); err == nil {
		t.Fatal("corrupt base accepted")
	}
	if err := local.Put(ctx, base.Pack, packed, ""); err != nil {
		t.Fatal(err)
	}
	if err := local.Put(ctx, chunks[0].Pack, []byte("broken"), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.readChunk(ctx, chunks[0]); err == nil {
		t.Fatal("corrupt delta accepted")
	}
}

// Both requests must arrive before either returns. A serial reader times out.
type pairedStore struct {
	store.Store
	mu    sync.Mutex
	count int
	both  chan struct{}
}

func (s *pairedStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	s.mu.Lock()
	s.count++
	if s.count == 2 {
		close(s.both)
	}
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, "", ctx.Err()
	case <-s.both:
	}
	return s.Store.Get(ctx, key, off, n)
}
func TestDeltaPayloadRangesAreFetchedInParallel(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	local, _ := store.NewLocal(t.TempDir())
	raw := bytes.Repeat([]byte("0123456789abcdef"), 128)
	enc, _ := newCompressor()
	defer enc.Close()
	baseData := enc.EncodeAll(raw, nil)
	base := chunkBase{Pack: "packs/base", Length: int64(len(baseData)), Hash: fmt.Sprintf("%x", sha256.Sum256(raw))}
	if err := local.Put(ctx, base.Pack, baseData, ""); err != nil {
		t.Fatal(err)
	}
	changed := bytes.Clone(raw)
	changed[100] = 'x'
	delta := enc.EncodeAll(newDeltaBase(raw).delta(changed), nil)
	c := chunk{Pack: "packs/delta", Length: int64(len(delta)), Hash: fmt.Sprintf("%x", sha256.Sum256(changed)), Base: &base}
	if err := local.Put(ctx, c.Pack, delta, ""); err != nil {
		t.Fatal(err)
	}
	paired := &pairedStore{Store: local, both: make(chan struct{})}
	snapshot := &Snapshot{idx: &index{store: paired, cache: newCache(8 << 10)}}
	got, err := snapshot.readChunk(ctx, c)
	if err != nil || !bytes.Equal(got, changed) {
		t.Fatalf("parallel read failed: %v", err)
	}
	snapshot.idx.cache.mu.Lock()
	defer snapshot.idx.cache.mu.Unlock()
	for _, entry := range snapshot.idx.cache.items {
		data := entry.Value.(cached).data
		if cap(data) > len(data)+64 {
			t.Fatalf("cache hides retained capacity: len=%d cap=%d", len(data), cap(data))
		}
	}
}

type failingSeedStore struct {
	store.Store
	first   chunk
	started chan struct{}
	failure error
}

func (s *failingSeedStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if !strings.HasPrefix(key, "packs/") {
		return s.Store.Get(ctx, key, off, n)
	}
	if key == s.first.Pack && off == s.first.Offset {
		select {
		case <-s.started:
			return nil, "", s.failure
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	close(s.started)
	<-ctx.Done()
	return nil, "", ctx.Err()
}

func TestImportFailureCancelsOtherBaseFetchesBeforeJoining(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	source := t.TempDir()
	command(t, source, "init", "-q")
	names := []string{"a", "b"}
	slices.SortFunc(names, func(a, b string) int {
		return bytes.Compare([]byte(fmt.Sprintf("%x", sha256.Sum256([]byte(a)))), []byte(fmt.Sprintf("%x", sha256.Sum256([]byte(b)))))
	})
	contents := make([][]byte, 2)
	for i, name := range names {
		contents[i] = make([]byte, 8192)
		rand.New(rand.NewSource(int64(i + 1))).Read(contents[i])
		write(t, source, name, contents[i])
	}
	first := commit(t, source)
	local, _ := store.NewLocal(t.TempDir())
	if _, err := Import(ctx, local, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	r, _ := New(local, DefaultCacheBytes)
	snapshot, err := r.Open(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	e, err := snapshot.Resolve(ctx, names[0])
	if err != nil {
		t.Fatal(err)
	}
	var c chunk
	if err := snapshot.idx.get(ctx, chunkKey(e.OID, 0), &c); err != nil {
		t.Fatal(err)
	}
	for i, name := range names {
		contents[i][0] ^= 255
		write(t, source, name, contents[i])
	}
	commit(t, source)
	injected := errors.New("seed read failed")
	backend := &failingSeedStore{Store: local, first: c, started: make(chan struct{}), failure: injected}
	head, _, err := local.Get(ctx, "HEAD", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := Import(ctx, backend, ImportOptions{Repo: source, CompressionWorkers: 4})
		result <- err
	}()
	select {
	case err := <-result:
		if !errors.Is(err, injected) {
			t.Fatalf("lost seed failure: %v", err)
		}
	case <-time.After(5 * time.Second):
		cancel()
		<-result
		t.Fatal("import waited for blocked base fetch before canceling it")
	}
	after, _, err := local.Get(ctx, "HEAD", 0, -1)
	if err != nil || !bytes.Equal(head, after) {
		t.Fatal("failed seed read changed publication")
	}
}
