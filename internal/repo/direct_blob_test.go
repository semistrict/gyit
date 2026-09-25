package repo

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	storagev1 "gat/internal/gen/gat/storage/v1"
	"gat/internal/spill"
	"gat/internal/store"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

type directCountStore struct {
	store.Store
	mu   sync.Mutex
	keys []string
}

func (c *directCountStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	c.mu.Lock()
	c.keys = append(c.keys, key)
	c.mu.Unlock()
	return c.Store.Get(ctx, key, off, n)
}
func (c *directCountStore) take() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := c.keys
	c.keys = nil
	return v
}

func TestDirectBlobSyntheticRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	backend := &directCountStore{Store: local}
	records, err := spill.New(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer records.Close()
	direct, err := newDirectBlobStage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer direct.records.Close()
	st := &stage{records: records, direct: direct}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	packs := &packWriter{ctx: ctx, store: backend, prefix: "direct-test", stats: &Stats{}}
	files := make(map[string][]byte)
	var largeID, emptyID string
	for i := 0; i < 260; i++ {
		raw := []byte(fmt.Sprintf("value-%03d\n", i))
		if i == 258 {
			raw = bytes.Repeat([]byte("abcdefgh01234567"), ChunkSize/8+3)
		}
		if i == 259 {
			raw = nil
		}
		h := sha1.New()
		fmt.Fprintf(h, "blob %d\x00", len(raw))
		h.Write(raw)
		oid := fmt.Sprintf("%x", h.Sum(nil))
		files[oid] = raw
		if i == 258 {
			largeID = oid
		}
		if i == 259 {
			emptyID = oid
		}
		// Intentionally stage chunk records before their size record.
		for off, part := 0, int64(0); off < len(raw); part++ {
			end := min(off+ChunkSize, len(raw))
			body := raw[off:end]
			c, err := packs.add(encoder.EncodeAll(body, nil), fmt.Sprintf("%x", sha256.Sum256(body)))
			if err != nil {
				t.Fatal(err)
			}
			if err := st.put(chunkKey(oid, part), c); err != nil {
				t.Fatal(err)
			}
			off = end
		}
		if err := st.put("o/"+oid, object{Kind: "blob", Size: int64(len(raw))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := packs.flush(); err != nil {
		t.Fatal(err)
	}
	blobRoot, err := direct.build(ctx, backend)
	if err != nil {
		t.Fatal(err)
	}
	idx := &index{store: backend, cache: newCache(32 << 20)}
	mainRoot, err := idx.updateSorted(ctx, records)
	if err != nil {
		t.Fatal(err)
	}
	idx.root, idx.blobRoot = mainRoot, blobRoot
	s := &Snapshot{idx: idx}
	if _, err := idx.directBlobPage(ctx, blobRoot); err != nil {
		t.Fatal(err)
	}
	var walk func(pageRef)
	walk = func(ref pageRef) {
		p, err := idx.directBlobPage(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		if ref.Length > directBlobPageBytes || len(p.Items)+len(p.Children) > fanout {
			t.Fatal("page bounds")
		}
		for _, c := range p.Children {
			walk(decodePageRef(c.Page))
		}
	}
	walk(blobRoot)
	for oid, want := range files {
		got := make([]byte, len(want)+3)
		n, err := s.ReadAt(ctx, oid, got, 0)
		if n != len(want) || !errors.Is(err, io.EOF) || !bytes.Equal(got[:n], want) {
			t.Fatalf("full body %s: n=%d err=%v", oid, n, err)
		}
	}
	large := files[largeID]
	for _, off := range []int64{0, ChunkSize - 7, ChunkSize + 11, int64(len(large) - 1), int64(len(large)), int64(len(large)) + 100*ChunkSize} {
		got := make([]byte, 79)
		n, err := s.ReadAt(ctx, largeID, got, off)
		want := []byte(nil)
		if off < int64(len(large)) {
			want = large[off:min(off+79, int64(len(large)))]
		}
		if n != len(want) || !bytes.Equal(got[:n], want) || (len(want) < 79 && !errors.Is(err, io.EOF)) || (len(want) == 79 && err != nil) {
			t.Fatalf("partial offset=%d n=%d err=%v", off, n, err)
		}
	}
	if n, err := s.ReadAt(ctx, emptyID, make([]byte, 1), 0); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("empty: %d %v", n, err)
	}
	if n, err := s.ReadAt(ctx, largeID, nil, 0); n != 0 || err != nil {
		t.Fatalf("zero read: %d %v", n, err)
	}
	if _, err := s.ReadAt(ctx, largeID, make([]byte, 1), -1); err == nil {
		t.Fatal("negative offset accepted")
	}
	if _, err := s.ReadAt(ctx, strings.Repeat("f", 40), make([]byte, 1), 0); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing blob: %v", err)
	}
	var stream bytes.Buffer
	if err := viewCopyBlob(ctx, s, largeID, int64(len(large)), &stream); err != nil || !bytes.Equal(stream.Bytes(), large) {
		t.Fatalf("stream read: %v", err)
	}
	backend.take()
	if _, err := s.ReadAt(ctx, largeID, make([]byte, 256), ChunkSize+5); err != nil {
		t.Fatal(err)
	}
	if keys := backend.take(); len(keys) != 0 {
		t.Fatalf("warm read fetched: %v", keys)
	}
	// A cold middle read and a far-EOF read must not probe main o/blob metadata.
	for _, off := range []int64{ChunkSize + 5, int64(len(large)) + 100*ChunkSize} {
		s.idx.cache = newCache(0)
		backend.take()
		_, err := s.ReadAt(ctx, largeID, make([]byte, 16), off)
		if off < int64(len(large)) && err != nil {
			t.Fatal(err)
		}
		keys := backend.take()
		metadata := 0
		for _, key := range keys {
			if key == mainRoot.Pack {
				t.Fatal("direct read fetched identity index")
			}
			if strings.HasPrefix(key, "index/") {
				metadata++
			}
		}
		if metadata != 2 {
			t.Fatalf("expected one two-level direct traversal; got %v", keys)
		}
	}
	s.idx.cache = newCache(64 << 10)
	if _, err := s.ReadAt(ctx, largeID, make([]byte, 128), ChunkSize); err != nil {
		t.Fatal(err)
	}
	if s.idx.cache.used > 64<<10 {
		t.Fatalf("cache exceeded budget: %d", s.idx.cache.used)
	}
}

func TestDirectBlobPageByteLimitAndMissingPart(t *testing.T) {
	ctx := t.Context()
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := &directBlobBuilder{w: &indexWriter{ctx: ctx, store: local, prefix: "direct-wide-test"}}
	for i := 0; i < 129; i++ {
		var base *storagev1.ChunkBase
		for depth := 0; depth < MaxDeltaDepth; depth++ {
			base = &storagev1.ChunkBase{Pack: "packs/" + strings.Repeat("a", 100), Length: 1, Hash: strings.Repeat("0", 64), Base: base}
		}
		oid := sha1.Sum([]byte(fmt.Sprint(i)))
		// Builder input is explicitly sorted below by monotonic binary IDs.
		clear(oid[:])
		oid[18], oid[19] = byte(i>>8), byte(i)
		r := &storagev1.DirectBlobPart{Oid: oid[:], Size: 1, Chunk: &storagev1.ChunkRecord{Pack: "packs/a", Length: 1, Hash: strings.Repeat("0", 64), Base: base}}
		if err := b.add(r); err != nil {
			t.Fatal(err)
		}
	}
	root, err := b.finish()
	if err != nil {
		t.Fatal(err)
	}
	idx := &index{store: local, cache: newCache(64 << 10), blobRoot: root}
	p, err := idx.directBlobPage(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Children) < 3 {
		t.Fatal("large descriptors did not force byte-bounded leaves")
	}
	for _, c := range p.Children {
		if _, err := idx.directBlobPage(ctx, decodePageRef(c.Page)); err != nil {
			t.Fatal(err)
		}
	}
	st, err := newDirectBlobStage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.records.Close()
	oid := strings.Repeat("1", 40)
	if _, err := st.add("o/"+oid, object{Kind: "blob", Size: 2 * ChunkSize}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.add(chunkKey(oid, 0), chunk{Pack: "packs/a", Length: 1, Hash: strings.Repeat("0", 64)}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.build(ctx, local); err == nil {
		t.Fatal("incomplete blob accepted")
	}
	// An oversized chain in any record must be rejected while decoding the page,
	// even if a lookup would have selected a different record.
	var deep *storagev1.ChunkBase
	for i := 0; i < 128; i++ {
		deep = &storagev1.ChunkBase{Base: deep}
	}
	malformed := &storagev1.DirectBlobPage{Items: []*storagev1.DirectBlobPart{{Oid: make([]byte, 20), Size: 1, Chunk: &storagev1.ChunkRecord{Base: deep}}}}
	raw, err := proto.Marshal(malformed)
	if err != nil {
		t.Fatal(err)
	}
	w := &indexWriter{ctx: ctx, store: local, prefix: "direct-invalid-test"}
	ref, err := w.saveBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.directBlobPage(ctx, ref); err == nil {
		t.Fatal("deep nested descriptor accepted")
	}
}

func TestDirectBlobSmallImportAndLegacyRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	source := t.TempDir()
	command(t, source, "init", "-q", "-b", "main")
	write(t, source, "file", []byte("old\n"))
	write(t, source, "empty", nil)
	old := commit(t, source)
	write(t, source, "file", []byte("new\n"))
	sha := commit(t, source)
	repackImportFixture(t, source)
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Import(ctx, local, ImportOptions{Repo: source, CompressionWorkers: 2}); err != nil {
		t.Fatal(err)
	}
	r, err := New(local, 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	s, err := r.Open(ctx, sha)
	if err != nil {
		t.Fatal(err)
	}
	if s.idx.blobRoot == (pageRef{}) {
		t.Fatal("snapshot lost direct blob root")
	}
	for _, revision := range []string{sha, old, "HEAD~1"} {
		snap, err := r.OpenRevision(ctx, revision, sha)
		if err != nil {
			t.Fatal(err)
		}
		if snap.idx.blobRoot != s.idx.blobRoot {
			t.Fatal("revision lost pinned direct root")
		}
		for _, args := range [][]string{{"cat-file", "-p", snap.SHA + ":file"}, {"cat-file", "-s", snap.SHA + ":file"}, {"ls-tree", "-rl", snap.SHA}} {
			checkObjectViewParity(t, ctx, r, snap, source, "", args)
		}
	}
	// Explicitly exercise the unmodified legacy body-reader branch.
	legacyStore, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rows, err := spill.New(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	stage := &stage{records: rows}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	body := bytes.Repeat([]byte("legacy"), ChunkSize/3)
	packs := &packWriter{ctx: ctx, store: legacyStore, prefix: "legacy-test", stats: &Stats{}}
	oid := strings.Repeat("2", 40)
	if err := stage.put("o/"+oid, object{Kind: "blob", Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	for off, part := 0, int64(0); off < len(body); part++ {
		end := min(off+ChunkSize, len(body))
		c, err := packs.add(enc.EncodeAll(body[off:end], nil), fmt.Sprintf("%x", sha256.Sum256(body[off:end])))
		if err != nil {
			t.Fatal(err)
		}
		if err := stage.put(chunkKey(oid, part), c); err != nil {
			t.Fatal(err)
		}
		off = end
	}
	if err := packs.flush(); err != nil {
		t.Fatal(err)
	}
	idx := &index{store: legacyStore, cache: newCache(32 << 20)}
	idx.root, err = idx.updateSorted(ctx, rows)
	if err != nil {
		t.Fatal(err)
	}
	legacy := &Snapshot{idx: idx}
	got := make([]byte, len(body))
	n, err := legacy.ReadAt(ctx, oid, got, 0)
	if err != nil || n != len(body) || !bytes.Equal(got, body) {
		t.Fatalf("legacy read %d %v", n, err)
	}
	var buf bytes.Buffer
	if err := viewCopyBlob(ctx, legacy, oid, int64(len(body)), &buf); err != nil || !bytes.Equal(buf.Bytes(), body) {
		t.Fatalf("legacy stream %v", err)
	}
}
