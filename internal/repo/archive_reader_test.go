package repo

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sizetable "gat/internal/globalsizes"
	sizewire "gat/internal/globalsizes/wire"
	"gat/internal/archive"
	archivewire "gat/internal/archive/wire"
	"gat/internal/store"
)

type archiveReaderFixture struct {
	backend     store.Store
	archiveID   [16]byte
	blobs       [][]byte
	blobRecipes []archivewire.Recipe
	chunks      []chunk
	treeRaw     [][]byte
	treeRecipes []archivewire.Recipe
	treeRefs    []pageRef
	manifests   []manifest
	commits     []string
}

func archiveTestOID(kind string, raw []byte) [20]byte {
	h := sha1.New()
	fmt.Fprintf(h, "%s %d\x00", kind, len(raw))
	h.Write(raw)
	var out [20]byte
	copy(out[:], h.Sum(nil))
	return out
}
func archiveTestDelta(base, target []byte) []byte {
	p := binary.AppendUvarint(nil, uint64(len(base)))
	p = binary.AppendUvarint(p, uint64(len(target)))
	for len(target) > 0 {
		n := min(127, len(target))
		p = append(p, byte(n))
		p = append(p, target[:n]...)
		target = target[n:]
	}
	return p
}
func archiveTestChunk(t *testing.T, r archivewire.Recipe) chunk {
	t.Helper()
	b, err := archivewire.Encode(r)
	if err != nil {
		t.Fatal(err)
	}
	return chunk{Hash: fmt.Sprintf("git-sha1:%x", r.TargetOID), ArchiveRecipe: string(b)}
}
func archiveSnapshotFixture(t *testing.T) archiveReaderFixture {
	t.Helper()
	ctx := t.Context()
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := archiveReaderFixture{backend: backend, archiveID: [16]byte{8, 9}, blobs: [][]byte{[]byte("base file\n"), []byte("changed file body\n"), []byte("third file version with more bytes\n")}}
	pack := make([]byte, 12)
	appendFrame := func(kind string, raw, program []byte) archivewire.Frame {
		var compressed bytes.Buffer
		w := zlib.NewWriter(&compressed)
		if _, err := w.Write(program); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		frame := archivewire.Frame{HeaderOffset: uint64(len(pack)), Offset: uint64(len(pack) + 2), Length: uint32(compressed.Len()), RawSize: uint32(len(program)), Size: uint32(len(raw)), OID: archiveTestOID(kind, raw)}
		pack = append(pack, 0, 0)
		pack = append(pack, compressed.Bytes()...)
		return frame
	}
	var blobFrames []archivewire.Frame
	for i, body := range f.blobs {
		program := body
		if i > 0 {
			program = archiveTestDelta(f.blobs[i-1], body)
		}
		blobFrames = append(blobFrames, appendFrame("blob", body, program))
		f.blobRecipes = append(f.blobRecipes, archivewire.Recipe{ArchiveID: f.archiveID, TargetOID: blobFrames[i].OID, Frames: append([]archivewire.Frame(nil), blobFrames...)})
	}
	for i := 0; i < 2; i++ {
		mode := "100644"
		if i == 1 {
			mode = "100755"
		}
		raw := append([]byte(mode+" file\x00"), blobFrames[i+1].OID[:]...)
		raw = append(raw, []byte("120000 link\x00")...)
		raw = append(raw, blobFrames[0].OID[:]...)
		f.treeRaw = append(f.treeRaw, raw)
	}
	var treeFrames []archivewire.Frame
	for i, raw := range f.treeRaw {
		program := raw
		if i > 0 {
			program = archiveTestDelta(f.treeRaw[i-1], raw)
		}
		treeFrames = append(treeFrames, appendFrame("tree", raw, program))
		f.treeRecipes = append(f.treeRecipes, archivewire.Recipe{ArchiveID: f.archiveID, TargetOID: treeFrames[i].OID, Frames: append([]archivewire.Frame(nil), treeFrames...)})
	}
	pack = append(pack, make([]byte, 20)...)
	for i := range f.blobRecipes {
		f.blobRecipes[i].PackSize = uint64(len(pack))
		f.chunks = append(f.chunks, archiveTestChunk(t, f.blobRecipes[i]))
	}
	for i := range f.treeRecipes {
		f.treeRecipes[i].PackSize = uint64(len(pack))
		encoded, err := archivewire.Encode(f.treeRecipes[i])
		if err != nil {
			t.Fatal(err)
		}
		writer := &indexWriter{ctx: ctx, store: backend, prefix: fmt.Sprintf("tree-archive-test-%d", i)}
		ref, err := writer.saveBytes(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.flush(); err != nil {
			t.Fatal(err)
		}
		f.treeRefs = append(f.treeRefs, ref)
	}
	if _, err := archive.Copy(ctx, backend, bytes.NewReader(pack), int64(len(pack)), f.archiveID); err != nil {
		t.Fatal(err)
	}
	for generation := 0; generation < 2; generation++ {
		count := generation + 2
		var records []sizetable.Record
		stage, err := newDirectBlobStage(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < count; i++ {
			records = append(records, sizetable.Record{OID: blobFrames[i].OID, Size: int64(len(f.blobs[i]))})
			oid := fmt.Sprintf("%x", blobFrames[i].OID)
			if _, err := stage.add("o/"+oid, object{Kind: "blob", Size: int64(len(f.blobs[i]))}); err != nil {
				t.Fatal(err)
			}
			if _, err := stage.add(chunkKey(oid, 0), f.chunks[i]); err != nil {
				t.Fatal(err)
			}
		}
		blobRoot, err := stage.build(ctx, backend)
		stage.records.Close()
		if err != nil {
			t.Fatal(err)
		}
		payload, _, err := sizetable.Build(ctx, records)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := sizewire.Encode(payload)
		if err != nil {
			t.Fatal(err)
		}
		ref := globalTestRef(t, backend, fmt.Sprintf("index/global-sizes-archive-test-%d", generation), encoded)
		main := map[string]any{"x/global-sizes": chunk{Pack: ref.Key, Hash: ref.Hash, Offset: ref.Offset, Length: ref.Length}}
		for i := 0; i < count; i++ {
			main[fmt.Sprintf("o/%x", blobFrames[i].OID)] = object{Kind: "blob", Size: int64(len(f.blobs[i]))}
		}
		for i := 0; i <= generation; i++ {
			oid := fmt.Sprintf("%x", treeFrames[i].OID)
			sha := fmt.Sprintf("%040x", i+100)
			main["o/"+oid] = object{Kind: "tree", Size: int64(len(f.treeRaw[i])), Directory: f.treeRefs[i]}
			main["o/"+sha] = object{Kind: "commit", Tree: oid}
		}
		root := globalOpenIndex(t, backend, fmt.Sprintf("archive-main-%d", generation), main)
		f.manifests = append(f.manifests, manifest{Version: 9014, Format: "sha1", Root: root, Blobs: blobRoot})
		f.commits = append(f.commits, fmt.Sprintf("%040x", generation+100))
	}
	return f
}

type archiveReadMeter struct {
	store.Store
	mu     sync.Mutex
	ranges []struct {
		key    string
		off, n int64
	}
	corrupt  bool
	started  chan struct{}
	finished atomic.Int32
}

func (m *archiveReadMeter) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if strings.HasPrefix(key, "packs/archive-") {
		m.mu.Lock()
		m.ranges = append(m.ranges, struct {
			key    string
			off, n int64
		}{key, off, n})
		m.mu.Unlock()
		if m.started != nil {
			m.started <- struct{}{}
			<-ctx.Done()
			m.finished.Add(1)
			return nil, "", ctx.Err()
		}
		b, token, err := m.Store.Get(ctx, key, off, n)
		if err == nil && m.corrupt {
			b = bytes.Clone(b)
			b[0] ^= 128
		}
		return b, token, err
	}
	return m.Store.Get(ctx, key, off, n)
}
func (m *archiveReadMeter) reset()     { m.mu.Lock(); m.ranges = nil; m.mu.Unlock() }
func (m *archiveReadMeter) count() int { m.mu.Lock(); defer m.mu.Unlock(); return len(m.ranges) }

func TestArchiveSnapshotCombinedReadAndGenerations(t *testing.T) {
	f := archiveSnapshotFixture(t)
	meter := &archiveReadMeter{Store: f.backend}
	r, err := New(meter, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	globalPublishManifest(t, f.backend, f.manifests[0])
	a, err := r.Open(t.Context(), f.commits[0])
	if err != nil {
		t.Fatal(err)
	}
	if meter.count() != 0 {
		t.Fatal("Open fetched archive content")
	}
	entries, err := a.ReadDir(t.Context(), a.Tree, "", 128)
	want := []Entry{{Name: "file", OID: fmt.Sprintf("%x", f.blobRecipes[1].TargetOID), Mode: 0100644, Size: int64(len(f.blobs[1]))}, {Name: "link", OID: fmt.Sprintf("%x", f.blobRecipes[0].TargetOID), Mode: 0120000, Size: int64(len(f.blobs[0]))}}
	if err != nil || !reflect.DeepEqual(entries, want) {
		t.Fatal("root listing", entries, err)
	}
	if meter.count() != 1 {
		t.Fatal("root listing fetched more than its tree", meter.count())
	}
	for _, b := range f.blobRecipes {
		if _, ok := a.idx.cache.get(archiveDataKey("blob", b.TargetOID)); ok {
			t.Fatal("root listing eagerly read file")
		}
	}
	page, err := a.ReadDirNames(t.Context(), a.Tree, "file", 1)
	if err != nil || len(page) != 1 || page[0].Name != "link" {
		t.Fatal("name pagination", page, err)
	}
	for _, e := range want {
		got, err := a.Lookup(t.Context(), a.Tree, e.Name)
		if err != nil || got != e {
			t.Fatal("lookup", got, err)
		}
	}
	meter.reset()
	buf := make([]byte, len(f.blobs[1]))
	if n, err := a.ReadAt(t.Context(), want[0].OID, buf, 0); err != nil || n != len(buf) || !bytes.Equal(buf, f.blobs[1]) {
		t.Fatal("blob read", n, err)
	}
	if meter.count() != 1 {
		t.Fatal("blob recipe did not merge adjacent source frames", meter.count())
	}
	if _, ok := a.idx.cache.get(archiveDataKey("blob", f.blobRecipes[0].TargetOID)); !ok {
		t.Fatal("verified intermediate absent")
	}
	globalPublishManifest(t, f.backend, f.manifests[1])
	b, err := r.Open(t.Context(), f.commits[1])
	if err != nil {
		t.Fatal(err)
	}
	meter.reset()
	buf = make([]byte, len(f.blobs[2]))
	if n, err := b.ReadAt(t.Context(), fmt.Sprintf("%x", f.blobRecipes[2].TargetOID), buf, 0); err != nil || n != len(buf) || !bytes.Equal(buf, f.blobs[2]) {
		t.Fatal("related blob", err)
	}
	last := f.blobRecipes[2].Frames[2]
	if meter.count() != 1 || meter.ranges[0].off != int64(last.Offset) || meter.ranges[0].n != int64(last.Length) {
		t.Fatal("did not reuse deepest verified intermediate", meter.ranges)
	}
	for _, s := range []*Snapshot{a, b, a} {
		page, err := s.ReadDir(t.Context(), s.Tree, "", 128)
		if err != nil {
			t.Fatal(err)
		}
		i := 1
		if s == b {
			i = 2
		}
		if page[0].Size != int64(len(f.blobs[i])) || page[0].OID != fmt.Sprintf("%x", f.blobRecipes[i].TargetOID) {
			t.Fatal("pinned table mismatch", page)
		}
		derived := &Snapshot{idx: s.idx, Tree: s.Tree}
		if e, err := derived.Resolve(t.Context(), "file"); err != nil || e != page[0] {
			t.Fatal("derived table binding", e, err)
		}
	}
	if a.idx.cache != b.idx.cache || a.idx.globalSizes != b.idx.globalSizes || a.idx.globalSizeRef == b.idx.globalSizeRef || r.cache.max != globalOrdinaryBudget || uint64(r.cache.used)+r.globalSizes.charged.Load() > DefaultCacheBytes {
		t.Fatal("unbounded generation retention")
	}
	if r.globalSizes.loads.Load() < 3 {
		t.Fatal("fixture did not exercise table replacement")
	}
	for i := 0; i < 20; i++ {
		r.cache.put(fmt.Sprintf("archive-pressure-%d", i), make([]byte, 1<<20))
	}
	if _, err := b.ReadDir(t.Context(), b.Tree, "", 128); err != nil {
		t.Fatal(err)
	}
	if uint64(r.cache.used)+r.globalSizes.charged.Load() > DefaultCacheBytes {
		t.Fatal("pressure exceeded retained budget")
	}
}

func TestArchiveSnapshotRejectsCorruptionAndWrongMetadata(t *testing.T) {
	f := archiveSnapshotFixture(t)
	for _, which := range []string{"payload", "union", "logical-size", "logical-oid", "tree-page-checksum", "tree-size", "kind-cache"} {
		t.Run(which, func(t *testing.T) {
			meter := &archiveReadMeter{Store: f.backend, corrupt: which == "payload"}
			s := &Snapshot{idx: &index{store: meter, cache: newCache(DefaultCacheBytes), root: f.manifests[0].Root, blobRoot: f.manifests[0].Blobs}}
			c := f.chunks[1]
			if which == "union" {
				c.Pack = "packs/unexpected"
			}
			if which == "logical-size" {
				if _, err := checkedArchiveBlob(c, fmt.Sprintf("%x", f.blobRecipes[1].TargetOID), int64(len(f.blobs[1])+1), 0); err == nil {
					t.Fatal("wrong size accepted")
				}
				return
			}
			if which == "logical-oid" {
				if _, err := checkedArchiveBlob(c, fmt.Sprintf("%x", f.blobRecipes[0].TargetOID), int64(len(f.blobs[1])), 0); err == nil {
					t.Fatal("wrong identity accepted")
				}
				return
			}
			if which == "tree-page-checksum" || which == "tree-size" || which == "kind-cache" {
				o := object{Kind: "tree", Size: int64(len(f.treeRaw[0])), Directory: f.treeRefs[0]}
				if which == "tree-page-checksum" {
					o.Directory.Hash = strings.Repeat("0", 64)
				}
				if which == "tree-size" {
					o.Size++
				}
				if which == "kind-cache" {
					// Even a matching OID-shaped cache entry in the other kind is unusable.
					s.idx.cache.put(archiveDataKey("blob", f.treeRecipes[0].TargetOID), []byte("wrong kind"))
					entries, err := s.archiveTreeEntries(t.Context(), fmt.Sprintf("%x", f.treeRecipes[0].TargetOID), o)
					if err != nil || len(entries) != 2 || meter.count() != 1 {
						t.Fatal("tree used blob cache namespace", entries, err)
					}
					return
				}
				if entries, err := s.archiveTreeEntries(t.Context(), fmt.Sprintf("%x", f.treeRecipes[0].TargetOID), o); err == nil || entries != nil || meter.count() != 0 {
					t.Fatal("bad tree metadata reached source", err)
				}
				return
			}
			if which == "payload" {
				buf := bytes.Repeat([]byte{0xa5}, len(f.blobs[1]))
				for range 2 {
					n, err := s.ReadAt(t.Context(), fmt.Sprintf("%x", f.blobRecipes[1].TargetOID), buf, 0)
					if err == nil || n != 0 || !bytes.Equal(buf, bytes.Repeat([]byte{0xa5}, len(buf))) {
						t.Fatal("corrupt payload exposed", n, err)
					}
				}
				for _, r := range f.blobRecipes[:2] {
					if _, ok := s.idx.cache.get(archiveDataKey("blob", r.TargetOID)); ok {
						t.Fatal("failed target populated decoded cache")
					}
				}
				return
			}
			if b, err := s.readChunk(t.Context(), c); err == nil || b != nil || meter.count() != 0 {
				t.Fatal("bad union fetched payload", err)
			}
		})
	}
}

func TestArchiveSnapshotCancellationJoinsFetch(t *testing.T) {
	f := archiveSnapshotFixture(t)
	meter := &archiveReadMeter{Store: f.backend, started: make(chan struct{}, 4)}
	s := &Snapshot{idx: &index{store: meter, cache: newCache(DefaultCacheBytes), root: f.manifests[0].Root, blobRoot: f.manifests[0].Blobs}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.readChunk(ctx, f.chunks[1]); done <- err }()
	select {
	case <-meter.started:
	case <-time.After(2 * time.Second):
		t.Fatal("archive request did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("archive read did not cancel")
	}
	// cache.load can return cancellation before its singleflight function exits.
	// Wait for that function to finish, and require every slot to be released.
	deadline := time.Now().Add(2 * time.Second)
	for len(s.idx.cache.slots) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if meter.finished.Load() != 1 || len(s.idx.cache.slots) != 0 {
		t.Fatal("archive worker retained", meter.finished.Load())
	}
}

func TestArchiveSnapshotChunkWireRoundTrip(t *testing.T) {
	f := archiveSnapshotFixture(t)
	for _, c := range f.chunks {
		encoded, err := marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		var got chunk
		if err := unmarshal(encoded, &got); err != nil || got != c {
			t.Fatal("archive recipe dropped by encoding seam", err)
		}
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(f.chunks[0].Hash, "git-sha1:")); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveSnapshotLegacyCatalogChecksRequestedIdentity(t *testing.T) {
	f := archiveSnapshotFixture(t)
	wantOID := fmt.Sprintf("%x", f.blobRecipes[0].TargetOID)
	// Keep all lengths internally consistent while substituting another blob.
	// Authentication of that substituted blob alone does not authenticate the
	// object the caller requested.
	root := globalOpenIndex(t, f.backend, "archive-wrong-oid", map[string]any{
		"o/" + wantOID:       object{Kind: "blob", Size: int64(len(f.blobs[1]))},
		chunkKey(wantOID, 0): f.chunks[1],
	})
	meter := &archiveReadMeter{Store: f.backend}
	s := &Snapshot{idx: &index{store: meter, cache: newCache(DefaultCacheBytes), root: root}}
	for _, stream := range []bool{false, true} {
		buf := bytes.Repeat([]byte{0xa5}, len(f.blobs[1]))
		var n int
		var err error
		if stream {
			r := &viewBlobReader{ctx: t.Context(), s: s, oid: wantOID, size: int64(len(buf))}
			n, err = r.Read(buf)
		} else {
			n, err = s.ReadAt(t.Context(), wantOID, buf, 0)
		}
		if err == nil || n != 0 || !bytes.Equal(buf, bytes.Repeat([]byte{0xa5}, len(buf))) || meter.count() != 0 {
			t.Fatal("wrong logical blob escaped", stream, n, err)
		}
	}
}
