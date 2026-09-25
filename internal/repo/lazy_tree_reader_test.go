package repo

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	storagev1 "gat/internal/gen/gat/storage/v1"
	"gat/internal/store"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

func lazyTreeHash(raw []byte) string {
	h := sha1.New()
	fmt.Fprintf(h, "tree %d\x00", len(raw))
	h.Write(raw)
	return fmt.Sprintf("git-tree-sha1:%x", h.Sum(nil))
}

func lazyTreeRow(mode uint32, name, oid string) []byte {
	id, err := hex.DecodeString(oid)
	if err != nil || len(id) != 20 {
		panic("invalid fixture OID")
	}
	return append([]byte(fmt.Sprintf("%o %s\x00", mode, name)), id...)
}

// This deliberately optimistic full-frame fixture isolates the extra size
// lookups. It does not claim admission or locality of an original native chain.
func lazyTreeFixtureDescriptor(t *testing.T, ctx context.Context, backend store.Store, raw []byte) (string, pageRef) {
	t.Helper()
	hash := lazyTreeHash(raw)
	oid := strings.TrimPrefix(hash, "git-tree-sha1:")
	encoded := deferredReaderZlib(t, raw)
	if len(raw) > nativeTreeBytes || len(encoded) > nativeTreeBytes {
		t.Fatal("full-frame fixture exceeds admission")
	}
	c := chunk{Pack: "packs/nativechain-tree-" + oid, Length: int64(len(encoded)), Hash: hash}
	if err := backend.Put(ctx, c.Pack, encoded, ""); err != nil {
		t.Fatal(err)
	}
	wire, err := marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	w := &indexWriter{ctx: ctx, store: backend, prefix: "native-trees-" + oid}
	ref, err := w.saveBytes(wire)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	return oid, ref
}

func lazyTreeFixtureIndex(t *testing.T, backend store.Store, objects map[string]any, blobRoot pageRef) *Snapshot {
	t.Helper()
	var p page
	for key, value := range objects {
		wire, err := marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		p.Items = append(p.Items, item{Key: key, Value: wire})
	}
	sort.Slice(p.Items, func(i, j int) bool { return p.Items[i].Key < p.Items[j].Key })
	w := &indexWriter{ctx: t.Context(), store: backend, prefix: "lazy-tree-fixture"}
	edge, err := w.save(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	return &Snapshot{idx: &index{store: backend, cache: newCache(DefaultCacheBytes), root: edge.ID, blobRoot: blobRoot}}
}

func TestLazyTreeNamesAndHydration(t *testing.T) {
	backend := &deferredReaderStore{}
	fileOID := strings.Repeat("01", 20)
	linkOID := strings.Repeat("02", 20)
	dirOID := strings.Repeat("03", 20)
	subOID := strings.Repeat("04", 20)
	raw := bytes.Join([][]byte{
		lazyTreeRow(0160000, "sub", subOID), lazyTreeRow(0100664, "foo.c", fileOID),
		lazyTreeRow(0040000, "foo", dirOID), lazyTreeRow(0120000, "link", linkOID),
	}, nil)
	oid, ref := lazyTreeFixtureDescriptor(t, t.Context(), backend, raw)
	s := lazyTreeFixtureIndex(t, backend, map[string]any{"o/" + oid: object{Kind: "tree", Size: int64(len(raw)), Directory: ref}}, pageRef{})
	s.Tree = oid
	// No blob or child-tree identity exists: names-only traversal must still work.
	got, err := s.ReadDirNames(t.Context(), oid, "", 128)
	if err != nil {
		t.Fatal(err)
	}
	want := []Dirent{{Name: "foo", OID: dirOID, Mode: 0040000}, {Name: "foo.c", OID: fileOID, Mode: 0100644, RawMode: 0100664}, {Name: "link", OID: linkOID, Mode: 0120000}, {Name: "sub", OID: subOID, Mode: 0160000}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("names=%+v want=%+v", got, want)
	}
	e, err := s.ResolveName(t.Context(), "foo.c")
	if err != nil || e != want[1] {
		t.Fatalf("resolve=%+v %v", e, err)
	}
	if _, err := s.LookupName(t.Context(), oid, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("lookup missing: %v", err)
	}
	if entries, err := s.ReadDir(t.Context(), oid, "", 128); err == nil || entries != nil {
		t.Fatalf("missing sizes accepted: %+v %v", entries, err)
	}
	if _, err := s.Lookup(t.Context(), oid, "foo.c"); err == nil {
		t.Fatal("missing size accepted")
	}

	// Actual direct-blob leaf supplies exact sizes, including symlink length.
	chunkProto := &storagev1.ChunkRecord{Pack: "packs/test", Length: 1, Hash: strings.Repeat("a", 64)}
	fileID, _ := hex.DecodeString(fileOID)
	linkID, _ := hex.DecodeString(linkOID)
	wire, err := proto.Marshal(&storagev1.DirectBlobPage{Items: []*storagev1.DirectBlobPart{
		{Oid: fileID, Size: 123, Chunk: chunkProto}, {Oid: linkID, Size: 7, Chunk: chunkProto},
	}})
	if err != nil {
		t.Fatal(err)
	}
	w := &indexWriter{ctx: t.Context(), store: backend, prefix: "lazy-tree-blobs"}
	s.idx.blobRoot, err = w.saveBytes(wire)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.flush(); err != nil {
		t.Fatal(err)
	}
	entries, err := s.ReadDir(t.Context(), oid, "", 128)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 || entries[1].Size != 123 || entries[2].Size != 7 || entries[0].Size != 0 || entries[3].Size != 0 {
		t.Fatalf("hydration=%+v", entries)
	}
	e2, err := s.Lookup(t.Context(), oid, "foo.c")
	if err != nil || e2 != entries[1] {
		t.Fatalf("lookup=%+v %v", e2, err)
	}
	if backend.gets() != 1 {
		t.Fatalf("size hydration fetched payloads: %d", backend.gets())
	}
}

func TestLazyTreePaginationAndCopiedNames(t *testing.T) {
	backend := &deferredReaderStore{}
	child := strings.Repeat("12", 20)
	var raw []byte
	// Cross the bufio refill boundary while reading OIDs after name slices.
	for i := 159; i >= 0; i-- {
		raw = append(raw, lazyTreeRow(0100755, fmt.Sprintf("file-%03d-%s", i, strings.Repeat("x", 19)), child)...)
	}
	oid, ref := lazyTreeFixtureDescriptor(t, t.Context(), backend, raw)
	s := lazyTreeFixtureIndex(t, backend, map[string]any{"o/" + oid: object{Kind: "tree", Size: int64(len(raw)), Directory: ref}}, pageRef{})
	first, err := s.ReadDirNames(t.Context(), oid, "", 128)
	if err != nil || len(first) != 128 || cap(first) != 128 {
		t.Fatalf("first %d/%d: %v", len(first), cap(first), err)
	}
	second, err := s.ReadDirNames(t.Context(), oid, first[127].Name, 128)
	if err != nil || len(second) != 32 || cap(second) != 32 {
		t.Fatalf("second %d/%d: %v", len(second), cap(second), err)
	}
	all := append(first, second...)
	for i, e := range all {
		if e.Name != fmt.Sprintf("file-%03d-%s", i, strings.Repeat("x", 19)) {
			t.Fatalf("name %d=%q", i, e.Name)
		}
	}
	parsed, err := parseNativeTree(raw)
	if err != nil {
		t.Fatal(err)
	}
	// Mutating the source must not alter parsed names.
	for i := range raw {
		raw[i] = 'z'
	}
	if parsed[0].Name != first[0].Name {
		t.Fatal("parsed name aliases raw tree")
	}
	if backend.gets() != 1 {
		t.Fatalf("pagination payload GETs=%d", backend.gets())
	}
}

func TestLazyTreeRejectsMalformedBeforeEntries(t *testing.T) {
	oid := strings.Repeat("12", 20)
	good := lazyTreeRow(0100644, "good", oid)
	cases := map[string][]byte{
		"duplicate": append(bytes.Clone(good), good...),
		"dot":       lazyTreeRow(0100644, ".", oid), "dotdot": lazyTreeRow(0100644, "..", oid),
		"slash": lazyTreeRow(0100644, "a/b", oid), "empty": lazyTreeRow(0100644, "", oid),
		"long": lazyTreeRow(0100644, strings.Repeat("a", 256), oid),
		"mode": lazyTreeRow(020000, "bad", oid), "truncated": good[:len(good)-1],
		"partial-mode": []byte("100644"), "overflow": append([]byte("777777777777777777777 "), good...),
		"trailing-mode":  append(bytes.Clone(good), []byte("100644")...),
		"trailing-space": append(bytes.Clone(good), []byte("100644 ")...),
		"mode-junk":      append([]byte("100644x file\x00"), bytes.Repeat([]byte{1}, 20)...),
		"mode-eight":     append([]byte("100648 file\x00"), bytes.Repeat([]byte{1}, 20)...),
		"empty-mode":     append([]byte(" file\x00"), bytes.Repeat([]byte{1}, 20)...),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			backend := &deferredReaderStore{}
			tree, ref := lazyTreeFixtureDescriptor(t, t.Context(), backend, raw)
			s := lazyTreeFixtureIndex(t, backend, map[string]any{"o/" + tree: object{Kind: "tree", Size: int64(len(raw)), Directory: ref}}, pageRef{})
			entries, err := s.ReadDirNames(t.Context(), tree, "", 128)
			if err == nil || entries != nil {
				t.Fatalf("exposed malformed tree: %+v %v", entries, err)
			}
		})
	}
}

func TestLazyTreeAuthenticationEmptyAndCancellation(t *testing.T) {
	backend := &deferredReaderStore{}
	oid, ref := lazyTreeFixtureDescriptor(t, t.Context(), backend, nil)
	s := lazyTreeFixtureIndex(t, backend, map[string]any{"o/" + oid: object{Kind: "tree", Directory: ref}}, pageRef{})
	got, err := s.ReadDirNames(t.Context(), oid, "", 128)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty=%v %v", got, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err = s.ReadDirNames(ctx, oid, "", 128); !errors.Is(err, context.Canceled) || got != nil {
		t.Fatalf("canceled=%v %v", got, err)
	}

	raw := lazyTreeRow(0100644, "file", strings.Repeat("12", 20))
	oid, ref = lazyTreeFixtureDescriptor(t, t.Context(), backend, raw)
	s = lazyTreeFixtureIndex(t, backend, map[string]any{"o/" + oid: object{Kind: "tree", Size: int64(len(raw)), Directory: ref}}, pageRef{})
	wrong := bytes.Clone(raw)
	wrong[len(wrong)-1] ^= 1
	backend.data["packs/nativechain-tree-"+oid] = deferredReaderZlib(t, wrong)
	for i := 0; i < 2; i++ {
		got, err = s.ReadDirNames(t.Context(), oid, "", 128)
		if err == nil || got != nil {
			t.Fatalf("corrupt read %d exposed %+v: %v", i, got, err)
		}
	}
	if _, ok := s.idx.cache.get("tree/git-tree-sha1:" + oid); ok {
		t.Fatal("corrupt tree cached")
	}
}

func lazyTreeLiteralProgram(base []byte, target []byte) []byte {
	appendNumber := func(out []byte, n int) []byte {
		for n >= 128 {
			out = append(out, byte(n)|128)
			n >>= 7
		}
		return append(out, byte(n))
	}
	out := appendNumber(nil, len(base))
	out = appendNumber(out, len(target))
	for len(target) > 0 {
		n := min(127, len(target))
		out = append(out, byte(n))
		out = append(out, target[:n]...)
		target = target[n:]
	}
	return out
}

func TestLazyTreeNativeBundleAndBounds(t *testing.T) {
	backend := &deferredReaderStore{}
	root := []byte{}
	target := lazyTreeRow(0100644, "file", strings.Repeat("12", 20))
	rootWire := deferredReaderZlib(t, root)
	bundle := deferredReaderBundle(t, lazyTreeLiteralProgram(root, target))
	base := &chunkBase{Pack: "packs/nativechain-tree-root", Length: int64(len(rootWire)), Hash: lazyTreeHash(root)}
	c := chunk{Pack: "packs/nativechain-tree-target", Length: int64(len(bundle)), Hash: lazyTreeHash(target), Base: base}
	backend.Put(t.Context(), base.Pack, rootWire, "")
	backend.Put(t.Context(), c.Pack, bundle, "")
	s := &Snapshot{idx: &index{store: backend, cache: newCache(DefaultCacheBytes)}}
	got, err := s.readNativeTree(t.Context(), c)
	if err != nil || !bytes.Equal(got, target) || backend.gets() != 2 {
		t.Fatalf("bundle=%q %v gets=%d", got, err, backend.gets())
	}
	// An authenticated empty root in cache remains a valid ancestor.
	s.idx.cache = newCache(DefaultCacheBytes)
	s.idx.cache.put("tree-root/"+base.Hash, nil)
	before := backend.gets()
	if _, err := s.readNativeTree(t.Context(), c); err != nil || backend.gets() != before+1 {
		t.Fatalf("cached empty root: %v", err)
	}
	for _, mutate := range []func(*chunk){
		func(c *chunk) { c.Hash = "git-sha1:" + strings.TrimPrefix(c.Hash, "git-tree-sha1:") },
		func(c *chunk) { c.Hash = strings.ToUpper(c.Hash) },
		func(c *chunk) { c.Length = nativeTreeBytes },
		func(c *chunk) { b := *c.Base; b.Base = &chunkBase{}; c.Base = &b },
	} {
		bad := c
		mutate(&bad)
		if _, err := s.readNativeTree(t.Context(), bad); err == nil {
			t.Fatal("malformed descriptor accepted")
		}
	}
	// Five 1MiB copy results exceed aggregate work although each is bounded.
	large := bytes.Repeat([]byte("x"), ChunkSize)
	rootWire = deferredReaderZlib(t, large)
	copyProgram := []byte{0x80, 0x80, 0x40, 0x80, 0x80, 0x40, 0xc0, 0x10}
	final := lazyTreeLiteralProgram(large, target)
	bundle = deferredReaderBundle(t, copyProgram, copyProgram, copyProgram, copyProgram, final)
	base = &chunkBase{Pack: "packs/nativechain-tree-work-root", Length: int64(len(rootWire)), Hash: lazyTreeHash(large)}
	c = chunk{Pack: "packs/nativechain-tree-work-target", Length: int64(len(bundle)), Hash: lazyTreeHash(target), Base: base}
	backend.Put(t.Context(), base.Pack, rootWire, "")
	backend.Put(t.Context(), c.Pack, bundle, "")
	s.idx.cache = newCache(DefaultCacheBytes)
	if _, err := s.readNativeTree(t.Context(), c); err == nil || !strings.Contains(err.Error(), "work limit") {
		t.Fatalf("work limit: %v", err)
	}
}

func TestLazyTreeCompiledFallback(t *testing.T) {
	backend := &deferredReaderStore{}
	child := bytes.Repeat([]byte{0x42}, 32)
	wire, err := proto.Marshal(&storagev1.DirectoryPage{Entries: []*storagev1.NamedEntry{{Name: []byte("file"), Oid: child, Mode: 0100644, Size: 123}}})
	if err != nil {
		t.Fatal(err)
	}
	z, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	packed := z.EncodeAll(wire, nil)
	z.Close()
	w := &indexWriter{ctx: t.Context(), store: backend, prefix: "compiled-tree-fixture"}
	ref, err := w.saveBytes(packed)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.flush(); err != nil {
		t.Fatal(err)
	}
	oid := strings.Repeat("21", 32)
	s := lazyTreeFixtureIndex(t, backend, map[string]any{"o/" + oid: object{Kind: "tree", Directory: ref}}, pageRef{})
	// Compiled sizes remain authoritative; no separate child identity is needed.
	entries, err := s.ReadDir(t.Context(), oid, "", 128)
	if err != nil || len(entries) != 1 || entries[0].Size != 123 {
		t.Fatalf("compiled ReadDir: %+v %v", entries, err)
	}
	names, err := s.ReadDirNames(t.Context(), oid, "", 128)
	if err != nil || len(names) != 1 || names[0] != direntOf(entries[0]) {
		t.Fatalf("compiled names: %+v %v", names, err)
	}
	e, err := s.Lookup(t.Context(), oid, "file")
	if err != nil || e != entries[0] {
		t.Fatalf("compiled Lookup: %+v %v", e, err)
	}
	n, err := s.LookupName(t.Context(), oid, "file")
	if err != nil || n != names[0] {
		t.Fatalf("compiled LookupName: %+v %v", n, err)
	}
}
