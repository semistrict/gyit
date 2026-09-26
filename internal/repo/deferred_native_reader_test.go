package repo

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	probev1 "gyit/internal/gen/gyit/probe/v1"
	storagev1 "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
	"google.golang.org/protobuf/proto"
)

type deferredReaderStore struct {
	mu          sync.Mutex
	data        map[string][]byte
	payloadGets int
}

func (s *deferredReaderStore) Get(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if key == "HEAD" {
		return nil, "", fmt.Errorf("test reader must not access HEAD")
	}
	data, ok := s.data[key]
	if !ok {
		return nil, "", store.ErrNotFound
	}
	if strings.HasPrefix(key, "packs/") {
		s.payloadGets++
	}
	if length == -1 {
		length = int64(len(data)) - off
	}
	if off < 0 || length < 0 || off > int64(len(data))-length {
		return nil, "", io.ErrUnexpectedEOF
	}
	return bytes.Clone(data[off : off+length]), "test-token", nil
}

func (s *deferredReaderStore) Put(ctx context.Context, key string, data []byte, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if key == "HEAD" {
		return fmt.Errorf("test reader must not publish")
	}
	if s.data == nil {
		s.data = map[string][]byte{}
	}
	s.data[key] = bytes.Clone(data)
	return nil
}

func (s *deferredReaderStore) gets() int { s.mu.Lock(); defer s.mu.Unlock(); return s.payloadGets }

func deferredReaderHash(raw []byte, legacy bool) string {
	if legacy {
		return fmt.Sprintf("%x", sha256.Sum256(raw))
	}
	h := sha1.New()
	fmt.Fprintf(h, "blob %d\x00", len(raw))
	h.Write(raw)
	return fmt.Sprintf("git-sha1:%x", h.Sum(nil))
}

func deferredReaderZlib(t *testing.T, raw []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	z := zlib.NewWriter(&b)
	if _, err := z.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func deferredReaderBundle(t *testing.T, programs ...[]byte) []byte {
	t.Helper()
	b := &probev1.NativeProgramBundle{}
	for _, p := range programs {
		b.Frames = append(b.Frames, &probev1.NativeFrame{RawSize: uint32(len(p)), Zlib: deferredReaderZlib(t, p)})
	}
	wire, err := proto.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func deferredReaderFixture(t *testing.T, legacy, coalesced bool) (*Snapshot, *deferredReaderStore, chunk) {
	t.Helper()
	s := &deferredReaderStore{}
	root := deferredReaderZlib(t, []byte("A"))
	bundle := deferredReaderBundle(t, []byte{1, 2, 0x90, 1, 1, 'B'})
	base := &chunkBase{Pack: "packs/nativechain-auth-root", Length: int64(len(root)), Hash: deferredReaderHash([]byte("A"), legacy)}
	c := chunk{Pack: "packs/nativechain-auth-target", Length: int64(len(bundle)), Hash: deferredReaderHash([]byte("AB"), legacy), Base: base}
	if coalesced {
		c.Pack = base.Pack
		c.Offset = base.Length
		if err := s.Put(t.Context(), c.Pack, append(root, bundle...), ""); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := s.Put(t.Context(), base.Pack, root, ""); err != nil {
			t.Fatal(err)
		}
		if err := s.Put(t.Context(), c.Pack, bundle, ""); err != nil {
			t.Fatal(err)
		}
	}
	return &Snapshot{idx: &index{store: s, cache: newCache(DefaultCacheBytes)}}, s, c
}

func TestDeferredNativeReaderAuthenticationAndCache(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, coalesced := range []bool{false, true} {
			t.Run(fmt.Sprintf("legacy=%v/coalesced=%v", legacy, coalesced), func(t *testing.T) {
				s, backend, c := deferredReaderFixture(t, legacy, coalesced)
				got, err := s.readChunk(t.Context(), c)
				if err != nil || string(got) != "AB" {
					t.Fatalf("read: %q %v", got, err)
				}
				wantGets := 2
				if coalesced {
					wantGets = 1
				}
				if backend.gets() != wantGets {
					t.Fatalf("payload GETs=%d want=%d", backend.gets(), wantGets)
				}
				got, err = s.readChunk(t.Context(), c)
				if err != nil || string(got) != "AB" || backend.gets() != wantGets {
					t.Fatal("verified target cache was not reused", err)
				}
				if s.idx.cache.used > DefaultCacheBytes {
					t.Fatal("cache budget exceeded")
				}
			})
		}
	}
}

func TestDeferredNativeReaderCorruptionReturnsNoBytes(t *testing.T) {
	s, backend, c := deferredReaderFixture(t, false, false)
	wrong := deferredReaderBundle(t, []byte{1, 2, 0x90, 1, 1, 'C'})
	if err := backend.Put(t.Context(), c.Pack, wrong, ""); err != nil {
		t.Fatal(err)
	}
	c.Length = int64(len(wrong))
	// Exercise the public copy boundary as well as the native decoder.
	builder := &directBlobBuilder{w: &indexWriter{ctx: t.Context(), store: backend, prefix: "reader-test"}}
	oid, err := hex.DecodeString(strings.TrimPrefix(c.Hash, "git-sha1:"))
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.add(&storagev1.DirectBlobPart{Oid: oid, Size: 2, Chunk: &storagev1.ChunkRecord{Pack: c.Pack, Offset: c.Offset, Length: c.Length, Hash: c.Hash, Base: encodeChunkBase(c.Base)}}); err != nil {
		t.Fatal(err)
	}
	s.idx.blobRoot, err = builder.finish()
	if err != nil {
		t.Fatal(err)
	}
	dest := []byte{0xee, 0xee}
	if n, err := s.ReadAt(t.Context(), fmt.Sprintf("%x", oid), dest, 0); n != 0 || err == nil || !bytes.Equal(dest, []byte{0xee, 0xee}) {
		t.Fatalf("corrupt target returned/copied data: n=%d dest=%x err=%v", n, dest, err)
	}
	if _, ok := s.idx.cache.get("data/" + c.Hash); ok {
		t.Fatal("unverified target entered cache")
	}
	if _, ok := s.idx.cache.get("data/" + c.Base.Hash); !ok {
		t.Fatal("verified root was not cached")
	}
	good := deferredReaderBundle(t, []byte{1, 2, 0x90, 1, 1, 'B'})
	if err := backend.Put(t.Context(), c.Pack, good, ""); err != nil {
		t.Fatal(err)
	}
	c.Length = int64(len(good))
	before := backend.gets()
	got, err := s.readChunk(t.Context(), c)
	if err != nil || string(got) != "AB" || backend.gets()-before != 1 {
		t.Fatalf("valid retry after corruption: %q %v", got, err)
	}
}

func TestDeferredNativeReaderAuthenticatesRootAndSeparatesOIDs(t *testing.T) {
	s, backend, c := deferredReaderFixture(t, false, false)
	wrongRoot := deferredReaderZlib(t, []byte("Z"))
	if err := backend.Put(t.Context(), c.Base.Pack, wrongRoot, ""); err != nil {
		t.Fatal(err)
	}
	c.Base.Length = int64(len(wrongRoot))
	// Target instructions ignore the root bytes. Root identity must still fail.
	literal := deferredReaderBundle(t, []byte{1, 2, 2, 'A', 'B'})
	if err := backend.Put(t.Context(), c.Pack, literal, ""); err != nil {
		t.Fatal(err)
	}
	c.Length = int64(len(literal))
	if got, err := s.readChunk(t.Context(), c); err == nil || got != nil {
		t.Fatalf("wrong root accepted: %q %v", got, err)
	}
	if _, ok := s.idx.cache.get("data/" + c.Base.Hash); ok {
		t.Fatal("unverified root entered cache")
	}

	s, backend, c = deferredReaderFixture(t, false, false)
	root := c.Base.chunk()
	if got, err := s.readChunk(t.Context(), root); err != nil || string(got) != "A" {
		t.Fatal("root read", err)
	}
	root.Hash = deferredReaderHash([]byte("Z"), false)
	before := backend.gets()
	if got, err := s.readChunk(t.Context(), root); err == nil || got != nil || backend.gets()-before != 1 {
		t.Fatalf("different OID reused prior authentication: %q %v", got, err)
	}
}

func TestDeferredNativeReaderRejectsBounds(t *testing.T) {
	for name, change := range map[string]func(*chunk){
		"uppercase_oid":  func(c *chunk) { c.Hash = "git-sha1:" + strings.Repeat("A", 40) },
		"invalid_oid":    func(c *chunk) { c.Hash = "git-sha1:" + strings.Repeat("z", 40) },
		"short_oid":      func(c *chunk) { c.Hash = "git-sha1:1234" },
		"oversize_range": func(c *chunk) { c.Length = 2*ChunkSize + 1 },
		"nested_base":    func(c *chunk) { copy := *c.Base; c.Base.Base = &copy },
	} {
		t.Run(name, func(t *testing.T) {
			s, backend, c := deferredReaderFixture(t, false, false)
			change(&c)
			if got, err := s.readChunk(t.Context(), c); err == nil || got != nil || backend.gets() != 0 {
				t.Fatalf("invalid descriptor fetched or returned bytes: %v", err)
			}
		})
	}
	for name, wire := range map[string][]byte{
		"empty":           nil,
		"count":           bytes.Repeat([]byte{10, 0}, 64),
		"allocation_bomb": bytes.Repeat([]byte{10, 0}, ChunkSize),
		"oversize":        make([]byte, 2*ChunkSize+1),
		"truncated":       {10, 2, 8},
		"unknown_field":   {18, 0},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := deferredNativeBundle(wire); err == nil {
				t.Fatal("malformed bundle accepted")
			}
		})
	}
	for name, frame := range map[string]*probev1.NativeFrame{
		"raw_program":        {RawSize: 2*ChunkSize + 1},
		"compressed_program": {RawSize: 1, Zlib: make([]byte, 2*ChunkSize+1)},
		"intermediate":       {RawSize: 4, Zlib: deferredReaderZlib(t, []byte{1, 0x81, 0x80, 0x40})},
		"bad_zlib":           {RawSize: 4, Zlib: []byte("bad")},
	} {
		t.Run(name, func(t *testing.T) {
			s, backend, c := deferredReaderFixture(t, false, false)
			wire, err := proto.Marshal(&probev1.NativeProgramBundle{Frames: []*probev1.NativeFrame{frame}})
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.Put(t.Context(), c.Pack, wire, ""); err != nil {
				t.Fatal(err)
			}
			c.Length = int64(len(wire))
			if got, err := s.readChunk(t.Context(), c); err == nil || got != nil {
				t.Fatalf("invalid program accepted: %q %v", got, err)
			}
		})
	}
}

func TestDeferredNativeReaderMaximumChain(t *testing.T) {
	s, backend, c := deferredReaderFixture(t, false, false)
	programs := make([][]byte, 63)
	for i := range programs {
		programs[i] = []byte{1, 1, 0x90, 1}
	}
	programs[62] = []byte{1, 2, 0x90, 1, 1, 'B'}
	wire := deferredReaderBundle(t, programs...)
	if err := backend.Put(t.Context(), c.Pack, wire, ""); err != nil {
		t.Fatal(err)
	}
	c.Length = int64(len(wire))
	if got, err := s.readChunk(t.Context(), c); err != nil || string(got) != "AB" || backend.gets() != 2 {
		t.Fatalf("maximum bounded chain: %q %v", got, err)
	}
}
