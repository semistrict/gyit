package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"

	"gyit/internal/orderedrows"
	archivewire "gyit/internal/archive/wire"
	storagev1 "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
	"google.golang.org/protobuf/proto"
)

func orderedDirectID(n byte) []byte { return bytes.Repeat([]byte{n}, 20) }

func orderedDirectArchive(t *testing.T, n byte) *storagev1.DirectBlobPart {
	t.Helper()
	id := orderedDirectID(n)
	var oid [20]byte
	copy(oid[:], id)
	r := archivewire.Recipe{ArchiveID: [16]byte{7}, PackSize: 64, TargetOID: oid,
		Frames: []archivewire.Frame{{HeaderOffset: 12, Offset: 13, Length: 5, RawSize: 1, Size: 1, OID: oid}}}
	encoded, err := archivewire.Encode(r)
	if err != nil {
		t.Fatal(err)
	}
	return &storagev1.DirectBlobPart{Oid: id, Size: 1, Chunk: &storagev1.ChunkRecord{Hash: fmt.Sprintf("git-sha1:%x", id), ArchiveRecipe: encoded}}
}

type orderedDirectRow struct{ key, value []byte }

func orderedDirectRows(t *testing.T, parts ...*storagev1.DirectBlobPart) []orderedDirectRow {
	t.Helper()
	var rows []orderedDirectRow
	for _, p := range parts {
		v, err := proto.MarshalOptions{Deterministic: true}.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, orderedDirectRow{directPartKey(p), v})
	}
	return rows
}

func orderedDirectCursor(rows []orderedDirectRow) orderedrows.Cursor {
	i := 0
	return func(context.Context) ([]byte, []byte, error) {
		if i == len(rows) {
			return nil, nil, io.EOF
		}
		r := rows[i]
		i++
		return r.key, r.value, nil
	}
}

func orderedDirectStage(t *testing.T) *directBlobStage {
	t.Helper()
	d, err := newDirectBlobStage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.records.Close(); err != nil {
			t.Error(err)
		}
	})
	return d
}

func orderedDirectFallback(t *testing.T, d *directBlobStage, id byte, size int64, parts ...int64) {
	t.Helper()
	oid := fmt.Sprintf("%x", orderedDirectID(id))
	// Staging is intentionally not final-key ordered.
	for _, part := range parts {
		if _, err := d.add(chunkKey(oid, part), chunk{Pack: "packs/test", Length: 5, Hash: fmt.Sprintf("%064x", id)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.add("o/"+oid, object{Kind: "blob", Size: size}); err != nil {
		t.Fatal(err)
	}
}

func TestOrderedDirectLayouts(t *testing.T) {
	for _, mode := range []string{"zero", "archive_only", "fallback_only", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			d := orderedDirectStage(t)
			var archive []*storagev1.DirectBlobPart
			if mode == "archive_only" || mode == "mixed" {
				archive = []*storagev1.DirectBlobPart{orderedDirectArchive(t, 2), orderedDirectArchive(t, 4)}
			}
			if mode == "fallback_only" || mode == "mixed" {
				orderedDirectFallback(t, d, 1, 0)
				orderedDirectFallback(t, d, 3, 2*ChunkSize+1, 2, 0, 1)
			}
			backend, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			root, err := d.buildOrdered(t.Context(), backend, orderedDirectCursor(orderedDirectRows(t, archive...)))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "zero" {
				if root != (pageRef{}) {
					t.Fatal("empty input returned a root")
				}
				return
			}
			s := &Snapshot{idx: &index{store: backend, cache: newCache(32 << 20), blobRoot: root}}
			for _, p := range archive {
				size, c, err := s.readBlobPart(t.Context(), fmt.Sprintf("%x", p.Oid), 0)
				if err != nil || size != int64(p.Size) || c.ArchiveRecipe != string(p.Chunk.ArchiveRecipe) {
					t.Fatalf("archive readback size=%d err=%v", size, err)
				}
			}
			if mode == "fallback_only" || mode == "mixed" {
				if size, c, err := s.readBlobPart(t.Context(), fmt.Sprintf("%x", orderedDirectID(1)), 0); err != nil || size != 0 || c != (chunk{}) {
					t.Fatalf("empty readback size=%d err=%v", size, err)
				}
				for part := int64(0); part < 3; part++ {
					size, c, err := s.readBlobPart(t.Context(), fmt.Sprintf("%x", orderedDirectID(3)), part)
					if err != nil || size != 2*ChunkSize+1 || c.Pack != "packs/test" {
						t.Fatalf("fallback readback part=%d size=%d err=%v", part, size, err)
					}
				}
			}
		})
	}
}

func TestOrderedDirectRejectsInvalidInputs(t *testing.T) {
	tests := []string{"duplicate", "out_of_order", "key_mismatch", "missing_chunk", "oversized_recipe", "oversized_row", "oversized_size", "archive_empty", "mixed_chunk_union", "missing_size", "missing_first_part", "missing_last_part", "empty_with_chunk"}
	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			d := orderedDirectStage(t)
			p := orderedDirectArchive(t, 2)
			parts := []*storagev1.DirectBlobPart{p}
			switch name {
			case "duplicate":
				orderedDirectFallback(t, d, 2, 1, 0)
			case "out_of_order":
				parts = []*storagev1.DirectBlobPart{orderedDirectArchive(t, 4), p}
			case "missing_chunk":
				p.Chunk = nil
			case "oversized_recipe":
				p.Chunk.ArchiveRecipe = make([]byte, 8193)
			case "oversized_size":
				p.Size = math.MaxUint64
			case "archive_empty":
				p.Size, p.Chunk = 0, nil
			case "mixed_chunk_union":
				p.Chunk.Pack = "packs/other"
			case "missing_size":
				_, err := d.add(chunkKey(fmt.Sprintf("%x", orderedDirectID(3)), 0), chunk{Pack: "packs/test"})
				if err != nil {
					t.Fatal(err)
				}
			case "missing_first_part":
				orderedDirectFallback(t, d, 3, ChunkSize+1, 1)
			case "missing_last_part":
				orderedDirectFallback(t, d, 3, ChunkSize+1, 0)
			case "empty_with_chunk":
				orderedDirectFallback(t, d, 3, 0, 0)
			}
			rows := orderedDirectRows(t, parts...)
			if name == "key_mismatch" {
				rows[0].key[0]++
			}
			if name == "oversized_row" {
				rows[0].value = make([]byte, directBlobPageBytes+1)
			}
			backend, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if root, err := d.buildOrdered(t.Context(), backend, orderedDirectCursor(rows)); err == nil || root != (pageRef{}) {
				t.Fatalf("invalid input returned root=%v err=%v", root, err)
			}
		})
	}
}

type orderedDirectFailStore struct {
	store.Store
	err error
}

func (s orderedDirectFailStore) Put(context.Context, string, []byte, string) error { return s.err }

func TestOrderedDirectFailureAndCancellation(t *testing.T) {
	cause := errors.New("ordered direct write failure")
	d := orderedDirectStage(t)
	root, err := d.buildOrdered(t.Context(), orderedDirectFailStore{err: cause}, orderedDirectCursor(orderedDirectRows(t, orderedDirectArchive(t, 2))))
	if !errors.Is(err, cause) || root != (pageRef{}) {
		t.Fatalf("backend failure root=%v err=%v", root, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	archive := func(context.Context) ([]byte, []byte, error) { cancel(); return nil, nil, context.Canceled }
	root, err = orderedDirectStage(t).buildOrdered(ctx, orderedDirectFailStore{err: cause}, archive)
	if !errors.Is(err, context.Canceled) || root != (pageRef{}) {
		t.Fatalf("cancellation root=%v err=%v", root, err)
	}
	closed, returned := false, false
	next, stop := pullDirectParts(t.Context(), func(ctx context.Context, emit func(*storagev1.DirectBlobPart) error) error {
		defer func() { closed = true }()
		err := emit(orderedDirectArchive(t, 2))
		returned = true
		return err
	})
	if _, _, err := next(t.Context()); err != nil || closed {
		t.Fatalf("producer did not suspend: closed=%v err=%v", closed, err)
	}
	stop()
	if !closed || !returned {
		t.Fatal("stop returned before producer cleanup")
	}
	stop()
}
