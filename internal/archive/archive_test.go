package archive

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"gat/internal/store"
)

type recordingStore struct {
	store.Store
	keys   []string
	bodies [][]byte
	failAt int
}

func (s *recordingStore) Put(_ context.Context, key string, body []byte, token string) error {
	if token != "*" {
		return errors.New("archive writes must be create-only")
	}
	if s.failAt > 0 && len(s.keys)+1 == s.failAt {
		return errors.New("injected storage failure")
	}
	s.keys = append(s.keys, key)
	s.bodies = append(s.bodies, bytes.Clone(body))
	return nil
}

func TestArchivePreservesBytesAndImmutableSegments(t *testing.T) {
	want := []byte("abcdefghijklmno")
	var id [16]byte
	id[0] = 0xaa
	s := &recordingStore{}
	got, err := copySegments(t.Context(), s, bytes.NewReader(want), int64(len(want)), id, 6)
	if err != nil || got.Bytes != 15 || got.Segments != 3 {
		t.Fatalf("copy result %+v: %v", got, err)
	}
	if !bytes.Equal(bytes.Join(s.bodies, nil), want) {
		t.Fatal("archived bytes changed")
	}
	for i, n := range []int{6, 6, 3} {
		if len(s.bodies[i]) != n || s.keys[i] != Key(id, uint64(i)) {
			t.Fatal("incorrect segment boundaries/key")
		}
	}
	if Key(id, 0) != "packs/archive-aa000000000000000000000000000000/00000000" {
		t.Fatal("unstable archive namespace")
	}
}

func TestArchiveStopsOnSourceStoreAndCancellationErrors(t *testing.T) {
	for _, kind := range []string{"short", "storage", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s := &recordingStore{}
			source := bytes.NewReader([]byte("abcdef"))
			size := int64(6)
			switch kind {
			case "short":
				size = 9
			case "storage":
				s.failAt = 2
			case "cancel":
				cancel()
			}
			got, err := copySegments(ctx, s, source, size, [16]byte{}, 4)
			if err == nil {
				t.Fatal("incomplete archive accepted")
			}
			if kind == "short" && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatal("source error lost", err)
			}
			if kind == "cancel" && (!errors.Is(err, context.Canceled) || len(s.keys) != 0) {
				t.Fatal("canceled archive wrote data")
			}
			if kind != "cancel" && (got.Segments != 1 || got.Bytes != 4 || len(s.keys) != 1) {
				t.Fatalf("continued after error: %+v", got)
			}
		})
	}
}

func TestArchiveRejectsInvalidSourceLength(t *testing.T) {
	for _, size := range []int64{0, -1} {
		s := &recordingStore{}
		if _, err := Copy(t.Context(), s, bytes.NewReader(nil), size, [16]byte{}); err == nil || len(s.keys) != 0 {
			t.Fatal("invalid source accepted")
		}
	}
}
