package spill_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"gyit/internal/spill"
)

func TestSortedWalkKeepsLatestValuesAcrossSpills(t *testing.T) {
	parent := t.TempDir()
	s, err := spill.New(parent, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// More data than a single run or merge group; reverse input order and
	// duplicate every key in a later batch. Inputs may be reused immediately.
	for pass := 0; pass < 3; pass++ {
		for i := 2047; i >= 0; i-- {
			key := []byte(fmt.Sprintf("%04d", i))
			value := []byte(fmt.Sprintf("pass %d value %04d", pass, i))
			if err := s.Add(key, value); err != nil {
				t.Fatal(err)
			}
			clear(key)
			clear(value)
		}
	}
	injected := errors.New("consumer stopped")
	if err := s.Walk(t.Context(), func(_, _ []byte) error { return injected }); !errors.Is(err, injected) {
		t.Fatalf("lost error during disk merge: %v", err)
	}
	for round := 0; round < 2; round++ {
		next := 0
		if err := s.Walk(t.Context(), func(key, value []byte) error {
			if string(key) != fmt.Sprintf("%04d", next) || string(value) != fmt.Sprintf("pass 2 value %04d", next) {
				t.Fatalf("record %d: %q %q", next, key, value)
			}
			next++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if next != 2048 {
			t.Fatalf("lost records: %d", next)
		}
	}
	if err := s.Add([]byte("0000"), []byte("updated after walk")); err != nil {
		t.Fatal(err)
	}
	if err := s.Walk(t.Context(), func(k, v []byte) error {
		if string(k) == "0000" && string(v) != "updated after walk" {
			t.Fatalf("later update was lost: %q", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(parent)
	if err != nil || len(files) != 0 {
		t.Fatalf("temporary files remain: %v %v", files, err)
	}
}

func TestBinaryKeysEmptyInputsAndStoppedWalk(t *testing.T) {
	s, err := spill.New(t.TempDir(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Walk(t.Context(), func(_, _ []byte) error { t.Fatal("empty walk emitted a record"); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.Add([]byte{255, 0}, []byte("last")); err != nil {
		t.Fatal(err)
	}
	if err := s.Add([]byte{0, 255}, []byte("first")); err != nil {
		t.Fatal(err)
	}
	var keys [][]byte
	if err := s.Walk(t.Context(), func(k, _ []byte) error { keys = append(keys, bytes.Clone(k)); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || !bytes.Equal(keys[0], []byte{0, 255}) || !bytes.Equal(keys[1], []byte{255, 0}) {
		t.Fatalf("wrong binary order: %x", keys)
	}
	injected := errors.New("stop")
	if err := s.Walk(t.Context(), func(_, _ []byte) error { return injected }); !errors.Is(err, injected) {
		t.Fatalf("lost callback error: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.Walk(ctx, func(_, _ []byte) error { t.Fatal("emitted after cancellation"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("lost cancellation: %v", err)
	}
	if err := s.Add([]byte("large"), make([]byte, 4096)); err == nil {
		t.Fatal("accepted record larger than run budget")
	}
}
