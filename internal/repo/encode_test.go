package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestChunkEncoderOwnsBuffersAndPreservesOrder(t *testing.T) {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	seen := 0
	encoder, err := newChunkEncoder(t.Context(), 4, 1, 4, nil, func(slot *encodeSlot) error {
		key, compressed, hash := slot.key, slot.compressed, slot.hash
		if key != fmt.Sprint(seen) {
			t.Fatalf("chunk order: %s, want %d", key, seen)
		}
		raw, err := dec.DecodeAll(compressed, nil)
		if err != nil {
			return err
		}
		expected := bytes.Repeat([]byte{byte(seen)}, 8192+seen)
		if !bytes.Equal(raw, expected) || hash != fmt.Sprintf("%x", sha256.Sum256(expected)) {
			t.Fatal("chunk bytes or hash changed")
		}
		seen++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.close()
	// Cross the complete 64-slot ring several times so reused buffers and the
	// final partial drain both exercise ordering and caller ownership.
	const count = 193
	for i := 0; i < count; i++ {
		buf := bytes.Repeat([]byte{byte(i)}, 8192+i)
		if err := encoder.add(fmt.Sprint(i), buf); err != nil {
			t.Fatal(err)
		}
		clear(buf)
		if i+1-seen > 64 {
			t.Fatal("unbounded pending chunks")
		}
	}
	if err := encoder.finish(); err != nil {
		t.Fatal(err)
	}
	if seen != count {
		t.Fatalf("lost tail chunks: %d", seen)
	}
}

func TestChunkEncoderFailureAndCancellation(t *testing.T) {
	injected := errors.New("upload failed")
	encoder, err := newChunkEncoder(t.Context(), 2, 1, 4, nil, func(*encodeSlot) error { return injected })
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := encoder.add("chunk", []byte("content")); err != nil {
			t.Fatal(err)
		}
	}
	if err := encoder.finish(); !errors.Is(err, injected) {
		t.Fatalf("lost write error: %v", err)
	}
	encoder.close()
	ctx, cancel := context.WithCancel(t.Context())
	canceled, err := newChunkEncoder(ctx, 2, 1, 4, nil, func(*encodeSlot) error { t.Fatal("published after cancellation"); return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer canceled.close()
	if err := canceled.add("chunk", []byte("content")); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := canceled.finish(); !errors.Is(err, context.Canceled) {
		t.Fatalf("lost cancellation: %v", err)
	}
}
