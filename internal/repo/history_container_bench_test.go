package repo

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
	"testing"
)

func historyContainerDecodeFixture(tb testing.TB) ([]byte, map[int64][]byte) {
	tb.Helper()
	encoder, err := newCompressor()
	if err != nil {
		tb.Fatal(err)
	}
	defer encoder.Close()
	random := rand.New(rand.NewSource(123))
	var packed []byte
	frames := map[int64][]byte{}
	for i := range 48 {
		raw := make([]byte, 8192)
		random.Read(raw[:4096])
		copy(raw[4096:], bytes.Repeat([]byte{byte(i)}, 4096))
		frames[int64(len(packed))] = raw
		packed = encoder.EncodeAll(raw, packed)
	}
	if len(packed) > progressiveContainerBytes {
		tb.Fatal("fixture exceeds a metadata container")
	}
	return packed, frames
}

func TestHistoryContainerIndependentFrames(t *testing.T) {
	packed, want := historyContainerDecodeFixture(t)
	got, err := decodeDirectoryContainer(t.Context(), packed)
	if err != nil || len(got) != len(want) {
		t.Fatalf("decoded %d frames: %v", len(got), err)
	}
	for offset, raw := range want {
		frame := got[offset]
		if !bytes.Equal(frame.raw, raw) {
			t.Fatalf("frame at %d differs", offset)
		}
		hash := sha256.Sum256(packed[offset : offset+frame.length])
		if frame.hash != hex.EncodeToString(hash[:]) {
			t.Fatal("frame hash differs")
		}
	}
}

func BenchmarkHistoryContainerDecode(b *testing.B) {
	packed, want := historyContainerDecodeFixture(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(packed)))
	b.ResetTimer()
	for b.Loop() {
		got, err := decodeDirectoryContainer(b.Context(), packed)
		if err != nil || len(got) != len(want) {
			b.Fatal(err)
		}
	}
}
