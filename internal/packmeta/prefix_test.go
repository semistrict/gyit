package planner

import (
	"bytes"
	"errors"
	"testing"
)

func TestMetadataPrefixValidatesOnlyCompleteShortPrograms(t *testing.T) {
	for _, size := range []int{2, 19, 20, 21, 100000} {
		raw := bytes.Repeat([]byte{0x20}, size)
		frame := compressPrefix(t, raw)
		var out [PrefixOutputLimit]byte
		n, consumed, err := metadataPrefix(out[:], frame, uint64(size))
		if err != nil || n != min(size, PrefixOutputLimit) || consumed > PrefixInputLimit || !bytes.Equal(out[:n], raw[:n]) {
			t.Fatalf("valid prefix size=%d n=%d consumed=%d err=%v", size, n, consumed, err)
		}
		frame[len(frame)-1] ^= 0x80
		n, consumed, err = metadataPrefix(out[:], frame, uint64(size))
		if size <= PrefixOutputLimit {
			if !errors.Is(err, ErrMalformed) || errors.Is(err, ErrUnsupported) {
				t.Fatalf("short program checksum classification: %v", err)
			}
		} else if err != nil || n != PrefixOutputLimit || consumed > PrefixInputLimit || !bytes.Equal(out[:n], raw[:n]) {
			t.Fatalf("prefix authenticated the deferred trailer: n=%d consumed=%d err=%v", n, consumed, err)
		}
	}
}

func TestMetadataPrefixMalformedClassification(t *testing.T) {
	raw := []byte{1, 1}
	frame := compressPrefix(t, raw)
	var out [PrefixOutputLimit]byte
	for cut := 0; cut < len(frame); cut++ {
		_, _, err := metadataPrefix(out[:], frame[:cut], uint64(len(raw)))
		if !errors.Is(err, ErrMalformed) || errors.Is(err, ErrUnsupported) {
			t.Fatalf("truncation at %d classified as %v", cut, err)
		}
	}
	for _, size := range []uint64{0, 1, 3, 100} {
		if _, _, err := metadataPrefix(out[:], frame, size); !errors.Is(err, ErrMalformed) || errors.Is(err, ErrUnsupported) {
			t.Fatalf("wrong size %d classified as %v", size, err)
		}
	}
}
