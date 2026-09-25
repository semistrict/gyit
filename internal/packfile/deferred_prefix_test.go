package packrecipe

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"testing"
)

func deferredPrefixFrame(t *testing.T, raw []byte, level int) []byte {
	t.Helper()
	var buffer bytes.Buffer
	w, err := zlib.NewWriterLevel(&buffer, level)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func TestDeferredPrefixBoundedOutput(t *testing.T) {
	for _, level := range []int{zlib.NoCompression, zlib.BestSpeed, zlib.DefaultCompression, zlib.BestCompression} {
		for _, size := range []int{2, 20, 21, 100000} {
			t.Run(fmt.Sprintf("level-%d/bytes-%d", level, size), func(t *testing.T) {
				raw := bytes.Repeat([]byte("prefix metadata and deferred contents\n"), (size+36)/37)[:size]
				frame := deferredPrefixFrame(t, raw, level)
				guarded := bytes.Repeat([]byte{0xa5}, 28)
				want := min(size, 20)
				n, err := inflateDeferredPrefix(guarded[4:4+want], frame, size)
				if err != nil || n != want || !bytes.Equal(guarded[4:4+n], raw[:want]) {
					t.Fatalf("prefix: n=%d err=%v", n, err)
				}
				if !bytes.Equal(guarded[:4], bytes.Repeat([]byte{0xa5}, 4)) || !bytes.Equal(guarded[4+want:], bytes.Repeat([]byte{0xa5}, 24-want)) {
					t.Fatal("prefix wrote outside the destination")
				}
				if size > 20 {
					frame[len(frame)-1] ^= 0x80
					n, err = inflateDeferredPrefix(guarded[4:24], frame, size)
					if err != nil || n != 20 || !bytes.Equal(guarded[4:24], raw[:20]) {
						t.Fatalf("prefix authenticated an unread trailer: n=%d err=%v", n, err)
					}
				}
			})
		}
	}
}

func TestDeferredPrefixRejectsMalformedShortPrograms(t *testing.T) {
	raw := []byte{0x80, 0x20, 0x80, 0x20}
	frame := deferredPrefixFrame(t, raw, zlib.DefaultCompression)
	for cut := 0; cut < len(frame); cut++ {
		if _, err := inflateDeferredPrefix(make([]byte, len(raw)), frame[:cut], len(raw)); err == nil {
			t.Fatalf("accepted incomplete short frame ending at %d", cut)
		}
	}
	bad := bytes.Clone(frame)
	bad[len(bad)-1] ^= 1
	if _, err := inflateDeferredPrefix(make([]byte, len(raw)), bad, len(raw)); err == nil {
		t.Fatal("accepted invalid short-program checksum")
	}
	for _, size := range []int{0, 3, 5} {
		if _, err := inflateDeferredPrefix(make([]byte, min(size, 20)), frame, size); err == nil {
			t.Fatalf("accepted wrong declared program size %d", size)
		}
	}
	twenty := deferredPrefixFrame(t, bytes.Repeat([]byte{1}, 20), zlib.DefaultCompression)
	if _, err := inflateDeferredPrefix(make([]byte, 20), twenty, 21); err == nil {
		t.Fatal("accepted completed frame shorter than declared at the prefix boundary")
	}
}
