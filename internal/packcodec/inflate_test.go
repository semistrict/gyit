package wirecodec

import (
	"bytes"
	"compress/zlib"
	"io"
	"math/rand"
	"testing"
)

func fixtureFrame(t *testing.T, raw []byte) []byte {
	t.Helper()
	var dst bytes.Buffer
	w := zlib.NewWriter(&dst)
	if _, err := w.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return dst.Bytes()
}

func TestInflateFrameBoundaries(t *testing.T) {
	random := make([]byte, 32<<10)
	rand.New(rand.NewSource(17)).Read(random)
	for name, raw := range map[string][]byte{"empty": {}, "repeated": bytes.Repeat([]byte("sample\n"), 257), "random": random} {
		t.Run(name, func(t *testing.T) {
			frame := fixtureFrame(t, raw)
			guarded := bytes.Repeat([]byte{0xa5}, len(raw)+16)
			out := guarded[8 : 8+len(raw)]
			if err := Inflate(out, frame); err != nil || !bytes.Equal(out, raw) {
				t.Fatalf("exact inflate: err=%v output matches=%t", err, bytes.Equal(out, raw))
			}
			if !bytes.Equal(guarded[:8], bytes.Repeat([]byte{0xa5}, 8)) || !bytes.Equal(guarded[8+len(raw):], bytes.Repeat([]byte{0xa5}, 8)) {
				t.Fatal("inflate wrote outside its destination")
			}
			tail := fixtureFrame(t, []byte("next packed object"))
			withTail := append(bytes.Clone(frame), tail...)
			consumed, err := InflatePrefix(out, withTail)
			if err != nil || consumed != len(frame) || !bytes.Equal(out, raw) {
				t.Fatalf("prefix: consumed=%d want=%d err=%v", consumed, len(frame), err)
			}
			if err := Inflate(out, withTail); err == nil {
				t.Fatal("exact inflate accepted an extra frame")
			}
			if _, err := InflateBounded(withTail, max(1, len(raw))); err == nil {
				t.Fatal("bounded inflate accepted an extra frame")
			}
			bounded, err := InflateBounded(frame, max(1, len(raw)))
			if err != nil || !bytes.Equal(bounded, raw) {
				t.Fatalf("bounded inflate: err=%v output matches=%t", err, bytes.Equal(bounded, raw))
			}
		})
	}
}

func TestInflateRejectsMalformedFrames(t *testing.T) {
	raw := []byte("verify the checksum and complete frame")
	frame := fixtureFrame(t, raw)
	check := func(t *testing.T, bad []byte) {
		t.Helper()
		if err := Inflate(make([]byte, len(raw)), bad); err == nil {
			t.Fatal("exact inflate accepted malformed frame")
		}
		if _, err := InflatePrefix(make([]byte, len(raw)), bad); err == nil {
			t.Fatal("prefix inflate accepted malformed frame")
		}
		if _, err := InflateBounded(bad, len(raw)+1); err == nil {
			t.Fatal("bounded inflate accepted malformed frame")
		}
	}
	for cut := 0; cut < len(frame); cut++ {
		check(t, frame[:cut])
	}
	for _, pos := range []int{0, 1, len(frame) - 1} {
		bad := bytes.Clone(frame)
		bad[pos] ^= 0xff
		check(t, bad)
	}
}

func TestInflateOutputBounds(t *testing.T) {
	raw := []byte("bounded output")
	frame := fixtureFrame(t, raw)
	for _, size := range []int{0, len(raw) - 1, len(raw) + 1} {
		if err := Inflate(make([]byte, size), frame); err == nil {
			t.Fatalf("exact inflate accepted output size %d", size)
		}
		if _, err := InflatePrefix(make([]byte, size), frame); err == nil {
			t.Fatalf("prefix inflate accepted output size %d", size)
		}
	}
	for _, limit := range []int{-1, 0, len(raw) - 1, (2 << 20) + 1} {
		if _, err := InflateBounded(frame, limit); err == nil {
			t.Fatalf("bounded inflate accepted limit %d", limit)
		}
	}
	got, err := InflateBounded(frame, len(raw)+1)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("shorter output within bound: %q %v", got, err)
	}
	// A second call must not overwrite the caller's retained result through the
	// scratch pool, and the maximum accepted limit remains a hard decode cap.
	large := bytes.Repeat([]byte{0x5a}, (2<<20)+1)
	if _, err := InflateBounded(fixtureFrame(t, large), 2<<20); err == nil {
		t.Fatal("accepted output larger than the maximum decode cap")
	}
	if !bytes.Equal(got, raw) {
		t.Fatal("a later decode overwrote an earlier result")
	}
}

func TestDeflateInteroperability(t *testing.T) {
	if _, err := Deflate(nil); err == nil {
		t.Fatal("deflate accepted empty input")
	}
	raw := bytes.Repeat([]byte("native and Go readers share the zlib format\n"), 1024)
	frame, err := Deflate(raw)
	if err != nil {
		t.Fatal(err)
	}
	input := bytes.NewReader(frame)
	r, err := zlib.NewReader(input)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, raw) || input.Len() != 0 {
		t.Fatal("deflate produced the wrong data or trailing bytes")
	}
	decoded := make([]byte, len(raw))
	if err := Inflate(decoded, frame); err != nil || !bytes.Equal(decoded, raw) {
		t.Fatalf("round trip: %v", err)
	}
}
