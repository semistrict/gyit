package repo

import (
	"bytes"
	"compress/zlib"
	"io"
	"testing"
)

func historyInflaterFixture(tb testing.TB) ([]byte, []byte) {
	tb.Helper()
	raw := bytes.Repeat([]byte("repeated object data\n"), 1024)
	var compressed bytes.Buffer
	z := zlib.NewWriter(&compressed)
	if _, err := z.Write(raw); err != nil {
		tb.Fatal(err)
	}
	if err := z.Close(); err != nil {
		tb.Fatal(err)
	}
	return raw, compressed.Bytes()
}

func TestHistoryInflaterReuse(t *testing.T) {
	raw, compressed := historyInflaterFixture(t)
	s := &historySource{inflaters: make(chan io.ReadCloser, 2)}
	for i := range 20 {
		z, err := s.inflater(bytes.NewReader(compressed))
		if err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			// Size lookup reads only the delta header, then returns the reader.
			got := make([]byte, 5)
			if _, err := io.ReadFull(z, got); err != nil || !bytes.Equal(got, raw[:5]) {
				t.Fatalf("prefix %q: %v", got, err)
			}
		} else {
			got, err := io.ReadAll(z)
			if err != nil || !bytes.Equal(got, raw) {
				t.Fatalf("object after reset: %v", err)
			}
		}
		s.releaseInflater(z)
	}
	if _, err := s.inflater(bytes.NewReader([]byte("invalid"))); err == nil {
		t.Fatal("accepted malformed compressed header")
	}
	z, err := s.inflater(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(z)
	s.releaseInflater(z)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("object after malformed input: %v", err)
	}
}

func BenchmarkHistoryInflater(b *testing.B) {
	_, compressed := historyInflaterFixture(b)
	for _, reuse := range []bool{false, true} {
		name := "new"
		if reuse {
			name = "reuse"
		}
		b.Run(name, func(b *testing.B) {
			s := &historySource{}
			if reuse {
				s.inflaters = make(chan io.ReadCloser, 2)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				z, err := s.inflater(bytes.NewReader(compressed))
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, z); err != nil {
					b.Fatal(err)
				}
				s.releaseInflater(z)
			}
		})
	}
}
