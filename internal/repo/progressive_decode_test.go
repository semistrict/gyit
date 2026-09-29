package repo

import (
	"bytes"
	"compress/zlib"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	pb "gyit/internal/gen/gyit/storage/v1"
)

func packedDecodeFixture(tb testing.TB, raw []byte, declared int64, damage string) (context.Context, *pb.ProgressiveObject) {
	tb.Helper()
	var compressed bytes.Buffer
	z := zlib.NewWriter(&compressed)
	if _, err := z.Write(raw); err != nil {
		tb.Fatal(err)
	}
	if err := z.Close(); err != nil {
		tb.Fatal(err)
	}
	data := compressed.Bytes()
	switch damage {
	case "checksum":
		data[len(data)-1] ^= 1
	case "truncated":
		data = data[:len(data)-2]
	}
	pack := make([]byte, 12)
	header := byte(3<<4) | byte(declared&15)
	for remaining := uint64(declared) >> 4; ; remaining >>= 7 {
		if remaining != 0 {
			header |= 128
		}
		pack = append(pack, header)
		if remaining == 0 {
			break
		}
		header = byte(remaining & 127)
	}
	pack = append(pack, data...)
	pack = append(pack, make([]byte, 20)...)
	id := strings.Repeat("a", 40)
	source := &historySource{packs: []historyPack{{id: id, data: pack}}, inflaters: make(chan io.ReadCloser, 2)}
	ctx := context.WithValue(tb.Context(), historySourceKey{}, source)
	return ctx, &pb.ProgressiveObject{Pack: id, Offset: 12, PackSize: int64(len(pack)), Size: declared}
}

func TestProgressiveObjectDecodeLengths(t *testing.T) {
	for _, test := range []struct {
		name     string
		size     int
		declared int64
		damage   string
		valid    bool
	}{
		{"empty", 0, 0, "", true},
		{"exact", 32768, 32768, "", true},
		{"short", 32767, 32768, "", false},
		{"long", 32769, 32768, "", false},
		{"empty-with-data", 1, 0, "", false},
		{"checksum", 32768, 32768, "checksum", false},
		{"empty-checksum", 0, 0, "checksum", false},
		{"truncated", 32768, 32768, "truncated", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := bytes.Repeat([]byte{'x'}, test.size)
			ctx, object := packedDecodeFixture(t, raw, test.declared, test.damage)
			budget := int64(progressiveObjectLimit)
			p := &Progressive{}
			got, _, err := p.decodeObjectWithOrigin(ctx, object, 0, &budget)
			if !test.valid {
				if err == nil {
					t.Fatal("accepted an invalid object body")
				}
				return
			}
			if err != nil || len(got) == 0 || got[0] != 3 || !bytes.Equal(got[1:], raw) {
				t.Fatalf("decoded bytes=%d err=%v", len(got), err)
			}
			budget = test.declared - 1
			if _, _, err := p.decodeObjectWithOrigin(ctx, object, 0, &budget); err == nil {
				t.Fatal("decoded beyond the working-memory budget")
			}
		})
	}
}

func BenchmarkProgressiveObjectDecode(b *testing.B) {
	for _, size := range []int{256, 4 << 10, 64 << 10, 1 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			raw := bytes.Repeat([]byte("tree entry and object identity\n"), size/31+1)[:size]
			ctx, object := packedDecodeFixture(b, raw, int64(size), "")
			p := &Progressive{}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for b.Loop() {
				budget := int64(progressiveObjectLimit)
				got, _, err := p.decodeObjectWithOrigin(ctx, object, 0, &budget)
				if err != nil || len(got) != size+1 || got[0] != 3 {
					b.Fatalf("decoded bytes=%d err=%v", len(got), err)
				}
			}
		})
	}
}
