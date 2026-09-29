//go:build !js

package repo

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"testing"
)

func historyTreeBenchmarkFixture(tb testing.TB, changes int) (*Progressive, context.Context, string, string) {
	tb.Helper()
	source := &historySource{indexedByGit: true, cache: newCache(32 << 20)}
	add := func(raw []byte) string {
		h := sha1.New()
		fmt.Fprintf(h, "tree %d%c", len(raw), 0)
		h.Write(raw)
		oid := hex.EncodeToString(h.Sum(nil))
		source.cache.put("progressive-object/"+oid, append([]byte{2}, raw...))
		return oid
	}
	var before, after bytes.Buffer
	for i := range 10000 {
		fmt.Fprintf(&before, "100644 file-%05d%c", i, 0)
		fmt.Fprintf(&after, "100644 file-%05d%c", i, 0)
		oid := sha1.Sum([]byte(fmt.Sprint(i)))
		before.Write(oid[:])
		if i < changes {
			oid[0] ^= 1
		}
		after.Write(oid[:])
	}
	return &Progressive{slots: make(chan struct{}, 2)}, context.WithValue(context.Background(), historySourceKey{}, source), add(before.Bytes()), add(after.Bytes())
}

func BenchmarkHistoryTreeDelta(b *testing.B) {
	for _, changes := range []int{1, 10000} {
		b.Run(fmt.Sprintf("changed-%d", changes), func(b *testing.B) {
			p, ctx, before, after := historyTreeBenchmarkFixture(b, changes)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				count := 0
				if err := p.historyTreeDelta(ctx, after, before, func(string) error { count++; return nil }); err != nil {
					b.Fatal(err)
				}
				if count != changes+1 {
					b.Fatalf("emitted %d changes, want %d including root", count, changes+1)
				}
			}
		})
	}
}
