package repo

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"
)

type countingHistoryInflater struct {
	io.ReadCloser
	starts int
}

func (r *countingHistoryInflater) Reset(input io.Reader, dictionary []byte) error {
	r.starts++
	return r.ReadCloser.(zlib.Resetter).Reset(input, dictionary)
}

// A REF_DELTA and its base exercise actual recipe lookup and recursive decode.
func historyRecipeFixture(tb testing.TB) (*Progressive, context.Context, string, []byte, *countingHistoryInflater) {
	tb.Helper()
	base := bytes.Repeat([]byte("abcdefgh"), 8192)
	result := append(bytes.Clone(base), 'X')
	oid := func(raw []byte) []byte {
		h := sha1.New()
		fmt.Fprintf(h, "blob %d%c", len(raw), 0)
		h.Write(raw)
		return h.Sum(nil)
	}
	baseID, resultID := oid(base), oid(result)
	pack := make([]byte, 12)
	var compressed []byte
	add := func(kind byte, raw, reference []byte) int {
		offset := len(pack)
		size := uint64(len(raw))
		header := kind<<4 | byte(size&15)
		for size >>= 4; ; size >>= 7 {
			if size != 0 {
				header |= 128
			}
			pack = append(pack, header)
			if size == 0 {
				break
			}
			header = byte(size & 127)
		}
		pack = append(pack, reference...)
		var buf bytes.Buffer
		z := zlib.NewWriter(&buf)
		if _, err := z.Write(raw); err != nil {
			tb.Fatal(err)
		}
		if err := z.Close(); err != nil {
			tb.Fatal(err)
		}
		compressed = buf.Bytes()
		pack = append(pack, compressed...)
		return offset
	}
	offsets := map[string]int{string(baseID): add(3, base, nil)}
	delta := binary.AppendUvarint(nil, uint64(len(base)))
	delta = binary.AppendUvarint(delta, uint64(len(result)))
	delta = append(delta, 0x80, 1, 'X') // copy 64 KiB, then append X
	offsets[string(resultID)] = add(7, delta, baseID)
	pack = append(pack, make([]byte, 20)...)
	ids := [][]byte{baseID, resultID}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i], ids[j]) < 0 })
	index := make([]byte, 1072+28*len(ids))
	for i, id := range ids {
		copy(index[1032+20*i:], id)
		binary.BigEndian.PutUint32(index[1032+24*len(ids)+4*i:], uint32(offsets[string(id)]))
	}
	inflater, err := zlib.NewReader(bytes.NewReader(compressed))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { inflater.Close() })
	counter := &countingHistoryInflater{ReadCloser: inflater}
	source := &historySource{packs: []historyPack{{id: strings.Repeat("a", 40), index: index, data: pack, count: len(ids)}}, cache: newCache(0), inflaters: make(chan io.ReadCloser, 1)}
	source.inflaters <- counter
	p := &Progressive{cache: newCache(0), slots: make(chan struct{}, 4)}
	return p, context.WithValue(tb.Context(), historySourceKey{}, source), hex.EncodeToString(resultID), result, counter
}

func TestHistoryObjectDecodesEachPackedMemberOnce(t *testing.T) {
	p, ctx, oid, want, counter := historyRecipeFixture(t)
	got, release, err := p.borrowObject(ctx, oid)
	defer release()
	if err != nil || len(got) == 0 || got[0] != 3 || !bytes.Equal(got[1:], want) {
		t.Fatalf("object differs: %v", err)
	}
	if counter.starts != 2 {
		t.Fatalf("two packed members require two inflater starts, got %d", counter.starts)
	}
	// A metadata-only size query must still return the reconstructed size.
	size, err := p.ObjectSize(ctx, oid)
	if err != nil || size != int64(len(want)) {
		t.Fatalf("size=%d: %v", size, err)
	}
}

func TestHistoryLocatedObjectStillChecksIdentity(t *testing.T) {
	p, ctx, oid, _, _ := historyRecipeFixture(t)
	source := ctx.Value(historySourceKey{}).(*historySource)
	id, _ := hex.DecodeString(oid)
	changed := bytes.Clone(id)
	changed[19] ^= 1
	for i := range source.packs[0].count {
		entry := source.packs[0].index[1032+20*i : 1032+20*(i+1)]
		if bytes.Equal(entry, id) {
			copy(entry, changed)
		}
	}
	_, release, err := p.borrowObject(ctx, hex.EncodeToString(changed))
	defer release()
	if err == nil || !strings.Contains(err.Error(), "object identity mismatch") {
		t.Fatalf("wrong object accepted: %v", err)
	}
}

func BenchmarkHistoryObjectRecipe(b *testing.B) {
	p, ctx, oid, want, _ := historyRecipeFixture(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(want)))
	for b.Loop() {
		got, release, err := p.borrowObject(ctx, oid)
		release()
		if err != nil || len(got) != len(want)+1 {
			b.Fatal(err)
		}
	}
}

func TestHistoryDecodedCacheSharesObjectAndOffset(t *testing.T) {
	p, ctx, oid, want, counter := historyRecipeFixture(t)
	source := ctx.Value(historySourceKey{}).(*historySource)
	source.cache = newCache(1 << 20)
	got, release, err := p.borrowObject(ctx, oid)
	release()
	if err != nil || len(got) != len(want)+1 {
		t.Fatal(err)
	}
	recipe, _, err := source.locate(oid)
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("offset/%s/%d", recipe.Pack, recipe.Offset)
	// Only the base and reconstructed object should consume decoded capacity.
	expected := len(want) - 1 + 1 + len(want) + 1
	if source.cache.used != expected {
		t.Fatalf("two objects use %d bytes, want %d", source.cache.used, expected)
	}
	starts := counter.starts
	for range 3 {
		_, release, err := p.borrowObject(ctx, oid)
		release()
		if err != nil {
			t.Fatal(err)
		}
	}
	if counter.starts != starts {
		t.Fatal("cached object was decoded again")
	}
	// Eviction must remove both names; aliases cannot retain unaccounted data.
	source.cache.put("evict", make([]byte, source.cache.max))
	for _, name := range []string{key, "progressive-object/" + oid} {
		if _, ok := source.cache.get(name); ok {
			t.Fatalf("evicted object retained under %s", name)
		}
	}
	if len(source.cache.items) != 1 || source.cache.used != source.cache.max {
		t.Fatal("eviction left cache entries or incorrect accounting")
	}
}

// Neighboring versions often share a decoded delta base. Keep that base warm
// while every timed request resolves a previously uncached result object.
func BenchmarkHistoryObjectWithCachedBase(b *testing.B) {
	p, ctx, oid, want, _ := historyRecipeFixture(b)
	source := ctx.Value(historySourceKey{}).(*historySource)
	base := append([]byte{3}, want[:len(want)-1]...)
	baseKey := fmt.Sprintf("offset/%s/%d", source.packs[0].id, 12)
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		source.cache = newCache(1 << 20)
		source.cache.put(baseKey, base)
		b.StartTimer()
		got, release, err := p.borrowObject(ctx, oid)
		release()
		if err != nil || len(got) != len(want)+1 {
			b.Fatal(err)
		}
	}
}
