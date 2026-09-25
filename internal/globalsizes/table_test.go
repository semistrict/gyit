package table

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"math/bits"
	"reflect"
	"testing"
	"unsafe"

	"gat/internal/globalsizes/wire"
)

func oid(i uint64) (out [20]byte) {
	binary.LittleEndian.PutUint64(out[:8], i)
	binary.LittleEndian.PutUint64(out[8:16], i*0x123456789abcdef)
	binary.LittleEndian.PutUint32(out[16:], uint32(i^0xfedcba98))
	return out
}

func checked(t *testing.T, records []Record) (Payload, Statistics, []byte) {
	t.Helper()
	p, stats, err := Build(context.Background(), records)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := wire.Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(encoded)
	r, err := wire.DecodeKnown(encoded, hex.EncodeToString(h[:]))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range records {
		got, err := r.LookupKnown(want.OID)
		if err != nil || got != want.Size {
			t.Fatalf("size for %x = %d, %v; want %d", want.OID, got, err, want.Size)
		}
	}
	if stats.Records != uint64(len(records)) || stats.Assigned != stats.Records || stats.InputRecordsChecked != stats.Records {
		t.Fatalf("population counters: %+v", stats)
	}
	if stats.InputBytes != uint64(cap(records))*uint64(unsafe.Sizeof(Record{})) {
		t.Fatalf("input backing not charged: %+v", stats)
	}
	if stats.EncodedBytes != uint64(len(encoded)) || stats.PeakScratchBytes > ScratchLimit || stats.PeakScratchBytes < stats.InputBytes+stats.RetainedBytes || stats.PeakBuilderBytes != stats.PeakScratchBytes-stats.InputBytes {
		t.Fatalf("memory/wire counters: %+v", stats)
	}
	if r.RetainedBytes() > EncodedLimit {
		t.Fatalf("reader retained %d", r.RetainedBytes())
	}
	remaining, visits := stats.Records, uint64(0)
	for i, level := range p.Levels {
		wantBits := uint32((remaining*Gamma + 63) / 64 * 64)
		if level.BitCount != wantBits || level.Seed != levelSeed(i) {
			t.Fatalf("fixed level rule changed at %d", i)
		}
		visits += 2 * remaining
		for j := 0; j < len(level.BitmapLE64); j += 8 {
			remaining -= uint64(bits.OnesCount64(binary.LittleEndian.Uint64(level.BitmapLE64[j:])))
		}
	}
	if remaining != 0 || visits != stats.HashVisits || stats.Passes != 1+2*stats.Levels {
		t.Fatalf("work counters: %+v, remaining %d, visits %d", stats, remaining, visits)
	}
	return p, stats, encoded
}

func TestKnownSizesRanksExceptionsAndDeterminism(t *testing.T) {
	records := make([]Record, 4096, 4112)
	for i := range records {
		records[i] = Record{OID: oid(uint64(i + 1)), Size: int64(i * 37)}
	}
	sizes := []int64{0, 1, math.MaxUint32 - 1, math.MaxUint32, math.MaxUint32 + 1, math.MaxInt64}
	for i, size := range sizes {
		records[i*31].Size = size
	}
	before := append([]Record(nil), records...)
	p, stats, encoded := checked(t, records)
	if stats.Exceptions != 3 || len(p.LargeSizes) != 36 || len(p.Levels[0].RanksLE32) < 8 {
		t.Fatalf("exceptions/rank blocks: %+v", stats)
	}
	if !reflect.DeepEqual(records, before) {
		t.Fatal("input changed")
	}
	for i, j := 0, len(records)-1; i < j; i, j = i+1, j-1 {
		records[i], records[j] = records[j], records[i]
	}
	_, _, reverse := checked(t, records)
	if !bytes.Equal(encoded, reverse) {
		t.Fatal("input order changed canonical retrieval table")
	}
}

func TestCollisionPromotionAndDuplicateBound(t *testing.T) {
	var seen [64][20]byte
	var occupied [64]bool
	var pair []Record
	for i := uint64(1); i <= 65; i++ {
		key := oid(i)
		p := wire.HashOID(key, levelSeed(0)) % 64
		if occupied[p] {
			pair = []Record{{seen[p], 123}, {key, 456}}
			break
		}
		seen[p], occupied[p] = key, true
	}
	if len(pair) != 2 {
		t.Fatal("pigeonhole collision not found")
	}
	p, stats, _ := checked(t, pair)
	if stats.Levels < 2 || binary.LittleEndian.Uint64(p.Levels[0].BitmapLE64) != 0 {
		t.Fatalf("colliding keys not promoted: %+v", stats)
	}
	p, stats, err := Build(context.Background(), []Record{{pair[0].OID, 12}, {pair[0].OID, 13}})
	if !errors.Is(err, ErrLimit) || !errors.Is(err, ErrUnresolved) {
		t.Fatalf("duplicate = %v", err)
	}
	if p.Count != 0 || stats.RetainedBytes != 0 || stats.Levels != LevelLimit || stats.HashVisits != 4*LevelLimit {
		t.Fatalf("duplicate was not bounded: %+v payload %+v", stats, p)
	}
}

func TestEmptyNegativeAndCancellation(t *testing.T) {
	p, _, encoded := checked(t, nil)
	if p.Count != 0 || len(encoded) != 0 || len(p.Levels) != 0 {
		t.Fatalf("noncanonical empty table %+v", p)
	}
	p, stats, err := Build(context.Background(), []Record{{OID: oid(1), Size: -1}})
	if !errors.Is(err, ErrInvalid) || p.Count != 0 || stats.Assigned != 0 {
		t.Fatalf("negative = %+v %+v %v", p, stats, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p, stats, err = Build(ctx, []Record{{OID: oid(1), Size: 1}})
	if !errors.Is(err, context.Canceled) || p.Count != 0 || stats.HashVisits != 0 {
		t.Fatalf("cancel = %+v %+v %v", p, stats, err)
	}
}

func TestDimensionAndMemoryBoundsBeforeAllocation(t *testing.T) {
	for _, test := range [][3]uint64{{MaxRecords + 1, MaxRecords + 1, 0}, {2, 1, 0}, {1, math.MaxUint64, 0}, {1, 1, 2}, {MaxRecords, MaxRecords, 1}} {
		if !errors.Is(dimensions(test[0], test[1], test[2]), ErrLimit) {
			t.Fatalf("accepted dimensions %v", test)
		}
	}
	if err := dimensions(3196537, 3196537, 0); err != nil {
		t.Fatal(err)
	}
	m := memory{}
	if err := m.add(ScratchLimit); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(m.add(1), ErrLimit) || m.live != ScratchLimit || m.peak != ScratchLimit {
		t.Fatalf("memory bound: %+v", m)
	}
	m.sub(17)
	if err := m.add(17); err != nil {
		t.Fatal(err)
	}
}

func TestRankAtWordAndBlockEdges(t *testing.T) {
	bitmap := make([]byte, 192)
	positions := []uint64{0, 1, 63, 64, 127, 511, 512, 513, 1023, 1024, 1535}
	for _, p := range positions {
		mark(bitmap, p)
	}
	ranks := make([]byte, 12)
	binary.LittleEndian.PutUint32(ranks[4:], 6)
	binary.LittleEndian.PutUint32(ranks[8:], 9)
	for i, p := range positions {
		if got := rank(bitmap, ranks, p); got != uint32(i) {
			t.Fatalf("rank(%d)=%d want%d", p, got, i)
		}
	}
}
