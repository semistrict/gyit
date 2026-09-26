package wire

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"testing"
	"unsafe"

	pb "gyit/internal/gen/gyit/globalsizes/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func testKey(i int) [20]byte {
	var out [20]byte
	binary.LittleEndian.PutUint64(out[:], uint64(i))
	binary.LittleEndian.PutUint64(out[8:], uint64(i)*19)
	binary.LittleEndian.PutUint32(out[16:], uint32(i)*79)
	return out
}
func checksum(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }

// This small independent constructor is a test oracle, not the production
// builder: it sorts unique slots and fills each resulting rank directly.
func fixture(n int) (Payload, map[[20]byte]int64) {
	p := Payload{Count: uint32(n), SizesLE32: make([]byte, n*4)}
	want := make(map[[20]byte]int64, n)
	remaining := make([][20]byte, n)
	sizes := []int64{0, 42, math.MaxUint32 - 1, math.MaxUint32, math.MaxInt64}
	for i := range remaining {
		remaining[i] = testKey(i)
		want[remaining[i]] = sizes[i%len(sizes)]
	}
	ordinal := uint32(0)
	for len(remaining) > 0 {
		level := Level{Seed: uint64(len(p.Levels)) + 57, BitCount: uint32((2*len(remaining) + 63) / 64 * 64), BaseOrdinal: ordinal}
		level.BitmapLE64 = make([]byte, int(level.BitCount)/8)
		level.RanksLE32 = make([]byte, (int(level.BitCount)+511)/512*4)
		slots := make(map[uint64][][20]byte)
		for _, key := range remaining {
			s := HashOID(key, level.Seed) % uint64(level.BitCount)
			slots[s] = append(slots[s], key)
		}
		var positions []uint64
		remaining = nil
		for slot, keys := range slots {
			if len(keys) == 1 {
				positions = append(positions, slot)
			} else {
				remaining = append(remaining, keys...)
			}
		}
		sort.Slice(positions, func(i, j int) bool { return positions[i] < positions[j] })
		for _, slot := range positions {
			w := level.BitmapLE64[slot/64*8:]
			binary.LittleEndian.PutUint64(w, binary.LittleEndian.Uint64(w)|1<<(slot%64))
			size := uint64(want[slots[slot][0]])
			if size >= math.MaxUint32 {
				record := make([]byte, 12)
				binary.LittleEndian.PutUint32(record, ordinal)
				binary.LittleEndian.PutUint64(record[4:], size)
				p.LargeSizes = append(p.LargeSizes, record...)
				size = math.MaxUint32
			}
			binary.LittleEndian.PutUint32(p.SizesLE32[ordinal*4:], uint32(size))
			ordinal++
		}
		count := uint32(0)
		for i := 0; i < len(level.BitmapLE64)/8; i++ {
			if i%8 == 0 {
				binary.LittleEndian.PutUint32(level.RanksLE32[(i/8)*4:], count)
			}
			count += uint32(bits.OnesCount64(binary.LittleEndian.Uint64(level.BitmapLE64[i*8:])))
		}
		p.Levels = append(p.Levels, level)
	}
	return p, want
}

func decodeFixture(t *testing.T, p Payload) (*Table, []byte) {
	t.Helper()
	b, err := Encode(p)
	if err != nil {
		t.Fatal(err)
	}
	table, err := DecodeKnown(b, checksum(b))
	if err != nil {
		t.Fatal(err)
	}
	return table, b
}
func rawMessage(t *testing.T, m *pb.Payload) []byte {
	t.Helper()
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func clonePayload(t *testing.T, p Payload) *pb.Payload {
	t.Helper()
	m, err := message(p)
	if err != nil {
		t.Fatal(err)
	}
	return proto.Clone(m).(*pb.Payload)
}

func TestKnownSizesAndZeroCopy(t *testing.T) {
	p, want := fixture(600)
	table, data := decodeFixture(t, p)
	if len(p.Levels) < 2 || p.Levels[0].BitCount <= 512 {
		t.Fatal("fixture must cover multiple levels and rank checkpoints")
	}
	if table.Count() != uint32(len(want)) {
		t.Fatal("count")
	}
	for key, size := range want {
		got, err := table.LookupKnown(key)
		if err != nil || got != size {
			t.Fatalf("key=%x size=%d want=%d err=%v", key, got, size, err)
		}
	}
	again, err := Encode(p)
	if err != nil || !bytes.Equal(data, again) {
		t.Fatal("encoding not deterministic", err)
	}
	n, err := EncodedSize(p)
	if err != nil || n != len(data) {
		t.Fatal("encoded size", n, err)
	}
	if table.OwnedBytes() != uint64(ReaderMetadataBytes) || table.OwnedBytes() > 8192 || table.BorrowedBytes() != uint64(len(data)) || table.RetainedBytes() != uint64(len(data)+ReaderMetadataBytes) {
		t.Fatal("retained accounting")
	}
	start := uintptr(unsafe.Pointer(unsafe.SliceData(data)))
	end := start + uintptr(len(data))
	views := [][]byte{table.payload.SizesLE32, table.payload.LargeSizes}
	for _, level := range table.payload.Levels {
		views = append(views, level.BitmapLE64, level.RanksLE32)
	}
	for _, view := range views {
		address := uintptr(unsafe.Pointer(unsafe.SliceData(view)))
		if address < start || address+uintptr(len(view)) > end {
			t.Fatal("decoded vector is not a view of the input")
		}
	}
}

func TestEmptyAndChecksum(t *testing.T) {
	table, b := decodeFixture(t, Payload{})
	if len(b) != 0 || table.Count() != 0 {
		t.Fatal("empty encoding")
	}
	if _, err := table.LookupKnown(testKey(0)); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	p, _ := fixture(2)
	_, b = decodeFixture(t, p)
	hash := checksum(b)
	b[len(b)-1] ^= 1
	if table, err := DecodeKnown(b, hash); err == nil || table != nil {
		t.Fatal("corruption accepted")
	}
	if _, err := DecodeKnown(nil, "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"); err == nil {
		t.Fatal("noncanonical checksum accepted")
	}
}

func TestMalformedShapeRejected(t *testing.T) {
	p, _ := fixture(600)
	tests := map[string]func(*pb.Payload){
		"wrong count":        func(m *pb.Payload) { m.Count++ },
		"value truncated":    func(m *pb.Payload) { m.SizesLe32 = m.SizesLe32[:len(m.SizesLe32)-1] },
		"base ordinal":       func(m *pb.Payload) { m.Levels[1].BaseOrdinal++ },
		"rank first":         func(m *pb.Payload) { m.Levels[0].RanksLe32[0] = 1 },
		"rank later":         func(m *pb.Payload) { m.Levels[0].RanksLe32[4] ^= 1 },
		"bitmap shape":       func(m *pb.Payload) { m.Levels[0].BitmapLe64 = append(m.Levels[0].BitmapLe64, 0) },
		"rank shape":         func(m *pb.Payload) { m.Levels[0].RanksLe32 = append(m.Levels[0].RanksLe32, 0) },
		"zero bits":          func(m *pb.Payload) { m.Levels[0].BitCount = 0 },
		"exception absent":   func(m *pb.Payload) { m.LargeSizes = m.LargeSizes[12:] },
		"exception extra":    func(m *pb.Payload) { m.LargeSizes = append(m.LargeSizes, m.LargeSizes[:12]...) },
		"exception unsorted": func(m *pb.Payload) { copy(m.LargeSizes[:12], m.LargeSizes[12:24]) },
		"exception small":    func(m *pb.Payload) { binary.LittleEndian.PutUint64(m.LargeSizes[4:], 1) },
		"exception negative": func(m *pb.Payload) { binary.LittleEndian.PutUint64(m.LargeSizes[4:], 1<<63) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			m := clonePayload(t, p)
			mutate(m)
			b := rawMessage(t, m)
			if table, err := DecodeKnown(b, checksum(b)); err == nil || table != nil {
				t.Fatal("malformed table accepted")
			}
		})
	}
	// A partial last word must have no set bits beyond BitCount.
	m := &pb.Payload{Count: 1, SizesLe32: make([]byte, 4), Levels: []*pb.Level{{BitCount: 1, BitmapLe64: []byte{2, 0, 0, 0, 0, 0, 0, 0}, RanksLe32: make([]byte, 4)}}}
	b := rawMessage(t, m)
	if _, err := DecodeKnown(b, checksum(b)); err == nil {
		t.Fatal("tail bit accepted")
	}
}

func TestWireBoundsBeforeAllocation(t *testing.T) {
	if _, err := DecodeKnown(make([]byte, RetainedLimit-ReaderMetadataBytes+1), ""); !errors.Is(err, ErrLimit) {
		t.Fatal("retained bound", err)
	}
	if _, err := EncodedSize(Payload{Count: math.MaxUint32}); !errors.Is(err, ErrLimit) {
		t.Fatal("count arithmetic", err)
	}
	var bomb []byte
	for i := 0; i < 65; i++ {
		bomb = protowire.AppendTag(bomb, 2, protowire.BytesType)
		bomb = protowire.AppendBytes(bomb, nil)
	}
	if _, err := DecodeKnown(bomb, checksum(bomb)); !errors.Is(err, ErrLimit) {
		t.Fatal("level bomb", err)
	}
	malformed := [][]byte{
		{0x08, 0, 0x08, 0},                   // duplicate singular
		{0x28, 0},                            // unknown field
		{0x12, 2, 0x08},                      // truncated bytes
		{0x0a, 0},                            // count wrong wire type
		{0x12, 4, 0x10, 0, 0x10, 0},          // duplicate level scalar
		{0x08, 0xff, 0xff, 0xff, 0xff, 0x1f}, // count exceeds uint32
	}
	for _, b := range malformed {
		if _, err := DecodeKnown(b, checksum(b)); err == nil {
			t.Fatalf("malformed accepted %x", b)
		}
	}
	// Raw vectors alone exceed the retained cap, before any word scan/marshal.
	p := Payload{Count: 1, SizesLE32: make([]byte, 4), LargeSizes: make([]byte, RetainedLimit)}
	if _, err := Encode(p); !errors.Is(err, ErrLimit) {
		t.Fatal("oversize payload", err)
	}
}

func TestHashUsesAllIdentityBytesAndSeed(t *testing.T) {
	var zero [20]byte
	baseline := HashOID(zero, 0)
	if HashOID(zero, 1) == baseline {
		t.Fatal("seed ignored")
	}
	for i := range zero {
		one := zero
		one[i] = 1
		if HashOID(one, 0) == baseline {
			t.Fatalf("identity byte%d ignored", i)
		}
	}
}
