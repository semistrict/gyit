package wire

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"errors"
	archivev1 "gat/internal/gen/gat/sourcearchive/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func testOID(b []byte) [20]byte {
	return sha1.Sum(append([]byte(fmt.Sprintf("blob %d\x00", len(b))), b...))
}
func testZlib(t testing.TB, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	if _, err := w.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
func literalDelta(base, target []byte) []byte {
	b := binary.AppendUvarint(nil, uint64(len(base)))
	b = binary.AppendUvarint(b, uint64(len(target)))
	for len(target) > 0 {
		n := min(127, len(target))
		b = append(b, byte(n))
		b = append(b, target[:n]...)
		target = target[n:]
	}
	return b
}

type testSource struct {
	r      Recipe
	packed [][]byte
	raw    [][]byte
}

func testChain(t testing.TB, n int, adjacent bool) testSource {
	t.Helper()
	s := testSource{r: Recipe{ArchiveID: [16]byte{1}}}
	offset := uint64(12)
	for i := 0; i < n; i++ {
		raw := []byte(fmt.Sprintf("version %d: bounded fixture\n", i))
		program := raw
		if i > 0 {
			program = literalDelta(s.raw[i-1], raw)
		}
		compressed := testZlib(t, program)
		f := Frame{HeaderOffset: offset, Offset: offset + 2, Length: uint32(len(compressed)), RawSize: uint32(len(program)), Size: uint32(len(raw)), OID: testOID(raw)}
		s.r.Frames = append(s.r.Frames, f)
		s.packed = append(s.packed, compressed)
		s.raw = append(s.raw, raw)
		offset = f.Offset + uint64(f.Length)
		if !adjacent {
			offset += 100
		}
	}
	s.r.PackSize = offset + 20
	s.r.TargetOID = s.r.Frames[n-1].OID
	return s
}
func (s testSource) fetch(_ context.Context, segment uint64, offset, length uint32) ([]byte, error) {
	start := segment*SegmentSize + uint64(offset)
	end := start + uint64(length)
	if uint64(offset)+uint64(length) > SegmentSize || end > s.r.PackSize {
		return nil, fmt.Errorf("bad fetch")
	}
	b := make([]byte, length)
	for i, f := range s.r.Frames {
		a, z := f.Offset, f.Offset+uint64(f.Length)
		if start >= z || end <= a {
			continue
		}
		lo, hi := max(start, a), min(end, z)
		copy(b[lo-start:hi-start], s.packed[i][lo-a:hi-a])
	}
	return b, nil
}

func TestArchiveRoundTripAndVerifiedBase(t *testing.T) {
	s := testChain(t, 3, true)
	encoded, err := Encode(s.r)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := EncodedSize(s.r); err != nil || n != len(encoded) {
		t.Fatal("encoded size", n, err)
	}
	r, err := DecodeRecipe(encoded, fmt.Sprintf("%x", sha256.Sum256(encoded)))
	if err != nil || !reflect.DeepEqual(r, s.r) {
		t.Fatal("roundtrip", err)
	}
	var cached []int
	got, m, err := Read(t.Context(), r, r.TargetOID, s.fetch, nil, func(i int, b []byte) {
		if !bytes.Equal(b, s.raw[i]) {
			t.Error("incorrect verified bytes")
		}
		cached = append(cached, i)
	})
	if err != nil || !bytes.Equal(got, s.raw[2]) || !reflect.DeepEqual(cached, []int{0, 1, 2}) || m.Fetches != 1 || m.VerifiedFrames != 3 {
		t.Fatal("full read", m, cached, err)
	}
	for _, ordinal := range []int{1, 2} {
		cached = nil
		got, m, err = Read(t.Context(), r, r.TargetOID, s.fetch, &VerifiedBase{ordinal, s.raw[ordinal]}, func(i int, b []byte) { cached = append(cached, i) })
		want := []int{2}
		gets := 1
		if ordinal == 2 {
			want = nil
			gets = 0
		}
		if err != nil || !bytes.Equal(got, s.raw[2]) || !reflect.DeepEqual(cached, want) || m.Fetches != gets {
			t.Fatal("cached suffix", ordinal, m, cached, err)
		}
	}
}

func TestArchiveSegmentSplitAndCopyDelta(t *testing.T) {
	base, target := []byte("abcdef"), []byte("abcXYZ")
	program := []byte{6, 6, 0x90, 3, 3, 'X', 'Y', 'Z'}
	rootPacked, deltaPacked := testZlib(t, base), testZlib(t, program)
	f := Frame{HeaderOffset: SegmentSize - 7, Offset: SegmentSize - 5, Length: uint32(len(rootPacked)), RawSize: 6, Size: 6, OID: testOID(base)}
	g := Frame{HeaderOffset: f.Offset + uint64(f.Length), Offset: f.Offset + uint64(f.Length) + 2, Length: uint32(len(deltaPacked)), RawSize: uint32(len(program)), Size: 6, OID: testOID(target)}
	s := testSource{r: Recipe{ArchiveID: [16]byte{2}, PackSize: g.Offset + uint64(g.Length) + 20, TargetOID: g.OID, Frames: []Frame{f, g}}, packed: [][]byte{rootPacked, deltaPacked}, raw: [][]byte{base, target}}
	plan, err := Plan(s.r, -1)
	if err != nil || len(plan) != 2 || plan[0].Segment != 0 || plan[0].Length != 5 || plan[1].Segment != 1 || plan[1].Offset != 0 {
		t.Fatal("segment plan", plan, err)
	}
	got, m, err := Read(t.Context(), s.r, g.OID, s.fetch, nil, nil)
	if err != nil || !bytes.Equal(got, target) || m.Fetches != 2 || m.CopiedFrameBytes != uint64(len(rootPacked)) {
		t.Fatal("split read", m, err)
	}
}

func TestArchiveFragmentedCachedSuffixUsesFullPlan(t *testing.T) {
	// In physical order, old and new chain nodes alternate. Removing a cached
	// prefix creates seventeen ranges even though the whole closure is one.
	var positions []int
	for i := 0; i < 16; i++ {
		positions = append(positions, 2*i+1)
	}
	for i := 0; i < 17; i++ {
		positions = append(positions, 2*i)
	}
	r := Recipe{ArchiveID: [16]byte{3}, PackSize: 1000}
	for i, p := range positions {
		f := Frame{HeaderOffset: uint64(12 + p*12), Offset: uint64(14 + p*12), Length: 10, RawSize: 2, Size: 2, OID: [20]byte{byte(i + 1)}}
		r.Frames = append(r.Frames, f)
	}
	r.TargetOID = r.Frames[len(r.Frames)-1].OID
	cold, err := Plan(r, -1)
	if err != nil {
		t.Fatal(err)
	}
	suffix, err := Plan(r, 15)
	if err != nil || len(cold) != 1 || !reflect.DeepEqual(cold, suffix) {
		t.Fatal("fragmentation increased GET bound", cold, suffix, err)
	}
}

func TestArchiveCorruptionExposesNoData(t *testing.T) {
	for _, which := range []string{"root", "program", "intermediate-id", "target-id", "cached-base", "external-target", "short-range"} {
		t.Run(which, func(t *testing.T) {
			s := testChain(t, 3, false)
			expected := s.r.TargetOID
			var base *VerifiedBase
			fetch := Fetch(s.fetch)
			switch which {
			case "root":
				s.packed[0] = bytes.Clone(s.packed[0])
				s.packed[0][len(s.packed[0])/2] ^= 128
			case "program":
				s.packed[2] = bytes.Clone(s.packed[2])
				s.packed[2][len(s.packed[2])/2] ^= 128
			case "intermediate-id":
				s.r.Frames[1].OID[0] ^= 1
			case "target-id":
				s.r.Frames[2].OID[0] ^= 1
				s.r.TargetOID = s.r.Frames[2].OID
				expected = s.r.TargetOID
			case "cached-base":
				base = &VerifiedBase{1, bytes.Repeat([]byte{'x'}, len(s.raw[1]))}
			case "external-target":
				expected[0] ^= 1
			case "short-range":
				fetch = func(ctx context.Context, segment uint64, off, n uint32) ([]byte, error) {
					return make([]byte, n-1), nil
				}
			}
			callbacks := 0
			got, _, err := Read(t.Context(), s.r, expected, fetch, base, func(int, []byte) { callbacks++ })
			if err == nil || got != nil || callbacks != 0 {
				t.Fatalf("unverified data escaped: len=%d callbacks=%d err=%v", len(got), callbacks, err)
			}
		})
	}
}

func TestArchiveAdmissionAndWireLimits(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Recipe)
		want   error
	}{
		{"empty", func(r *Recipe) { r.Frames = nil }, ErrMalformed},
		{"count", func(r *Recipe) {
			f := r.Frames[0]
			r.Frames = make([]Frame, 65)
			for i := range r.Frames {
				r.Frames[i] = f
			}
			r.TargetOID = f.OID
		}, ErrLimit},
		{"object", func(r *Recipe) { r.Frames[0].RawSize = ObjectLimit + 1; r.Frames[0].Size = ObjectLimit + 1 }, ErrLimit},
		{"program", func(r *Recipe) { r.Frames[1].RawSize = ProgramLimit + 1 }, ErrLimit},
		{"work", func(r *Recipe) {
			r.Frames[0].RawSize = ObjectLimit
			r.Frames[0].Size = ObjectLimit
			r.Frames[1].RawSize = ProgramLimit
			r.Frames[1].Size = ObjectLimit
		}, ErrLimit},
		{"overflow", func(r *Recipe) { r.Frames[0].Offset = math.MaxUint64 }, ErrMalformed},
		{"overlap", func(r *Recipe) {
			r.Frames[1].HeaderOffset = r.Frames[0].Offset + 1
			r.Frames[1].Offset = r.Frames[1].HeaderOffset + 2
		}, ErrMalformed},
		{"duplicate", func(r *Recipe) { r.Frames[1].OID = r.Frames[0].OID }, ErrMalformed},
		{"packed", func(r *Recipe) { r.Frames[0].Length = PackedLimit + 1 }, ErrLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testChain(t, 3, true)
			s.r.PackSize = 16 << 20
			tc.mutate(&s.r)
			if err := Validate(s.r); !errors.Is(err, tc.want) {
				t.Fatalf("got %v want %v", err, tc.want)
			}
		})
	}
	s := testChain(t, 17, false)
	if err := Validate(s.r); !errors.Is(err, ErrLimit) {
		t.Fatal("seventeen GETs admitted", err)
	}
	s = testChain(t, 1, true)
	b, err := Encode(s.r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeRecipe(b, strings.Repeat("0", 64)); !errors.Is(err, ErrMalformed) {
		t.Fatal("bad recipe checksum", err)
	}
	if _, err = DecodeRecipe(make([]byte, WireLimit+1), ""); !errors.Is(err, ErrLimit) {
		t.Fatal("wire bound", err)
	}
	for _, extra := range [][]byte{protowire.AppendVarint(protowire.AppendTag(nil, 9, protowire.VarintType), 1), protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), make([]byte, 16))} {
		bad := append(bytes.Clone(b), extra...)
		if _, err = DecodeRecipe(bad, fmt.Sprintf("%x", sha256.Sum256(bad))); !errors.Is(err, ErrMalformed) {
			t.Fatal("unknown/duplicate field", err)
		}
	}
	f := s.r.Frames[0]
	msg := &archivev1.Recipe{ArchiveId: s.r.ArchiveID[:], PackSize: s.r.PackSize, TargetOid: s.r.TargetOID[:]}
	for range 65 {
		msg.Frames = append(msg.Frames, &archivev1.Frame{HeaderOffset: f.HeaderOffset, Offset: f.Offset, Length: f.Length, RawSize: f.RawSize, Size: f.Size, Oid: f.OID[:]})
	}
	bad, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = DecodeRecipe(bad, fmt.Sprintf("%x", sha256.Sum256(bad))); !errors.Is(err, ErrLimit) {
		t.Fatal("predecode count", err)
	}
}

func TestArchiveEmptyBlob(t *testing.T) {
	b := testZlib(t, nil)
	f := Frame{HeaderOffset: 12, Offset: 13, Length: uint32(len(b)), OID: testOID(nil)}
	r := Recipe{ArchiveID: [16]byte{4}, PackSize: uint64(33 + len(b)), TargetOID: f.OID, Frames: []Frame{f}}
	got, _, err := Read(t.Context(), r, f.OID, func(context.Context, uint64, uint32, uint32) ([]byte, error) { return b, nil }, nil, nil)
	if err != nil || len(got) != 0 {
		t.Fatal("empty blob", err)
	}
}

func TestArchiveObjectKindAuthentication(t *testing.T) {
	s := testChain(t, 1, true)
	h := sha1.New()
	fmt.Fprintf(h, "tree %d\x00", len(s.raw[0]))
	h.Write(s.raw[0])
	copy(s.r.Frames[0].OID[:], h.Sum(nil))
	s.r.TargetOID = s.r.Frames[0].OID
	got, _, err := ReadObject(t.Context(), "tree", s.r, s.r.TargetOID, s.fetch, nil, nil)
	if err != nil || !bytes.Equal(got, s.raw[0]) {
		t.Fatal("tree identity", err)
	}
	for _, kind := range []string{"blob", "commit", ""} {
		got, _, err := ReadObject(t.Context(), kind, s.r, s.r.TargetOID, s.fetch, nil, nil)
		if err == nil || got != nil {
			t.Fatal("object kind was not authenticated", kind, err)
		}
	}
}

func TestArchiveFetchCancellationJoins(t *testing.T) {
	s := testChain(t, 4, false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{}, 4)
	var exited atomic.Int32
	result := make(chan error, 1)
	go func() {
		got, _, err := Read(ctx, s.r, s.r.TargetOID, func(ctx context.Context, _ uint64, _, _ uint32) ([]byte, error) {
			started <- struct{}{}
			<-ctx.Done()
			exited.Add(1)
			return nil, ctx.Err()
		}, nil, nil)
		if got != nil {
			result <- fmt.Errorf("canceled bytes exposed")
			return
		}
		result <- err
	}()
	for range 4 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("payload reads were not parallel")
		}
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || exited.Load() != 4 {
			t.Fatal("fetch workers not joined", exited.Load(), err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read failed to cancel")
	}
}
