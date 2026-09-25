// Package wire describes authenticated reads from a segmented immutable Git pack.
package wire

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"sort"

	archivev1 "gat/internal/gen/gat/sourcearchive/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	SegmentSize      = 64 << 20
	WireLimit        = 8 << 10
	FrameLimit       = 64
	RangeLimit       = 16
	FetchConcurrency = 4
	PackedLimit      = 3 << 20
	ObjectLimit      = 1 << 20
	ProgramLimit     = 2 << 20
	WorkLimit        = 4 << 20
)

var (
	ErrLimit     = errors.New("archive recipe limit")
	ErrMalformed = errors.New("malformed archive recipe")
)

type Frame struct {
	HeaderOffset, Offset  uint64
	Length, RawSize, Size uint32
	OID                   [20]byte
}
type Recipe struct {
	ArchiveID [16]byte
	PackSize  uint64
	TargetOID [20]byte
	Frames    []Frame
}
type Range struct {
	Segment        uint64
	Offset, Length uint32
}

// Validate checks metadata, total work and the complete cold request plan. It
// does not inflate or authenticate source content during import admission.
func Validate(r Recipe) error {
	if r.PackSize < 32 || r.PackSize > math.MaxInt64 || len(r.Frames) == 0 || r.TargetOID != r.Frames[len(r.Frames)-1].OID {
		return ErrMalformed
	}
	if len(r.Frames) > FrameLimit {
		return ErrLimit
	}
	seen := make(map[[20]byte]bool, len(r.Frames))
	var work uint64
	for i, f := range r.Frames {
		if f.HeaderOffset < 12 || f.HeaderOffset >= f.Offset || f.Offset-f.HeaderOffset > 32 || f.Length == 0 || f.Offset > r.PackSize-20 || uint64(f.Length) > r.PackSize-20-f.Offset || seen[f.OID] {
			return ErrMalformed
		}
		seen[f.OID] = true
		if f.Size > ObjectLimit || f.Length > PackedLimit {
			return ErrLimit
		}
		if i == 0 {
			if f.RawSize != f.Size {
				return ErrMalformed
			}
			work += uint64(f.Size)
		} else {
			if f.RawSize < 2 {
				return ErrMalformed
			}
			if f.RawSize > ProgramLimit {
				return ErrLimit
			}
			work += uint64(f.RawSize) + uint64(f.Size)
		}
		if work > WorkLimit {
			return ErrLimit
		}
	}
	_, err := plan(r.Frames)
	return err
}

// Plan skips all frames through afterOrdinal. Removing a cached prefix can
// fragment physical ranges; in that case use the admitted full cold plan.
func Plan(r Recipe, afterOrdinal int) ([]Range, error) {
	if err := Validate(r); err != nil {
		return nil, err
	}
	if afterOrdinal < -1 || afterOrdinal >= len(r.Frames) {
		return nil, ErrMalformed
	}
	result, err := plan(r.Frames[afterOrdinal+1:])
	if errors.Is(err, ErrLimit) {
		return plan(r.Frames)
	}
	return result, err
}

func plan(frames []Frame) ([]Range, error) {
	order := append([]Frame(nil), frames...)
	sort.Slice(order, func(i, j int) bool { return order[i].HeaderOffset < order[j].HeaderOffset })
	type extent struct{ start, end uint64 }
	extents := make([]extent, 0, len(order))
	for _, f := range order {
		end := f.Offset + uint64(f.Length)
		if len(extents) > 0 {
			prior := &extents[len(extents)-1]
			if f.HeaderOffset < prior.end {
				return nil, ErrMalformed
			}
			if f.HeaderOffset == prior.end {
				prior.end = end
				continue
			}
		}
		extents = append(extents, extent{f.Offset, end})
	}
	result := make([]Range, 0, RangeLimit)
	var bytes uint64
	for _, e := range extents {
		bytes += e.end - e.start
		if bytes > PackedLimit {
			return nil, ErrLimit
		}
		for start := e.start; start < e.end; {
			segment, offset := start/SegmentSize, start%SegmentSize
			length := min(e.end-start, SegmentSize-offset)
			result = append(result, Range{segment, uint32(offset), uint32(length)})
			if len(result) > RangeLimit {
				return nil, ErrLimit
			}
			start += length
		}
	}
	return result, nil
}

func scalarSize(v uint64) int {
	if v == 0 {
		return 0
	}
	return 1 + protowire.SizeVarint(v)
}
func frameSize(f Frame) int {
	return scalarSize(f.HeaderOffset) + scalarSize(f.Offset) + scalarSize(uint64(f.Length)) + scalarSize(uint64(f.RawSize)) + scalarSize(uint64(f.Size)) + 22
}
func EncodedSize(r Recipe) (int, error) {
	if err := Validate(r); err != nil {
		return 0, err
	}
	n := 18 + scalarSize(r.PackSize) + 22
	for _, f := range r.Frames {
		s := frameSize(f)
		n += 1 + protowire.SizeBytes(s)
	}
	if n > WireLimit {
		return 0, ErrLimit
	}
	return n, nil
}
func Encode(r Recipe) ([]byte, error) {
	n, err := EncodedSize(r)
	if err != nil {
		return nil, err
	}
	p := &archivev1.Recipe{ArchiveId: r.ArchiveID[:], PackSize: r.PackSize, TargetOid: r.TargetOID[:], Frames: make([]*archivev1.Frame, len(r.Frames))}
	for i := range r.Frames {
		f := &r.Frames[i]
		p.Frames[i] = &archivev1.Frame{HeaderOffset: f.HeaderOffset, Offset: f.Offset, Length: f.Length, RawSize: f.RawSize, Size: f.Size, Oid: f.OID[:]}
	}
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(p)
	if err == nil && len(b) != n {
		return nil, ErrMalformed
	}
	return b, err
}

func DecodeRecipe(encoded []byte, expectedSHA256 string) (Recipe, error) {
	if len(encoded) > WireLimit {
		return Recipe{}, ErrLimit
	}
	want, err := hex.DecodeString(expectedSHA256)
	actual := sha256.Sum256(encoded)
	if err != nil || len(want) != 32 || hex.EncodeToString(actual[:]) != expectedSHA256 {
		return Recipe{}, ErrMalformed
	}
	r, err := parse(encoded)
	if err == nil {
		err = Validate(r)
	}
	if err != nil {
		return Recipe{}, err
	}
	return r, nil
}
