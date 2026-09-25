// Package wire implements a bounded retrieval table for known blob identities.
// A minimal perfect hash is not a membership test: unknown keys may return an
// unrelated value. Callers must establish membership separately when required.
package wire

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"unsafe"

	pb "gat/internal/gen/gat/globalsizes/v1"
	"google.golang.org/protobuf/proto"
)

const EncodedLimit = 16 << 20
const LevelLimit = 64

// RetainedLimit includes the borrowed wire buffer and the reader's metadata.
const RetainedLimit = EncodedLimit
const ReaderMetadataBytes = int(unsafe.Sizeof(Table{})) + LevelLimit*int(unsafe.Sizeof(Level{}))

var ErrLimit = errors.New("global size table limit")
var ErrMalformed = errors.New("malformed global size table")
var ErrUnknown = errors.New("no slot for supplied key; membership is not proved")

type Level struct {
	Seed                  uint64
	BitCount, BaseOrdinal uint32
	BitmapLE64, RanksLE32 []byte
}
type Payload struct {
	Count                 uint32
	Levels                []Level
	SizesLE32, LargeSizes []byte
}

// HashOID is fixed for this wire format. It includes every byte of the identity.
func HashOID(oid [20]byte, seed uint64) uint64 {
	mix := func(z uint64) uint64 {
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		return z ^ (z >> 31)
	}
	h := seed + 0x9e3779b97f4a7c15
	h = mix(h ^ binary.LittleEndian.Uint64(oid[:8]))
	h = mix(h ^ binary.LittleEndian.Uint64(oid[8:16]))
	return mix(h ^ uint64(binary.LittleEndian.Uint32(oid[16:])))
}

func validate(p Payload) error {
	if len(p.Levels) > LevelLimit || uint64(p.Count)*4 > EncodedLimit {
		return ErrLimit
	}
	remaining := RetainedLimit - ReaderMetadataBytes
	consume := func(n int) bool {
		if n > remaining {
			return false
		}
		remaining -= n
		return true
	}
	if !consume(len(p.SizesLE32)) || !consume(len(p.LargeSizes)) {
		return ErrLimit
	}
	for _, l := range p.Levels {
		if !consume(len(l.BitmapLE64)) || !consume(len(l.RanksLE32)) {
			return ErrLimit
		}
	}
	if uint64(len(p.SizesLE32)) != uint64(p.Count)*4 || len(p.LargeSizes)%12 != 0 {
		return ErrMalformed
	}
	if p.Count == 0 && len(p.Levels) != 0 || p.Count != 0 && len(p.Levels) == 0 {
		return ErrMalformed
	}
	total := uint64(0)
	for _, l := range p.Levels {
		if l.BitCount == 0 || uint64(l.BitCount) > EncodedLimit*8 {
			return ErrLimit
		}
		words := (uint64(l.BitCount) + 63) / 64
		blocks := (uint64(l.BitCount) + 511) / 512
		if uint64(len(l.BitmapLE64)) != words*8 || uint64(len(l.RanksLE32)) != blocks*4 || uint64(l.BaseOrdinal) != total {
			return ErrMalformed
		}
		local := uint64(0)
		for i := uint64(0); i < words; i++ {
			if i%8 == 0 && uint64(binary.LittleEndian.Uint32(l.RanksLE32[(i/8)*4:])) != local {
				return ErrMalformed
			}
			word := binary.LittleEndian.Uint64(l.BitmapLE64[i*8:])
			if i == words-1 && l.BitCount%64 != 0 && word>>(l.BitCount%64) != 0 {
				return ErrMalformed
			}
			local += uint64(bits.OnesCount64(word))
		}
		total += local
		if total > uint64(p.Count) {
			return ErrMalformed
		}
	}
	if total != uint64(p.Count) {
		return ErrMalformed
	}
	exception := 0
	for i := uint32(0); i < p.Count; i++ {
		if binary.LittleEndian.Uint32(p.SizesLE32[uint64(i)*4:]) != math.MaxUint32 {
			continue
		}
		if exception >= len(p.LargeSizes)/12 {
			return ErrMalformed
		}
		record := p.LargeSizes[exception*12:]
		size := binary.LittleEndian.Uint64(record[4:])
		if binary.LittleEndian.Uint32(record) != i || size < math.MaxUint32 || size > math.MaxInt64 {
			return ErrMalformed
		}
		exception++
	}
	if exception != len(p.LargeSizes)/12 {
		return ErrMalformed
	}
	return nil
}

func message(p Payload) (*pb.Payload, error) {
	if err := validate(p); err != nil {
		return nil, err
	}
	out := &pb.Payload{Count: p.Count, SizesLe32: p.SizesLE32, LargeSizes: p.LargeSizes, Levels: make([]*pb.Level, len(p.Levels))}
	for i, l := range p.Levels {
		out.Levels[i] = &pb.Level{Seed: l.Seed, BitCount: l.BitCount, BaseOrdinal: l.BaseOrdinal, BitmapLe64: l.BitmapLE64, RanksLe32: l.RanksLE32}
	}
	return out, nil
}

// EncodedSize validates shape and measures deterministic protobuf encoding. It
// does not allocate an encoded table or copy any bitmap/value bytes.
func EncodedSize(p Payload) (int, error) {
	m, err := message(p)
	if err != nil {
		return 0, err
	}
	n := proto.Size(m)
	if n > RetainedLimit-ReaderMetadataBytes {
		return 0, ErrLimit
	}
	return n, nil
}
func Encode(p Payload) ([]byte, error) {
	m, err := message(p)
	if err != nil {
		return nil, err
	}
	if proto.Size(m) > RetainedLimit-ReaderMetadataBytes {
		return nil, ErrLimit
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(m)
}

type Table struct {
	payload Payload
	encoded []byte
}

// DecodeKnown authenticates and validates once, retaining views into encoded.
// The caller must keep that buffer immutable. No bitmap, rank, size or exception
// vector is copied. Unknown-key lookups can alias a known key's value.
func DecodeKnown(encoded []byte, expectedSHA256 string) (*Table, error) {
	if len(encoded) > RetainedLimit-ReaderMetadataBytes {
		return nil, ErrLimit
	}
	hash := sha256.Sum256(encoded)
	if len(expectedSHA256) != 64 || hex.EncodeToString(hash[:]) != expectedSHA256 {
		return nil, fmt.Errorf("global size table checksum mismatch")
	}
	p, err := parsePayload(encoded)
	if err != nil {
		return nil, err
	}
	if err := validate(p); err != nil {
		return nil, err
	}
	return &Table{payload: p, encoded: encoded}, nil
}

func (t *Table) Count() uint32 { return t.payload.Count }

// OwnedBytes counts the reader structure and its level backing array; allocator
// overhead is excluded. BorrowedBytes is charged once to the shared byte cache.
func (t *Table) OwnedBytes() uint64 {
	return uint64(unsafe.Sizeof(*t)) + uint64(cap(t.payload.Levels))*uint64(unsafe.Sizeof(Level{}))
}
func (t *Table) BorrowedBytes() uint64 { return uint64(len(t.encoded)) }
func (t *Table) RetainedBytes() uint64 { return t.OwnedBytes() + t.BorrowedBytes() }

// LookupKnown returns an exact size only when oid belongs to the builder's key
// set. ErrUnknown is possible for absent keys, but its absence proves nothing.
func (t *Table) LookupKnown(oid [20]byte) (int64, error) {
	for _, l := range t.payload.Levels {
		position := HashOID(oid, l.Seed) % uint64(l.BitCount)
		wordIndex, bit := position/64, position%64
		word := binary.LittleEndian.Uint64(l.BitmapLE64[wordIndex*8:])
		if word&(uint64(1)<<bit) == 0 {
			continue
		}
		rank := uint64(l.BaseOrdinal) + uint64(binary.LittleEndian.Uint32(l.RanksLE32[(position/512)*4:]))
		for i := (wordIndex / 8) * 8; i < wordIndex; i++ {
			rank += uint64(bits.OnesCount64(binary.LittleEndian.Uint64(l.BitmapLE64[i*8:])))
		}
		rank += uint64(bits.OnesCount64(word & ((uint64(1) << bit) - 1)))
		value := binary.LittleEndian.Uint32(t.payload.SizesLE32[rank*4:])
		if value != math.MaxUint32 {
			return int64(value), nil
		}
		i := sort.Search(len(t.payload.LargeSizes)/12, func(i int) bool { return uint64(binary.LittleEndian.Uint32(t.payload.LargeSizes[i*12:])) >= rank })
		// Validation proved a one-to-one escape record for every escaped ordinal.
		return int64(binary.LittleEndian.Uint64(t.payload.LargeSizes[i*12+4:])), nil
	}
	return 0, ErrUnknown
}
