// Package table builds a private fixed-parameter static size retrieval table.
// It is a known-key retrieval structure, not an exact membership dictionary.
package table

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"unsafe"

	"gyit/internal/globalsizes/wire"
)

const (
	Gamma        = 2
	LevelLimit   = 64
	EncodedLimit = 16 << 20
	ScratchLimit = 256 << 20
	// A uint32 value alone takes four bytes; actual wire admission is stricter.
	MaxRecords        = EncodedLimit / 4
	metadataAllowance = 64*512 + 1024
)

var (
	ErrLimit      = errors.New("static size table limit")
	ErrInvalid    = errors.New("invalid size record")
	ErrUnresolved = errors.New("keys remain after fixed singleton levels; duplicate keys or excessive collisions")
)

type Record struct {
	OID  [20]byte
	Size int64
}
type Payload = wire.Payload
type Statistics struct {
	Records, Levels, Passes, HashVisits, Assigned, Exceptions uint64
	InputRecordsChecked, BitmapWords, RankRecords             uint64
	InputBytes, PeakScratchBytes, PeakBuilderBytes            uint64
	BitmapBytes, RankBytes, SizeBytes, ExceptionBytes         uint64
	RetainedBytes, EncodedBytes, MetadataAllowanceBytes       uint64
}

type memory struct{ live, peak, input uint64 }

func (m *memory) add(n uint64) error {
	if m.live > ScratchLimit || n > ScratchLimit-m.live {
		return fmt.Errorf("%w: logical construction memory", ErrLimit)
	}
	m.live += n
	m.peak = max(m.peak, m.live)
	return nil
}
func (m *memory) sub(n uint64) { m.live -= n }

func dimensions(count, capacity, exceptions uint64) error {
	if count > MaxRecords || capacity < count {
		return fmt.Errorf("%w: record count", ErrLimit)
	}
	if capacity > ScratchLimit/uint64(unsafe.Sizeof(Record{})) {
		return fmt.Errorf("%w: input backing capacity", ErrLimit)
	}
	if exceptions > count || count*4+exceptions*12 > EncodedLimit {
		return fmt.Errorf("%w: minimum value bytes", ErrLimit)
	}
	return nil
}
func levelSeed(level int) uint64 { return 0xd1b54a32d192ed03 ^ (uint64(level+1) * 0x9e3779b97f4a7c15) }
func mark(bitmap []byte, pos uint64) bool {
	i := pos / 64 * 8
	v := binary.LittleEndian.Uint64(bitmap[i : i+8])
	mask := uint64(1) << (pos % 64)
	binary.LittleEndian.PutUint64(bitmap[i:i+8], v|mask)
	return v&mask != 0
}
func present(bitmap []byte, pos uint64) bool {
	i := pos / 64 * 8
	return binary.LittleEndian.Uint64(bitmap[i:i+8])&(uint64(1)<<(pos%64)) != 0
}
func rank(bitmap, ranks []byte, pos uint64) uint32 {
	block := pos / 512
	result := binary.LittleEndian.Uint32(ranks[block*4 : block*4+4])
	word := pos / 64
	for i := block * 8; i < word; i++ {
		result += uint32(bits.OnesCount64(binary.LittleEndian.Uint64(bitmap[i*8 : i*8+8])))
	}
	v := binary.LittleEndian.Uint64(bitmap[word*8 : word*8+8])
	result += uint32(bits.OnesCount64(v & ((uint64(1) << (pos % 64)) - 1)))
	return result
}

type exceptionOrder []byte

func (e exceptionOrder) Len() int { return len(e) / 12 }
func (e exceptionOrder) Less(i, j int) bool {
	return binary.LittleEndian.Uint32(e[i*12:]) < binary.LittleEndian.Uint32(e[j*12:])
}
func (e exceptionOrder) Swap(i, j int) {
	var b [12]byte
	copy(b[:], e[i*12:i*12+12])
	copy(e[i*12:i*12+12], e[j*12:j*12+12])
	copy(e[j*12:j*12+12], b[:])
}

// Build reads only supplied records. It never enumerates trees or reads source
// objects. Caller buffers are immutable and their full capacity is charged to
// the fixed construction-memory bound. No encoded output bytes are allocated;
// the caller encodes/stores the returned neutral payload inside its own timer.
// Errors return an empty payload and actual work/peak counters accumulated so far.
func Build(ctx context.Context, records []Record) (out Payload, stats Statistics, retErr error) {
	stats.Records = uint64(len(records))
	if err := ctx.Err(); err != nil {
		return out, stats, err
	}
	if err := dimensions(uint64(len(records)), uint64(cap(records)), 0); err != nil {
		return out, stats, err
	}
	stats.InputBytes = uint64(cap(records)) * uint64(unsafe.Sizeof(Record{}))
	stats.MetadataAllowanceBytes = metadataAllowance
	mem := memory{input: stats.InputBytes}
	defer func() {
		stats.PeakScratchBytes = mem.peak
		if mem.peak >= mem.input {
			stats.PeakBuilderBytes = mem.peak - mem.input
		}
		if retErr != nil {
			out = Payload{}
			stats.RetainedBytes = 0
		}
	}()
	if err := mem.add(stats.InputBytes + metadataAllowance); err != nil {
		return out, stats, err
	}
	stats.Passes++
	for i, r := range records {
		if i&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return out, stats, err
			}
		}
		stats.InputRecordsChecked++
		if r.Size < 0 {
			return out, stats, fmt.Errorf("%w: negative size at record %d", ErrInvalid, i)
		}
		if uint64(r.Size) >= math.MaxUint32 {
			stats.Exceptions++
		}
	}
	if err := dimensions(uint64(len(records)), uint64(cap(records)), stats.Exceptions); err != nil {
		return out, stats, err
	}
	out.Count = uint32(len(records))
	stats.SizeBytes = uint64(len(records)) * 4
	stats.ExceptionBytes = stats.Exceptions * 12
	levelStorage := uint64(LevelLimit) * uint64(unsafe.Sizeof(wire.Level{}))
	if err := mem.add(stats.SizeBytes + stats.ExceptionBytes + levelStorage + uint64(unsafe.Sizeof(out))); err != nil {
		return out, stats, err
	}
	out.SizesLE32 = make([]byte, int(stats.SizeBytes))
	out.LargeSizes = make([]byte, int(stats.ExceptionBytes))
	out.Levels = make([]wire.Level, 0, LevelLimit)
	indicesBytes := uint64(len(records)) * 4
	maxBits := ((uint64(len(records))*Gamma + 63) / 64) * 64
	collisionBytes := maxBits / 8
	if err := mem.add(indicesBytes + collisionBytes); err != nil {
		return out, stats, err
	}
	indices := make([]uint32, len(records))
	for i := range indices {
		indices[i] = uint32(i)
	}
	collisions := make([]byte, int(collisionBytes))
	remaining := indices
	var base, exceptionPos uint32
	for len(remaining) > 0 {
		if err := ctx.Err(); err != nil {
			return out, stats, err
		}
		if len(out.Levels) == LevelLimit {
			return out, stats, errors.Join(ErrLimit, ErrUnresolved)
		}
		bitCount := uint32(((uint64(len(remaining))*Gamma + 63) / 64) * 64)
		bitmapBytes := uint64(bitCount) / 8
		rankBytes := uint64((bitCount+511)/512) * 4
		if err := mem.add(bitmapBytes + rankBytes); err != nil {
			return out, stats, err
		}
		level := wire.Level{Seed: levelSeed(len(out.Levels)), BitCount: bitCount, BaseOrdinal: base, BitmapLE64: make([]byte, int(bitmapBytes)), RanksLE32: make([]byte, int(rankBytes))}
		clear(collisions[:bitmapBytes])
		stats.Passes++
		for i, index := range remaining {
			if i&4095 == 0 {
				if err := ctx.Err(); err != nil {
					return out, stats, err
				}
			}
			pos := wire.HashOID(records[index].OID, level.Seed) % uint64(bitCount)
			stats.HashVisits++
			if mark(level.BitmapLE64, pos) {
				mark(collisions, pos)
			}
		}
		var singletons uint32
		for word := uint64(0); word < bitmapBytes/8; word++ {
			if word&4095 == 0 {
				if err := ctx.Err(); err != nil {
					return out, stats, err
				}
			}
			if word%8 == 0 {
				binary.LittleEndian.PutUint32(level.RanksLE32[word/8*4:], singletons)
				stats.RankRecords++
			}
			v := binary.LittleEndian.Uint64(level.BitmapLE64[word*8:]) &^ binary.LittleEndian.Uint64(collisions[word*8:])
			binary.LittleEndian.PutUint64(level.BitmapLE64[word*8:], v)
			singletons += uint32(bits.OnesCount64(v))
			stats.BitmapWords++
		}
		if uint64(base)+uint64(singletons) > uint64(len(records)) {
			return out, stats, fmt.Errorf("%w: assigned count overflow", ErrInvalid)
		}
		stats.Passes++
		next := remaining[:0]
		var assigned uint32
		for i, index := range remaining {
			if i&4095 == 0 {
				if err := ctx.Err(); err != nil {
					return out, stats, err
				}
			}
			pos := wire.HashOID(records[index].OID, level.Seed) % uint64(bitCount)
			stats.HashVisits++
			if !present(level.BitmapLE64, pos) {
				next = append(next, index)
				continue
			}
			ordinal := base + rank(level.BitmapLE64, level.RanksLE32, pos)
			if ordinal >= out.Count {
				return out, stats, fmt.Errorf("%w: value ordinal", ErrInvalid)
			}
			size := uint64(records[index].Size)
			if size >= math.MaxUint32 {
				binary.LittleEndian.PutUint32(out.SizesLE32[uint64(ordinal)*4:], math.MaxUint32)
				if uint64(exceptionPos) >= stats.Exceptions {
					return out, stats, fmt.Errorf("%w: exception count", ErrInvalid)
				}
				at := uint64(exceptionPos) * 12
				binary.LittleEndian.PutUint32(out.LargeSizes[at:], ordinal)
				binary.LittleEndian.PutUint64(out.LargeSizes[at+4:], size)
				exceptionPos++
			} else {
				binary.LittleEndian.PutUint32(out.SizesLE32[uint64(ordinal)*4:], uint32(size))
			}
			assigned++
			stats.Assigned++
		}
		if assigned != singletons {
			return out, stats, fmt.Errorf("%w: singleton assignment mismatch", ErrInvalid)
		}
		base += singletons
		remaining = next
		out.Levels = append(out.Levels, level)
		stats.Levels++
		stats.BitmapBytes += bitmapBytes
		stats.RankBytes += rankBytes
		// A lower bound permits early rejection; final canonical wire admission
		// includes every protobuf header and is performed below.
		if stats.SizeBytes+stats.ExceptionBytes+stats.BitmapBytes+stats.RankBytes > EncodedLimit {
			return out, stats, fmt.Errorf("%w: table bytes", ErrLimit)
		}
	}
	if uint64(base) != uint64(len(records)) || uint64(exceptionPos) != stats.Exceptions {
		return out, stats, fmt.Errorf("%w: final assignment population", ErrInvalid)
	}
	sort.Sort(exceptionOrder(out.LargeSizes))
	if err := ctx.Err(); err != nil {
		return out, stats, err
	}
	n, err := wire.EncodedSize(out)
	if err != nil {
		if errors.Is(err, wire.ErrLimit) {
			return out, stats, errors.Join(ErrLimit, err)
		}
		return out, stats, err
	}
	if n > EncodedLimit {
		return out, stats, fmt.Errorf("%w: canonical encoded bytes", ErrLimit)
	}
	stats.EncodedBytes = uint64(n)
	stats.RetainedBytes = uint64(unsafe.Sizeof(out)) + levelStorage + stats.SizeBytes + stats.ExceptionBytes + stats.BitmapBytes + stats.RankBytes
	mem.sub(indicesBytes + collisionBytes)
	return out, stats, nil
}
