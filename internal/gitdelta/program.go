// Package gitdelta composes Git's native delta instructions into a bounded
// recipe against one original full base. It is an import-time operation: readers
// receive the existing protobuf ChunkDelta and never traverse native chains.
package gitdelta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"google.golang.org/protobuf/encoding/protowire"
)

const MaxSize = 1 << 20
const maxOperations = MaxSize/8 + 1

// ErrLimit asks the importer to materialize and rechunk/re-encode the object.
// Malformed instructions return other errors and must not be silently accepted.
var ErrLimit = errors.New("native delta exceeds bounded chunk representation")

type span struct {
	offset, end uint32
	literal     []byte
}

// Program is immutable. Copies address the original full base, even after many
// compositions; literals are owned by the program, independent of source buffers.
type Program struct {
	rootSize, size uint32
	spans          []span
}

func From(size uint32) (*Program, error) {
	if size > MaxSize {
		return nil, ErrLimit
	}
	p := &Program{rootSize: size, size: size}
	if size > 0 {
		p.spans = []span{{end: size}}
	}
	return p, nil
}

func (p *Program) Size() uint32 { return p.size }

// Compose consumes one complete, inflated Git delta. The returned recipe has
// the same full base as p, regardless of native chain depth.
func (p *Program) Compose(data []byte) (*Program, error) {
	if len(data) > 2*MaxSize {
		return nil, ErrLimit
	}
	takeSize := func() (uint64, error) {
		value, n := binary.Uvarint(data)
		if n <= 0 {
			return 0, fmt.Errorf("invalid native delta size")
		}
		data = data[n:]
		return value, nil
	}
	base, err := takeSize()
	if err != nil {
		return nil, err
	}
	if base != uint64(p.size) {
		return nil, fmt.Errorf("native delta base size mismatch")
	}
	target, err := takeSize()
	if err != nil {
		return nil, err
	}
	if target > MaxSize {
		return nil, ErrLimit
	}
	out := &Program{rootSize: p.rootSize, size: uint32(target), spans: make([]span, 0, max(16, len(p.spans)))}
	var written uint32
	add := func(offset, n uint32, literal []byte) error {
		if n == 0 || n > out.size-written {
			return fmt.Errorf("native delta output exceeds target size")
		}
		written += n
		if len(out.spans) > 0 {
			last := &out.spans[len(out.spans)-1]
			previous := uint32(0)
			if len(out.spans) > 1 {
				previous = out.spans[len(out.spans)-2].end
			}
			if literal == nil && last.literal == nil && last.offset+last.end-previous == offset {
				last.end = written
				return nil
			}
			if literal != nil && last.literal != nil {
				last.literal = append(last.literal, literal...)
				last.end = written
				return nil
			}
		}
		if len(out.spans) == maxOperations {
			return ErrLimit
		}
		out.spans = append(out.spans, span{offset: offset, end: written, literal: bytes.Clone(literal)})
		return nil
	}
	for len(data) > 0 {
		op := data[0]
		data = data[1:]
		if op&0x80 == 0 {
			if op == 0 {
				return nil, fmt.Errorf("reserved native delta opcode")
			}
			if len(data) < int(op) {
				return nil, io.ErrUnexpectedEOF
			}
			if err := add(0, uint32(op), data[:op]); err != nil {
				return nil, err
			}
			data = data[op:]
			continue
		}
		var offset, n uint32
		for bit := 0; bit < 7; bit++ {
			if op&(1<<bit) == 0 {
				continue
			}
			if len(data) == 0 {
				return nil, io.ErrUnexpectedEOF
			}
			if bit < 4 {
				offset |= uint32(data[0]) << (8 * bit)
			} else {
				n |= uint32(data[0]) << (8 * (bit - 4))
			}
			data = data[1:]
		}
		if n == 0 {
			n = 65536
		}
		if uint64(offset)+uint64(n) > uint64(p.size) {
			return nil, fmt.Errorf("native delta copy exceeds base")
		}
		if n > out.size-written {
			return nil, fmt.Errorf("native delta copy exceeds target")
		}
		i := sort.Search(len(p.spans), func(i int) bool { return p.spans[i].end > offset })
		for n > 0 {
			previous := uint32(0)
			if i > 0 {
				previous = p.spans[i-1].end
			}
			part := p.spans[i]
			within := offset - previous
			length := min(n, part.end-offset)
			var literal []byte
			if part.literal != nil {
				literal = part.literal[within : within+length]
			}
			if err := add(part.offset+within, length, literal); err != nil {
				return nil, err
			}
			offset += length
			n -= length
			i++
		}
	}
	if written != out.size {
		return nil, fmt.Errorf("native delta output is truncated")
	}
	return out, nil
}

// Materialize is useful for object-ID verification during import. It never
// expands more than a single chunk, and takes the original full base.
func (p *Program) Materialize(base []byte) ([]byte, error) {
	return p.MaterializeInto(nil, base)
}

// MaterializeInto reuses a caller-owned output buffer across import jobs.
func (p *Program) MaterializeInto(dst, base []byte) ([]byte, error) {
	if uint64(len(base)) != uint64(p.rootSize) {
		return nil, fmt.Errorf("wrong full base length")
	}
	if cap(dst) < int(p.size) {
		dst = make([]byte, p.size)
	}
	out := dst[:p.size]
	var previous uint32
	for _, part := range p.spans {
		n := part.end - previous
		if part.literal != nil {
			copy(out[previous:part.end], part.literal)
		} else {
			copy(out[previous:part.end], base[part.offset:part.offset+n])
		}
		previous = part.end
	}
	return out, nil
}

// Encode emits exactly the wire representation already accepted by readers.
func (p *Program) Encode() ([]byte, error) {
	return p.AppendTo(nil)
}

// AppendTo uses the protobuf wire encoder directly to avoid allocating one
// generated message per span; generated-message tests verify interoperability.
func (p *Program) AppendTo(dst []byte) ([]byte, error) {
	if p.size == 0 {
		return nil, ErrLimit
	} // Empty blobs do not need a chunk.
	start := len(dst)
	dst = protowire.AppendTag(dst, 1, protowire.VarintType)
	dst = protowire.AppendVarint(dst, uint64(p.size))
	var previous uint32
	for _, part := range p.spans {
		dst = protowire.AppendTag(dst, 2, protowire.BytesType)
		if part.literal != nil {
			n := 1 + protowire.SizeBytes(len(part.literal))
			dst = protowire.AppendVarint(dst, uint64(n))
			dst = protowire.AppendTag(dst, 3, protowire.BytesType)
			dst = protowire.AppendBytes(dst, part.literal)
		} else {
			length := part.end - previous
			n := 1 + protowire.SizeVarint(uint64(length))
			if part.offset != 0 {
				n += 1 + protowire.SizeVarint(uint64(part.offset))
			}
			dst = protowire.AppendVarint(dst, uint64(n))
			if part.offset != 0 {
				dst = protowire.AppendTag(dst, 1, protowire.VarintType)
				dst = protowire.AppendVarint(dst, uint64(part.offset))
			}
			dst = protowire.AppendTag(dst, 2, protowire.VarintType)
			dst = protowire.AppendVarint(dst, uint64(length))
		}
		previous = part.end
	}
	if len(dst)-start > 2*MaxSize {
		return nil, ErrLimit
	}
	return dst, nil
}

// AppendNative is an ignored diagnostic: emit a single Git delta against root.
func (p *Program) AppendNative(dst []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(p.rootSize))
	dst = binary.AppendUvarint(dst, uint64(p.size))
	var previous uint32
	for _, part := range p.spans {
		n := part.end - previous
		previous = part.end
		if part.literal != nil {
			lit := part.literal
			for len(lit) > 0 {
				size := min(127, len(lit))
				dst = append(dst, byte(size))
				dst = append(dst, lit[:size]...)
				lit = lit[size:]
			}
			continue
		}
		var args [7]byte
		count := 0
		op := byte(128)
		for bit := range 7 {
			var v byte
			if bit < 4 {
				v = byte(part.offset >> (8 * bit))
			} else if n != 65536 {
				v = byte(n >> (8 * (bit - 4)))
			}
			if v != 0 {
				op |= 1 << bit
				args[count] = v
				count++
			}
		}
		dst = append(dst, op)
		dst = append(dst, args[:count]...)
	}
	return dst
}
func (p *Program) MemoryBytes() int {
	n := 32 + cap(p.spans)*32
	for _, s := range p.spans {
		n += cap(s.literal)
	}
	return n
}
