package repo

import (
	"fmt"
	"io"
)

func number(data []byte, pos *int) (uint64, error) {
	var n uint64
	for shift := uint(0); shift < 63; shift += 7 {
		if *pos >= len(data) {
			return 0, io.ErrUnexpectedEOF
		}
		b := data[*pos]
		*pos++
		n |= uint64(b&127) << shift
		if b&128 == 0 {
			return n, nil
		}
	}
	return 0, fmt.Errorf("varint overflow")
}
func apply(base, program []byte) ([]byte, error) {
	pos := 0
	n, e := number(program, &pos)
	if e != nil || n != uint64(len(base)) {
		return nil, fmt.Errorf("delta base size")
	}
	n, e = number(program, &pos)
	if e != nil || n > ChunkSize {
		return nil, fmt.Errorf("delta result limit")
	}
	out := make([]byte, 0, int(n))
	for pos < len(program) {
		op := program[pos]
		pos++
		if op == 0 {
			return nil, fmt.Errorf("zero opcode")
		}
		if op&128 == 0 {
			size := int(op)
			if pos+size > len(program) || size > cap(out)-len(out) {
				return nil, fmt.Errorf("literal bounds")
			}
			out = append(out, program[pos:pos+size]...)
			pos += size
			continue
		}
		var off, size uint64
		for j := uint(0); j < 7; j++ {
			if op&(1<<j) == 0 {
				continue
			}
			if pos >= len(program) {
				return nil, io.ErrUnexpectedEOF
			}
			if j < 4 {
				off |= uint64(program[pos]) << (8 * j)
			} else {
				size |= uint64(program[pos]) << (8 * (j - 4))
			}
			pos++
		}
		if size == 0 {
			size = 65536
		}
		if off+size > uint64(len(base)) || size > uint64(cap(out)-len(out)) {
			return nil, fmt.Errorf("copy bounds")
		}
		out = append(out, base[off:off+size]...)
	}
	if len(out) != cap(out) {
		return nil, fmt.Errorf("result size")
	}
	return out, nil
}
