package wire

import (
	"context"
	"fmt"
	"io"
)

func deltaNumber(data []byte, pos *int) (uint64, error) {
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
	return 0, fmt.Errorf("native delta varint overflow")
}

func applyRaw(ctx context.Context, base, program []byte, resultSize uint32) ([]byte, error) {
	pos := 0
	n, err := deltaNumber(program, &pos)
	if err != nil || n != uint64(len(base)) {
		return nil, fmt.Errorf("native delta base size")
	}
	n, err = deltaNumber(program, &pos)
	if err != nil || n != uint64(resultSize) || n > ObjectLimit {
		return nil, fmt.Errorf("native delta target size")
	}
	out := make([]byte, int(n))
	written, steps := 0, 0
	for pos < len(program) {
		if steps&63 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		steps++
		op := program[pos]
		pos++
		if op == 0 {
			return nil, fmt.Errorf("native delta zero opcode")
		}
		if op&128 == 0 {
			count := int(op)
			if count > len(program)-pos || count > len(out)-written {
				return nil, fmt.Errorf("native delta literal bounds")
			}
			copy(out[written:], program[pos:pos+count])
			written += count
			pos += count
			continue
		}
		var off, count uint64
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
				count |= uint64(program[pos]) << (8 * (j - 4))
			}
			pos++
		}
		if count == 0 {
			count = 65536
		}
		if off+count > uint64(len(base)) || count > uint64(len(out)-written) {
			return nil, fmt.Errorf("native delta copy bounds")
		}
		copy(out[written:], base[off:off+count])
		written += int(count)
	}
	if written != len(out) {
		return nil, fmt.Errorf("native delta result size")
	}
	return out, nil
}
