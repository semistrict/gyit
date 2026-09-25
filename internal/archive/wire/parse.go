package wire

import (
	"google.golang.org/protobuf/encoding/protowire"
	"math"
)

// Flat bounded parsing avoids allocation from attacker-controlled protobuf
// counts. Each OID is copied into a fixed array; there is no nested message DAG.
func parse(b []byte) (Recipe, error) {
	var r Recipe
	seen := uint8(0)
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 || num < 1 || num > 4 {
			return Recipe{}, ErrMalformed
		}
		b = b[n:]
		if num != 4 {
			if seen&(1<<num) != 0 {
				return Recipe{}, ErrMalformed
			}
			seen |= 1 << num
		}
		if num == 2 {
			if typ != protowire.VarintType {
				return Recipe{}, ErrMalformed
			}
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return Recipe{}, ErrMalformed
			}
			r.PackSize = v
			b = b[n:]
			continue
		}
		if typ != protowire.BytesType {
			return Recipe{}, ErrMalformed
		}
		v, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return Recipe{}, ErrMalformed
		}
		b = b[n:]
		switch num {
		case 1:
			if len(v) != 16 {
				return Recipe{}, ErrMalformed
			}
			copy(r.ArchiveID[:], v)
		case 3:
			if len(v) != 20 {
				return Recipe{}, ErrMalformed
			}
			copy(r.TargetOID[:], v)
		case 4:
			if len(r.Frames) == FrameLimit {
				return Recipe{}, ErrLimit
			}
			f, err := parseFrame(v)
			if err != nil {
				return Recipe{}, err
			}
			r.Frames = append(r.Frames, f)
		}
	}
	if seen != 14 {
		return Recipe{}, ErrMalformed
	}
	return r, nil
}
func parseFrame(b []byte) (Frame, error) {
	var f Frame
	seen := uint8(0)
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 || num < 1 || num > 6 || seen&(1<<num) != 0 {
			return Frame{}, ErrMalformed
		}
		seen |= 1 << num
		b = b[n:]
		if num == 6 {
			if typ != protowire.BytesType {
				return Frame{}, ErrMalformed
			}
			v, n := protowire.ConsumeBytes(b)
			if n < 0 || len(v) != 20 {
				return Frame{}, ErrMalformed
			}
			copy(f.OID[:], v)
			b = b[n:]
			continue
		}
		if typ != protowire.VarintType {
			return Frame{}, ErrMalformed
		}
		v, n := protowire.ConsumeVarint(b)
		if n < 0 || (num >= 3 && v > math.MaxUint32) {
			return Frame{}, ErrMalformed
		}
		b = b[n:]
		switch num {
		case 1:
			f.HeaderOffset = v
		case 2:
			f.Offset = v
		case 3:
			f.Length = uint32(v)
		case 4:
			f.RawSize = uint32(v)
		case 5:
			f.Size = uint32(v)
		}
	}
	if seen&(1<<6) == 0 {
		return Frame{}, ErrMalformed
	}
	return f, nil
}
