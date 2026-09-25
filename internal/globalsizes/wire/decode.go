package wire

import (
	"google.golang.org/protobuf/encoding/protowire"
	"math"
)

// The fixed-depth schema is decoded directly so bytes fields keep their input
// backing. No generated Unmarshal (and no payload-sized allocation) is used.
func parsePayload(data []byte) (Payload, error) {
	p := Payload{Levels: make([]Level, 0, LevelLimit)}
	seen := uint32(0)
	for len(data) > 0 {
		field, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return Payload{}, ErrMalformed
		}
		data = data[n:]
		if field < 1 || field > 4 {
			return Payload{}, ErrMalformed
		}
		if field != 2 {
			mask := uint32(1) << field
			if seen&mask != 0 {
				return Payload{}, ErrMalformed
			}
			seen |= mask
		}
		if field == 1 {
			if typ != protowire.VarintType {
				return Payload{}, ErrMalformed
			}
			v, n := protowire.ConsumeVarint(data)
			if n < 0 || v > math.MaxUint32 {
				return Payload{}, ErrMalformed
			}
			p.Count = uint32(v)
			data = data[n:]
			continue
		}
		if typ != protowire.BytesType {
			return Payload{}, ErrMalformed
		}
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return Payload{}, ErrMalformed
		}
		data = data[n:]
		switch field {
		case 2:
			if len(p.Levels) == LevelLimit {
				return Payload{}, ErrLimit
			}
			l, err := parseLevel(b)
			if err != nil {
				return Payload{}, err
			}
			p.Levels = append(p.Levels, l)
		case 3:
			p.SizesLE32 = b
		case 4:
			p.LargeSizes = b
		}
	}
	return p, nil
}

func parseLevel(data []byte) (Level, error) {
	var l Level
	seen := uint32(0)
	for len(data) > 0 {
		field, typ, n := protowire.ConsumeTag(data)
		if n < 0 || field < 1 || field > 5 {
			return Level{}, ErrMalformed
		}
		data = data[n:]
		mask := uint32(1) << field
		if seen&mask != 0 {
			return Level{}, ErrMalformed
		}
		seen |= mask
		if field <= 3 {
			if typ != protowire.VarintType {
				return Level{}, ErrMalformed
			}
			v, n := protowire.ConsumeVarint(data)
			if n < 0 || field != 1 && v > math.MaxUint32 {
				return Level{}, ErrMalformed
			}
			data = data[n:]
			switch field {
			case 1:
				l.Seed = v
			case 2:
				l.BitCount = uint32(v)
			case 3:
				l.BaseOrdinal = uint32(v)
			}
		} else {
			if typ != protowire.BytesType {
				return Level{}, ErrMalformed
			}
			b, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return Level{}, ErrMalformed
			}
			data = data[n:]
			switch field {
			case 4:
				l.BitmapLE64 = b
			case 5:
				l.RanksLE32 = b
			}
		}
	}
	return l, nil
}
