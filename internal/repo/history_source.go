package repo

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	pb "gyit/internal/gen/gyit/storage/v1"
	"io"
	"sort"
)

type historySourceKey struct{}
type historyPack struct {
	id          string
	index, data []byte
	count       int
}
type historySource struct {
	// Only completed, immutable Git acquisition packs may skip a second OID
	// hash. Store recipes and bytes (including delta bases) remain untrusted.
	indexedByGit bool
	packs        []historyPack
	cache        *cache
	// Decoder workspaces belong to this acquisition view and die with it.
	// Retain at most four, matching the bounded object decode concurrency.
	inflaters chan io.ReadCloser
}

func (s *historySource) inflater(r io.Reader) (io.ReadCloser, error) {
	select {
	case z := <-s.inflaters:
		if err := z.(zlib.Resetter).Reset(r, nil); err != nil {
			z.Close()
			return nil, err
		}
		return z, nil
	default:
		return zlib.NewReader(r)
	}
}

func (s *historySource) releaseInflater(z io.ReadCloser) {
	z.Close()
	select {
	case s.inflaters <- z:
	default:
	}
}

func (s *historySource) locate(oid string) (*pb.ProgressiveObject, []byte, error) {
	id, err := hex.DecodeString(oid)
	if err != nil || len(id) != 20 {
		return nil, nil, fmt.Errorf("invalid object identity")
	}
	for _, p := range s.packs {
		n := p.count
		i := sort.Search(n, func(i int) bool { return bytes.Compare(p.index[1032+20*i:1032+20*(i+1)], id) >= 0 })
		if i == n || !bytes.Equal(p.index[1032+20*i:1032+20*(i+1)], id) {
			continue
		}
		off := uint64(binary.BigEndian.Uint32(p.index[1032+24*n+4*i:]))
		if off&0x80000000 != 0 {
			pos := uint64(1032+28*n) + 8*(off&0x7fffffff)
			if pos+8 > uint64(len(p.index)-40) {
				return nil, nil, fmt.Errorf("pack offset bounds")
			}
			off = binary.BigEndian.Uint64(p.index[pos:])
		}
		if off < 12 || off >= uint64(len(p.data)-20) {
			return nil, nil, fmt.Errorf("pack offset bounds")
		}
		return &pb.ProgressiveObject{Pack: p.id, PackSize: int64(len(p.data)), Offset: int64(off)}, p.data[off : len(p.data)-20], nil
	}
	return nil, nil, nil
}

// Size queries need the delta result length. Decoders instead use locate and
// validate the lengths while decoding, avoiding a second zlib pass.
func (s *historySource) lookup(oid string) (*pb.ProgressiveObject, error) {
	o, raw, err := s.locate(oid)
	if err != nil || o == nil {
		return o, err
	}
	r := bytes.NewReader(raw)
	kind, size, _, _, err := progressiveHeader(r, o.Offset)
	if err != nil {
		return nil, err
	}
	if kind == 6 || kind == 7 {
		z, err := s.inflater(r)
		if err != nil {
			return nil, err
		}
		b := bufio.NewReaderSize(z, 32)
		_, err = binary.ReadUvarint(b)
		if err == nil {
			var n uint64
			n, err = binary.ReadUvarint(b)
			if n > progressiveObjectLimit {
				err = fmt.Errorf("delta result size")
			}
			size = int64(n)
		}
		s.releaseInflater(z)
		if err != nil {
			return nil, err
		}
	}
	o.Size = size
	return o, nil
}
