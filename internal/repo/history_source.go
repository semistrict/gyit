package repo

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	pb "gyit/internal/gen/gyit/storage/v1"
	"sort"
)

type historySourceKey struct{}
type historyPack struct {
	id          string
	index, data []byte
	count       int
}
type historySource struct {
	packs []historyPack
	cache *cache
}

func (s *historySource) lookup(oid string) (*pb.ProgressiveObject, error) {
	id, err := hex.DecodeString(oid)
	if err != nil || len(id) != 20 {
		return nil, fmt.Errorf("invalid object identity")
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
				return nil, fmt.Errorf("pack offset bounds")
			}
			off = binary.BigEndian.Uint64(p.index[pos:])
		}
		if off < 12 || off >= uint64(len(p.data)-20) {
			return nil, fmt.Errorf("pack offset bounds")
		}
		r := bytes.NewReader(p.data[off : len(p.data)-20])
		kind, size, _, _, err := progressiveHeader(r, int64(off))
		if err != nil {
			return nil, err
		}
		if kind == 6 || kind == 7 {
			z, err := zlib.NewReader(r)
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
			z.Close()
			if err != nil {
				return nil, err
			}
		}
		return &pb.ProgressiveObject{Pack: p.id, PackSize: int64(len(p.data)), Offset: int64(off), Size: size}, nil
	}
	return nil, nil
}
