package repo

// The writer already owns Git's acquisition packs. Index construction may read
// those immutable files directly; mounted readers never need a local clone.
import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
	pb "gyit/internal/gen/gyit/storage/v1"
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

func (s *historySource) close() {
	for _, p := range s.packs {
		unix.Munmap(p.index)
		unix.Munmap(p.data)
	}
}
func mapHistoryFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < 1 || st.Size() > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("pack file size")
	}
	return unix.Mmap(int(f.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_PRIVATE)
}
func openHistorySource(dirs []string) (*historySource, error) {
	s := &historySource{cache: newCache(32 << 20)}
	for _, dir := range dirs {
		files, err := filepath.Glob(filepath.Join(dir, "objects", "pack", "pack-*.idx"))
		if err != nil {
			s.close()
			return nil, err
		}
		for _, file := range files {
			idx, err := mapHistoryFile(file)
			if err != nil {
				s.close()
				return nil, err
			}
			data, err := mapHistoryFile(strings.TrimSuffix(file, ".idx") + ".pack")
			if err != nil {
				unix.Munmap(idx)
				s.close()
				return nil, err
			}
			id := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(file), "pack-"), ".idx")
			p := historyPack{id: id, index: idx, data: data}
			s.packs = append(s.packs, p)
			if len(idx) < 1072 || !bytes.Equal(idx[:8], []byte{255, 't', 'O', 'c', 0, 0, 0, 2}) || len(data) < 32 || string(data[:4]) != "PACK" || binary.BigEndian.Uint32(data[4:8]) != 2 || !validProgressiveOID(id) || hex.EncodeToString(data[len(data)-20:]) != id {
				s.close()
				return nil, fmt.Errorf("invalid acquisition pack")
			}
			n := int(binary.BigEndian.Uint32(idx[1028:1032]))
			if n > (len(idx)-1072)/28 || int(binary.BigEndian.Uint32(data[8:12])) != n {
				s.close()
				return nil, fmt.Errorf("pack index bounds")
			}
			s.packs[len(s.packs)-1].count = n
		}
	}
	return s, nil
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
