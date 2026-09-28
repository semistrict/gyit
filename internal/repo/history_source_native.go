//go:build !js

package repo

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"strings"
)

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
