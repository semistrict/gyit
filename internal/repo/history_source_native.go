//go:build !js

package repo

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"strings"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
)

func (s *historySource) close() {
	s.inflaters = nil
	for _, p := range s.packs {
		unix.Munmap(p.index)
		unix.Munmap(p.data)
	}
}

// A concurrent acquisition can finish a local pack before its durable import.
// Such packs must not let coverage claim completion ahead of object publication.
func (p *Progressive) publishedHistorySource(ctx context.Context, dirs []string) (*historySource, error) {
	s, err := openHistorySource(dirs)
	if err != nil {
		return nil, err
	}
	kept := make([]historyPack, 0, len(s.packs))
	selected := make([]bool, len(s.packs))
	for i, pack := range s.packs {
		var published pb.ProgressiveObject
		err := p.get(ctx, "pack/"+pack.id, &published)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			s.close()
			return nil, err
		}
		if err == nil {
			if published.Pack != pack.id || published.PackSize != int64(len(pack.data)) {
				s.close()
				return nil, fmt.Errorf("history acquisition pack identity mismatch")
			}
			kept = append(kept, pack)
			selected[i] = true
		}
	}
	// Do not unmap rejected entries until validation finishes: error cleanup
	// still owns every mapping in the original slice.
	for i, pack := range s.packs {
		if !selected[i] {
			unix.Munmap(pack.index)
			unix.Munmap(pack.data)
		}
	}
	s.packs = kept
	return s, nil
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
	// These directories contain completed packs indexed by the Git acquisition
	// process. It already computed the object identities in their indexes.
	s := &historySource{indexedByGit: true, cache: newCache(32 << 20), inflaters: make(chan io.ReadCloser, 4)}
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
