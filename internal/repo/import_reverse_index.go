package repo

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"gyit/internal/packmeta"
	"golang.org/x/sys/unix"
)

// Older Git installations omit reverse indexes. Construct only that small index
// in owned staging space; never repack, copy, or modify the source repository.
func prepareArchivePack(ctx context.Context, prefix, tmp string) (string, error) {
	if st, err := os.Stat(prefix + ".rev"); err == nil {
		if !st.Mode().IsRegular() {
			return "", fmt.Errorf("source reverse index is not a regular file")
		}
		return prefix, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	f, err := os.Open(prefix + ".idx")
	if err != nil {
		return "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", err
	}
	if st.Size() < 1072 || st.Size() > 1072+36*int64(planner.MaxObjects) {
		return "", fmt.Errorf("%w: source index bounds", planner.ErrUnsupported)
	}
	idx, err := unix.Mmap(int(f.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_PRIVATE)
	if err != nil {
		return "", err
	}
	defer unix.Munmap(idx)
	if !bytes.Equal(idx[:8], []byte{255, 't', 'O', 'c', 0, 0, 0, 2}) {
		return "", fmt.Errorf("%w: source index version", planner.ErrUnsupported)
	}
	n := binary.BigEndian.Uint32(idx[1028:1032])
	base := uint64(1072) + 28*uint64(n)
	if n > planner.MaxObjects || uint64(len(idx)) < base || (uint64(len(idx))-base)%8 != 0 {
		return "", fmt.Errorf("%w: source index count", planner.ErrUnsupported)
	}
	offsets := uint64(1032) + 24*uint64(n)
	large := offsets + 4*uint64(n)
	offset := func(ordinal uint32) uint64 {
		v := binary.BigEndian.Uint32(idx[offsets+4*uint64(ordinal):])
		if v&0x80000000 == 0 {
			return uint64(v)
		}
		return binary.BigEndian.Uint64(idx[large+8*uint64(v&0x7fffffff):])
	}
	// The only retained array uses four bytes per object (at most 48 MB).
	ordinals := make([]uint32, n)
	for i := uint32(0); i < n; i++ {
		if i&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return "", err
			}
		}
		v := binary.BigEndian.Uint32(idx[offsets+4*uint64(i):])
		if v&0x80000000 != 0 && large+8*uint64(v&0x7fffffff)+8 > uint64(len(idx))-40 {
			return "", fmt.Errorf("%w: source index large offset", planner.ErrUnsupported)
		}
		ordinals[i] = i
	}
	var sortErr error
	comparisons := 0
	sort.Slice(ordinals, func(i, j int) bool {
		comparisons++
		if comparisons&65535 == 0 {
			sortErr = ctx.Err()
		}
		if sortErr != nil {
			return false
		}
		return offset(ordinals[i]) < offset(ordinals[j])
	})
	if err := ctx.Err(); err != nil {
		return "", err
	}
	owned := filepath.Join(tmp, "source-pack")
	for _, suffix := range []string{".pack", ".idx"} {
		if err := os.Symlink(prefix+suffix, owned+suffix); err != nil {
			return "", err
		}
	}
	rev, err := os.OpenFile(owned+".rev", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	defer rev.Close()
	out := bufio.NewWriterSize(rev, 64<<10)
	hash := sha1.New()
	writer := io.MultiWriter(out, hash)
	if _, err = writer.Write([]byte{'R', 'I', 'D', 'X', 0, 0, 0, 1, 0, 0, 0, 1}); err != nil {
		return "", err
	}
	var row [4]byte
	for i, ordinal := range ordinals {
		if i&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return "", err
			}
		}
		binary.BigEndian.PutUint32(row[:], ordinal)
		if _, err = writer.Write(row[:]); err != nil {
			return "", err
		}
	}
	if _, err = writer.Write(idx[len(idx)-40 : len(idx)-20]); err != nil {
		return "", err
	}
	if _, err = out.Write(hash.Sum(nil)); err != nil {
		return "", err
	}
	if err = out.Flush(); err != nil {
		return "", err
	}
	if err = rev.Close(); err != nil {
		return "", err
	}
	return owned, nil
}
