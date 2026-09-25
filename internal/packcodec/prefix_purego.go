//go:build !cgo

package wirecodec

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
)

// InflateMetadataPrefix returns at most twenty decoded bytes from one zlib
// frame. Callers bound compressed input before calling. The standard decoder
// may expand one internal 32 KiB window, but a longer frame's remaining content
// and trailer are not requested or authenticated by this operation.
func InflateMetadataPrefix(dst, src []byte, rawSize uint64) (n int, consumed uint64, err error) {
	if len(dst) == 0 || len(dst) > 20 || len(src) == 0 || rawSize < uint64(len(dst)) {
		return 0, 0, fmt.Errorf("invalid metadata prefix bounds")
	}
	input := bytes.NewReader(src)
	defer func() { consumed = uint64(len(src) - input.Len()) }()
	r, err := zlib.NewReader(input)
	if err != nil {
		return 0, 0, err
	}
	defer r.Close()
	for n < len(dst) {
		var got int
		got, err = r.Read(dst[n:])
		n += got
		if err == io.EOF && uint64(n) == rawSize {
			return n, 0, nil
		}
		if err != nil {
			return n, 0, fmt.Errorf("metadata prefix: %w", err)
		}
		if got == 0 {
			return n, 0, io.ErrNoProgress
		}
	}
	if rawSize > uint64(n) {
		return n, 0, nil
	}
	// A complete short program must match its declared size and authenticate its
	// zlib trailer, just as native uncompress2 does when the full frame fits.
	var extra [1]byte
	got, err := r.Read(extra[:])
	if got != 0 || err != io.EOF {
		return n, 0, fmt.Errorf("metadata prefix does not end at its declared size: %v", err)
	}
	return n, 0, nil
}
