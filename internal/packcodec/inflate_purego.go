//go:build !cgo

package wirecodec

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"sync"
)

func Inflate(dst, src []byte) error {
	_, consumed, err := inflateInto(dst, src, true)
	if err == nil && consumed != len(src) {
		err = fmt.Errorf("inflate: trailing frame bytes")
	}
	return err
}

func Deflate(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, fmt.Errorf("empty input")
	}
	var dst bytes.Buffer
	w, err := zlib.NewWriterLevel(&dst, zlib.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(src); err != nil {
		w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return dst.Bytes(), nil
}

func InflatePrefix(dst, src []byte) (int, error) {
	_, consumed, err := inflateInto(dst, src, true)
	return consumed, err
}

var inflateScratch sync.Pool

func InflateBounded(src []byte, limit int) ([]byte, error) {
	if len(src) == 0 || limit < 1 || limit > 2<<20 {
		return nil, fmt.Errorf("invalid inflate bound")
	}
	v := inflateScratch.Get()
	var dst []byte
	if v == nil {
		dst = make([]byte, 2<<20)
	} else {
		dst = v.([]byte)
	}
	defer inflateScratch.Put(dst)
	n, consumed, err := inflateInto(dst[:limit], src, false)
	if err != nil {
		return nil, err
	}
	if consumed != len(src) {
		return nil, fmt.Errorf("inflate bounded: trailing frame bytes")
	}
	return bytes.Clone(dst[:n]), nil
}

// bytes.Reader implements io.ByteReader, so zlib stops at the end of exactly
// one frame without consuming bytes from the next packed object.
func inflateInto(dst, src []byte, exact bool) (size, consumed int, err error) {
	if len(src) == 0 {
		return 0, 0, fmt.Errorf("empty frame")
	}
	input := bytes.NewReader(src)
	r, err := zlib.NewReader(input)
	if err != nil {
		return 0, 0, fmt.Errorf("inflate: %w", err)
	}
	defer r.Close()
	n, err := io.ReadFull(r, dst)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return 0, 0, fmt.Errorf("inflate: %w", err)
	}
	if exact && n != len(dst) {
		return 0, 0, fmt.Errorf("inflate: decoded size %d, expected %d", n, len(dst))
	}
	// Reading beyond the requested output both rejects oversized data and forces
	// validation of the zlib trailer when the output exactly fills dst.
	var extra [1]byte
	more, err := r.Read(extra[:])
	if more != 0 {
		return 0, 0, fmt.Errorf("inflate: decoded size exceeds %d", len(dst))
	}
	if err != io.EOF {
		if err == nil {
			err = io.ErrNoProgress
		}
		return 0, 0, fmt.Errorf("inflate: %w", err)
	}
	return n, len(src) - input.Len(), nil
}
