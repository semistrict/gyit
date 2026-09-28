package repo

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

const compressorName = "Go Zstandard SpeedFastest"

func newCompressor() (*zstd.Encoder, error) {
	return zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))
}

func decodeFrame(data []byte, limit int) ([]byte, error) {
	var header zstd.Header
	if err := header.Decode(data); err != nil {
		return nil, err
	}
	capacity := limit
	if header.HasFCS {
		if header.FrameContentSize > uint64(limit) {
			return nil, fmt.Errorf("frame exceeds decoded size limit")
		}
		capacity = int(header.FrameContentSize)
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(8<<20), zstd.WithDecodeAllCapLimit(true))
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	decoded, err := dec.DecodeAll(data, make([]byte, 0, capacity))
	if err != nil {
		return nil, err
	}
	// The LRU accounts for length; do not retain oversized decode buffers.
	if cap(decoded) != len(decoded) {
		decoded = bytes.Clone(decoded)
	}
	return decoded, nil
}
