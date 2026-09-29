package repo

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

func newCompressor() (*zstd.Encoder, error) {
	return zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))
}

func newFrameDecoder() (*zstd.Decoder, error) {
	return zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(8<<20), zstd.WithDecodeAllCapLimit(true))
}

func decodeFrame(data []byte, limit int) ([]byte, error) {
	dec, err := newFrameDecoder()
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	return decodeFrameWith(dec, data, limit)
}

// A container owns one decoder workspace. DecodeAll resets frame state between
// independent frames; each output still has its own exact bounded allocation.
func decodeFrameWith(dec *zstd.Decoder, data []byte, limit int) ([]byte, error) {
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
