package repo

import "github.com/klauspost/compress/zstd"

const compressorName = "Go Zstandard SpeedFastest"

func newCompressor() (*zstd.Encoder, error) {
	return zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))
}
