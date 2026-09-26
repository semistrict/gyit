// Package archive copies a source pack once into immutable bounded segments.
// It does not publish a repository generation or verify Git object contents.
package archive

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"

	"gyit/internal/store"
)

const SegmentSize int64 = 64 << 20

type Result struct {
	Bytes    int64
	Segments uint64
}

func Key(id [16]byte, segment uint64) string {
	return fmt.Sprintf("packs/archive-%s/%08x", hex.EncodeToString(id[:]), segment)
}

// Copy uses at most one 64 MiB upload buffer, independent of source size. Readers
// authenticate reconstructed objects. The caller must finish this copy before
// publishing any manifest that references the archive.
func Copy(ctx context.Context, backend store.Store, source io.ReaderAt, size int64, id [16]byte) (Result, error) {
	return copySegments(ctx, backend, source, size, id, SegmentSize)
}

func copySegments(ctx context.Context, backend store.Store, source io.ReaderAt, size int64, id [16]byte, segmentSize int64) (Result, error) {
	var result Result
	if size <= 0 || segmentSize <= 0 || segmentSize > SegmentSize {
		return result, fmt.Errorf("invalid archive source or segment size")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	buffer := make([]byte, int(min(size, segmentSize)))
	for result.Bytes < size {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		part := buffer[:int(min(size-result.Bytes, segmentSize))]
		n, err := source.ReadAt(part, result.Bytes)
		if n != len(part) {
			if err == nil || err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return result, fmt.Errorf("read archive segment: %w", err)
		}
		if err != nil && err != io.EOF {
			return result, fmt.Errorf("read archive segment: %w", err)
		}
		if err = ctx.Err(); err != nil {
			return result, err
		}
		if err = backend.Put(ctx, Key(id, result.Segments), part, "*"); err != nil {
			return result, fmt.Errorf("write archive segment: %w", err)
		}
		result.Bytes += int64(len(part))
		result.Segments++
	}
	return result, nil
}
