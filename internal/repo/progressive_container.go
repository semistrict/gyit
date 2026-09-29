package repo

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"
)

const progressiveContainerBytes = 256 << 10
const progressiveContainerDecodedBytes = 4 << 20

var errDirectoryExpansion = errors.New("directory container exceeds prefetch decode budget")

// directoryBytes fetches an immutable container once, then keeps only decoded
// pages in the existing bounded cache. The singleflight result lives only until
// its waiters return; it also serves readers when the disk cache is full/disabled.
func (p *Progressive) directoryBytes(ctx context.Context, ref pageRef) ([]byte, error) {
	if !strings.HasPrefix(ref.Pack, "index/progressive-") || ref.Offset < 0 || ref.Length <= 0 || ref.Length > 128<<10 || ref.Offset > progressiveContainerBytes-ref.Length {
		return nil, fmt.Errorf("metadata range bounds")
	}
	key := "progressive-directory/" + ref.Hash
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if raw, ok := p.cache.get(key); ok {
			return raw, nil
		}
		ch := p.cache.flight.DoChan("progressive-container/"+ref.Pack, func() (any, error) {
			// A previous flight may have populated the cache between the caller's miss
			// and joining this flight. Nil tells all waiters to recheck their own page.
			if _, ok := p.cache.get(key); ok {
				return nil, nil
			}
			select {
			case p.cache.slots <- struct{}{}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			defer func() { <-p.cache.slots }()
			packed, _, err := p.store.Get(ctx, ref.Pack, 0, -1)
			if err != nil {
				return nil, err
			}
			if len(packed) == 0 || len(packed) > progressiveContainerBytes {
				return nil, fmt.Errorf("metadata container size")
			}
			pages, err := decodeDirectoryContainer(ctx, packed)
			if err != nil {
				return nil, err
			}
			for _, page := range pages {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				p.cache.put("progressive-directory/"+page.hash, page.raw)
			}
			return pages, nil
		})
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case result := <-ch:
			if errors.Is(result.Err, errDirectoryExpansion) {
				// Highly compressible, valid containers must still work with bounded
				// scratch memory. Fall back to decoding only the requested page.
				return p.cache.load(ctx, key, func() ([]byte, error) {
					packed, _, err := p.store.Get(ctx, ref.Pack, ref.Offset, ref.Length)
					if err != nil {
						return nil, err
					}
					if fmt.Sprintf("%x", sha256.Sum256(packed)) != ref.Hash {
						return nil, fmt.Errorf("metadata checksum")
					}
					return decodeFrame(packed, 64<<10)
				})
			}
			if result.Err != nil {
				return nil, result.Err
			}
			if result.Val == nil {
				continue
			}
			page, ok := result.Val.(map[int64]directoryFrame)[ref.Offset]
			if !ok || page.length != ref.Length || page.hash != ref.Hash {
				return nil, fmt.Errorf("metadata checksum")
			}
			return page.raw, nil
		}
	}
}

type directoryFrame struct {
	hash   string
	length int64
	raw    []byte
}

func decodeDirectoryContainer(ctx context.Context, packed []byte) (map[int64]directoryFrame, error) {
	pages := make(map[int64]directoryFrame)
	if len(packed) == 0 {
		return pages, nil
	}
	decoder, err := newFrameDecoder()
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	var offset int64
	total := 0
	for len(packed) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, err := directoryFrameLength(packed)
		if err != nil {
			return nil, err
		}
		raw, err := decodeFrameWith(decoder, packed[:n], 64<<10)
		if err != nil {
			return nil, err
		}
		total += len(raw)
		if total > progressiveContainerDecodedBytes || len(pages) >= 8192 {
			return nil, errDirectoryExpansion
		}
		hash := fmt.Sprintf("%x", sha256.Sum256(packed[:n]))
		pages[offset] = directoryFrame{hash: hash, length: int64(n), raw: raw}
		offset += int64(n)
		packed = packed[n:]
	}
	return pages, nil
}

// directoryFrameLength walks Zstandard block headers without decoding payloads.
// Frames remain independently checksummed and use the existing storage format.
func directoryFrameLength(b []byte) (int, error) {
	var h zstd.Header
	if err := h.Decode(b); err != nil {
		return 0, err
	}
	if h.Skippable || h.DictionaryID != 0 {
		return 0, fmt.Errorf("unsupported metadata frame")
	}
	off := h.HeaderSize
	for {
		if len(b)-off < 3 {
			return 0, io.ErrUnexpectedEOF
		}
		header := uint32(b[off]) | uint32(b[off+1])<<8 | uint32(b[off+2])<<16
		off += 3
		size := int(header >> 3)
		switch (header >> 1) & 3 {
		case 1:
			size = 1
		case 3:
			return 0, fmt.Errorf("reserved metadata block type")
		}
		if size > len(b)-off {
			return 0, io.ErrUnexpectedEOF
		}
		off += size
		if header&1 != 0 {
			break
		}
	}
	if h.HasCheckSum {
		if len(b)-off < 4 {
			return 0, io.ErrUnexpectedEOF
		}
		off += 4
	}
	return off, nil
}
