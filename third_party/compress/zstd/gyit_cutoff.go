// Derived from encoder.go in github.com/klauspost/compress v1.20.0.
// Copyright 2019+ Klaus Post. All rights reserved. See LICENSE.
package zstd

import (
	"crypto/rand"
)

// EncodeAllBelow appends the same frame as EncodeAll, but may stop when the
// total output size (including dst) is provably at least limit. On a stopped
// result the output is incomplete and MUST be discarded. The encoder remains
// reusable. Ordinary EncodeAll and all decoder paths are unchanged.
// Early matching exits apply only to SpeedFastest without a dictionary.
func (e *Encoder) EncodeAllBelow(src, dst []byte, limit int) ([]byte, bool) {
	if len(dst) >= limit {
		return dst, true
	}
	// Bound arithmetic and avoid entropy checks when no cutoff is possible.
	if limit-len(dst) >= e.MaxEncodedSize(len(src)) {
		return e.EncodeAll(src, dst), false
	}
	e.init.Do(e.initialize)
	enc := <-e.encoders
	defer func() { e.encoders <- enc }()
	return e.encodeAllBelow(enc, src, dst, limit)
}
func (e *Encoder) encodeAllBelow(enc encoder, src, dst []byte, limit int) ([]byte, bool) {
	if len(src) == 0 {
		if e.o.fullZero {
			// Add frame header.
			fh := frameHeader{
				ContentSize:   0,
				WindowSize:    MinWindowSize,
				SingleSegment: true,
				// Adding a checksum would be a waste of space.
				Checksum: false,
				DictID:   0,
			}
			dst = fh.appendTo(dst)

			// Write raw block as last one only.
			var blk blockHeader
			blk.setSize(0)
			blk.setType(blockTypeRaw)
			blk.setLast(true)
			dst = blk.appendTo(dst)
		}
		return dst, false
	}

	// Use single segments when above minimum window and below window size.
	single := len(src) <= e.o.windowSize && len(src) > MinWindowSize
	if e.o.single != nil {
		single = *e.o.single
	}
	fh := frameHeader{
		ContentSize:   uint64(len(src)),
		WindowSize:    uint32(enc.WindowSize(int64(len(src)))),
		SingleSegment: single,
		Checksum:      e.o.crc,
		DictID:        e.o.dict.ID(),
	}

	// If less than 1MB, allocate a buffer up front.
	if len(dst) == 0 && cap(dst) == 0 && len(src) < 1<<20 && !e.o.lowMem {
		dst = make([]byte, 0, len(src))
	}
	dst = fh.appendTo(dst)

	// If we can do everything in one block, prefer that.
	if len(src) <= e.o.blockSize {
		enc.Reset(e.o.dict, true)
		// Slightly faster with no history and everything in one block.
		if e.o.crc {
			_, _ = enc.CRC().Write(src)
		}
		blk := enc.Block()
		blk.last = true
		if e.o.dict == nil {
			if fast, ok := enc.(*fastEncoder); ok {
				if fast.EncodeNoHistBelow(blk, src, max(1, limit-len(dst))) {
					return dst, true
				}
			} else {
				enc.EncodeNoHist(blk, src)
			}
		} else {
			enc.Encode(blk, src)
		}

		// If we got the exact same number of literals as input,
		// assume the literals cannot be compressed.
		oldout := blk.output
		// Output directly to dst
		blk.output = dst

		err := blk.encode(src, e.o.noEntropy, !e.o.allLitEntropy)
		if err != nil {
			panic(err)
		}
		dst = blk.output
		blk.output = oldout
	} else {
		enc.Reset(e.o.dict, false)
		blk := enc.Block()
		for len(src) > 0 {
			todo := src
			if len(todo) > e.o.blockSize {
				todo = todo[:e.o.blockSize]
			}
			src = src[len(todo):]
			if e.o.crc {
				_, _ = enc.CRC().Write(todo)
			}
			blk.pushOffsets()
			if fast, ok := enc.(*fastEncoder); ok {
				if fast.EncodeBelow(blk, todo, max(1, limit-len(dst))) {
					return dst, true
				}
			} else {
				enc.Encode(blk, todo)
			}
			if len(src) == 0 {
				blk.last = true
			}
			err := blk.encode(todo, e.o.noEntropy, !e.o.allLitEntropy)
			if err != nil {
				panic(err)
			}
			dst = append(dst, blk.output...)
			blk.reset(nil)
			if len(dst) >= limit {
				return dst, true
			}
		}
	}
	if e.o.crc {
		dst = enc.AppendCRC(dst)
	}
	// Add padding with content from crypto/rand.Reader
	if e.o.pad > 0 {
		add := calcSkippableFrame(int64(len(dst)), int64(e.o.pad))
		var err error
		dst, err = skippableFrame(dst, add, rand.Reader)
		if err != nil {
			panic(err)
		}
	}
	return dst, false
}
