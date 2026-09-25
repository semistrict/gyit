package packrecipe

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	probev1 "gat/internal/gen/gat/probe/v1"
	"gat/internal/gitdelta"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

type sourceWorkCounters struct {
	FullInflations, FullInflatedBytes, Reconstructions, ReconstructedBytes, GetCalls int64
}

// Snapshots are read only after this reader's owning worker is joined. Counters
// describe actual code paths, not inferred savings from target byte counts.
type ConversionCounters struct {
	TreeCalls, TreeObjects, TreeRawBytes, TreeLimitFallbacks                                             int64
	TreePayloadBytes, TreeDecodedWork                                                                    int64
	TreeFullInflations, TreeFullInflatedBytes, TreeReconstructions, TreeReconstructedBytes, TreeGetCalls int64
	FastObjects, FastRawBytes, LegacyObjects, LegacyRawBytes, EagerObjects, EagerRawBytes                int64
	PrefixInflations, PrefixBytes, FramePlans, MetadataHits                                              int64
	BundledFrameBytes, ReturnedRootBytes                                                                 int64
	FullInflations, FullInflatedBytes, Reconstructions, ReconstructedBytes, GetCalls                     int64
	FastFullInflations, FastFullInflatedBytes, FastReconstructions, FastReconstructedBytes, FastGetCalls int64
}

func (r *Reader) SnapshotCounters() ConversionCounters {
	s := r.conversion
	s.FullInflations = r.p.work.FullInflations
	s.FullInflatedBytes = r.p.work.FullInflatedBytes
	s.Reconstructions = r.p.work.Reconstructions
	s.ReconstructedBytes = r.p.work.ReconstructedBytes
	s.GetCalls = r.p.work.GetCalls
	return s
}

func (r *Reader) Convert(oid string, dst []byte) (Output, error) {
	if err := r.ctx.Err(); err != nil {
		return Output{}, err
	}
	if r.deferred == nil {
		out, err := r.convertEager(oid, dst)
		if err == nil {
			r.conversion.EagerObjects++
			r.conversion.EagerRawBytes += int64(out.Size)
		}
		return out, err
	}
	return r.convertDeferred(oid, dst)
}

type deferredFrame struct {
	header                          frame
	base, end, objectSize, baseSize int
	oid                             [20]byte
}

type deferredSource struct {
	p       *packReader
	reverse []byte
	frames  bounded[deferredFrame]
}

func openDeferred(p *packReader, prefix string) (*deferredSource, error) {
	if p.count < 0 || p.count > 12_000_000 {
		return nil, fmt.Errorf("deferred source index count")
	}
	rev, err := mapped(prefix + ".rev")
	if err != nil {
		return nil, fmt.Errorf("deferred reverse index: %w", err)
	}
	d := &deferredSource{p: p, reverse: rev, frames: bounded[deferredFrame]{limit: 16 << 20}}
	if len(rev) != 12+4*p.count+40 || string(rev[:4]) != "RIDX" || binary.BigEndian.Uint32(rev[4:8]) != 1 || binary.BigEndian.Uint32(rev[8:12]) != 1 || !bytes.Equal(rev[len(rev)-40:len(rev)-20], p.pack[len(p.pack)-20:]) {
		d.close()
		return nil, fmt.Errorf("invalid deferred SHA1 reverse index")
	}
	return d, nil
}

func (d *deferredSource) close() {
	if d.reverse != nil {
		unix.Munmap(d.reverse)
		d.reverse = nil
	}
}

// ordinalAt reads an original index ordinal at a physical pack position. Every
// selected offset is bounds checked; source checksums/OID proofs are deliberately
// not recomputed here. Mounted readers must authenticate the returned objects.
func (d *deferredSource) ordinalAt(position int) (int, int, error) {
	if position < 0 || position >= d.p.count {
		return 0, 0, fmt.Errorf("reverse position bounds")
	}
	ordinal := int(binary.BigEndian.Uint32(d.reverse[12+4*position:]))
	if ordinal >= d.p.count {
		return 0, 0, fmt.Errorf("reverse ordinal bounds")
	}
	off := uint64(binary.BigEndian.Uint32(d.p.idx[1032+24*d.p.count+4*ordinal:]))
	if off&0x80000000 != 0 {
		pos := uint64(1032+28*d.p.count) + 8*(off&0x7fffffff)
		if pos+8 > uint64(len(d.p.idx)-40) {
			return 0, 0, fmt.Errorf("deferred large offset bounds")
		}
		off = binary.BigEndian.Uint64(d.p.idx[pos:])
	}
	if off < 12 || off >= uint64(len(d.p.pack)-20) {
		return 0, 0, fmt.Errorf("deferred source offset bounds")
	}
	return ordinal, int(off), nil
}

func (d *deferredSource) extent(offset int) (int, int, error) {
	lo, hi := 0, d.p.count
	for lo < hi {
		mid := lo + (hi-lo)/2
		_, off, err := d.ordinalAt(mid)
		if err != nil {
			return 0, 0, err
		}
		if off < offset {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == d.p.count {
		return 0, 0, fmt.Errorf("source frame absent from reverse index")
	}
	ordinal, off, err := d.ordinalAt(lo)
	if err != nil {
		return 0, 0, err
	}
	if off != offset {
		return 0, 0, fmt.Errorf("source frame absent from reverse index")
	}
	end := len(d.p.pack) - 20
	if lo+1 < d.p.count {
		_, end, err = d.ordinalAt(lo + 1)
	}
	if err != nil {
		return 0, 0, err
	}
	if end <= offset {
		return 0, 0, fmt.Errorf("unordered source extent")
	}
	return ordinal, end, nil
}

func (r *Reader) planFrame(offset int) (deferredFrame, error) {
	d := r.deferred
	if f, ok := d.frames.get(offset); ok {
		r.conversion.MetadataHits++
		return f, nil
	}
	r.conversion.FramePlans++
	h, base, err := r.p.header(offset)
	if err != nil {
		return deferredFrame{}, err
	}
	ordinal, end, err := d.extent(offset)
	if err != nil {
		return deferredFrame{}, err
	}
	if h.body >= end {
		return deferredFrame{}, fmt.Errorf("empty source frame")
	}
	if h.size > 2<<20 || end-h.body > 2<<20 {
		return deferredFrame{}, gitdelta.ErrLimit
	}
	f := deferredFrame{header: h, base: base, end: end, objectSize: h.size}
	copy(f.oid[:], r.p.idx[1032+20*ordinal:1032+20*(ordinal+1)])
	if base == 0 {
		if h.kind != 3 && h.kind != 2 {
			return deferredFrame{}, gitdelta.ErrLimit
		}
		if h.size > 1<<20 {
			return deferredFrame{}, gitdelta.ErrLimit
		}
	} else {
		if h.size < 2 {
			return deferredFrame{}, fmt.Errorf("missing delta sizes")
		}
		var prefix [20]byte
		r.conversion.PrefixInflations++
		n, err := inflateDeferredPrefix(prefix[:min(h.size, len(prefix))], r.p.pack[h.body:end], h.size)
		r.conversion.PrefixBytes += int64(n)
		if err != nil {
			return deferredFrame{}, err
		}
		baseSize, k := binary.Uvarint(prefix[:n])
		if k <= 0 {
			return deferredFrame{}, fmt.Errorf("invalid delta base size")
		}
		targetSize, k := binary.Uvarint(prefix[k:n])
		if k <= 0 {
			return deferredFrame{}, fmt.Errorf("invalid delta target size")
		}
		if baseSize > 1<<20 || targetSize > 1<<20 {
			return deferredFrame{}, gitdelta.ErrLimit
		}
		f.baseSize, f.objectSize = int(baseSize), int(targetSize)
	}
	d.frames.put(offset, f, 128)
	return f, nil
}

func (r *Reader) convertDeferred(oid string, dst []byte) (Output, error) {
	before := r.p.work
	id, err := hex.DecodeString(oid)
	if err != nil {
		return Output{}, err
	}
	offset, err := r.p.offset(id)
	if err != nil {
		return Output{}, err
	}
	var chain [64]deferredFrame
	used := 0
	for {
		if err := r.ctx.Err(); err != nil {
			return Output{}, err
		}
		if used == len(chain) {
			return Output{}, gitdelta.ErrLimit
		}
		for i := 0; i < used; i++ {
			if chain[i].header.offset == offset {
				return Output{}, fmt.Errorf("native dependency cycle")
			}
		}
		f, err := r.planFrame(offset)
		if err != nil {
			return Output{}, err
		}
		if used > 0 && chain[used-1].baseSize != f.objectSize {
			return Output{}, fmt.Errorf("delta ancestor size mismatch")
		}
		chain[used] = f
		used++
		if f.base == 0 {
			break
		}
		offset = f.base
	}
	target, root := chain[0], chain[used-1]
	if root.header.kind != 3 {
		return Output{}, gitdelta.ErrLimit
	}
	if used > 1 && (target.objectSize < 4096 || root.objectSize > target.objectSize*5/4) {
		out, err := r.convertEager(oid, dst)
		if err == nil {
			r.conversion.LegacyObjects++
			r.conversion.LegacyRawBytes += int64(out.Size)
			r.conversion.EagerObjects++
			r.conversion.EagerRawBytes += int64(out.Size)
		}
		return out, err
	}
	rootPacked := r.p.pack[root.header.body:root.end]
	hash := "git-sha1:" + hex.EncodeToString(id)
	out := Output{Hash: hash, Size: target.objectSize}
	if used == 1 {
		out.Data, out.Full = rootPacked, true
	} else {
		bundle := &probev1.NativeProgramBundle{Frames: make([]*probev1.NativeFrame, 0, used-1)}
		var copied int64
		for i := used - 2; i >= 0; i-- {
			f := chain[i]
			packed := r.p.pack[f.header.body:f.end]
			copied += int64(len(packed))
			bundle.Frames = append(bundle.Frames, &probev1.NativeFrame{RawSize: uint32(f.header.size), Zlib: packed})
		}
		if proto.Size(bundle) > 2<<20 {
			return Output{}, gitdelta.ErrLimit
		}
		out.Data, err = proto.MarshalOptions{Deterministic: true}.MarshalAppend(dst[:0], bundle)
		if err != nil {
			return Output{}, err
		}
		out.Base = &Base{Packed: rootPacked, Hash: "git-sha1:" + hex.EncodeToString(root.oid[:])}
		r.conversion.BundledFrameBytes += copied
	}
	r.conversion.ReturnedRootBytes += int64(len(rootPacked))
	r.conversion.FastObjects++
	r.conversion.FastRawBytes += int64(out.Size)
	r.conversion.FastFullInflations += r.p.work.FullInflations - before.FullInflations
	r.conversion.FastFullInflatedBytes += r.p.work.FullInflatedBytes - before.FullInflatedBytes
	r.conversion.FastReconstructions += r.p.work.Reconstructions - before.Reconstructions
	r.conversion.FastReconstructedBytes += r.p.work.ReconstructedBytes - before.ReconstructedBytes
	r.conversion.FastGetCalls += r.p.work.GetCalls - before.GetCalls
	return out, nil
}
