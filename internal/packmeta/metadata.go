package planner

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"os"
	"path/filepath"
	"unsafe"

	wire "gat/internal/archive/wire"
	"golang.org/x/sys/unix"
)

// OpenSource constructs complete kind/size metadata and bounded recipe plans
// over one physical source graph. Unsupported metadata aborts this constructor;
// the caller must fall back before publishing or dispatching conversion work.
func OpenSource(ctx context.Context, prefix, tmp string) (_ *Planner, retErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if unsafe.Sizeof(record{}) != 40 {
		return nil, fmt.Errorf("unsupported record layout")
	}
	p := &Planner{ctx: ctx, seed: maphash.MakeSeed()}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, p.Close())
		}
	}()
	var err error
	if p.idx, err = mapSource(prefix + ".idx"); err != nil {
		return nil, err
	}
	if p.rev, err = mapSource(prefix + ".rev"); err != nil {
		return nil, err
	}
	if p.pack, err = mapSource(prefix + ".pack"); err != nil {
		return nil, err
	}
	p.stats.SourceMappedBytes = uint64(len(p.idx)) + uint64(len(p.rev)) + uint64(len(p.pack))
	count, err := p.validateSource()
	if err != nil {
		return nil, sourceError(err)
	}
	p.stats.SourceObjects = uint64(count)
	if p.scratchDir, err = os.MkdirTemp(tmp, "pack-metadata-"); err != nil {
		return nil, err
	}
	if err = p.allocate(count); err != nil {
		return nil, err
	}
	for physical := uint32(0); physical < count; physical++ {
		if physical&4095 == 0 {
			if err = ctx.Err(); err != nil {
				return nil, err
			}
		}
		ordinal := binary.BigEndian.Uint32(p.rev[12+4*uint64(physical) : 16+4*uint64(physical)])
		if ordinal >= count {
			return nil, sourceError(fmt.Errorf("%w: reverse ordinal", ErrMalformed))
		}
		off, e := p.indexOffset(ordinal, count)
		if e != nil {
			return nil, sourceError(e)
		}
		if physical > 0 && off <= p.records[physical-1].offset {
			return nil, sourceError(fmt.Errorf("%w: reverse order", ErrMalformed))
		}
		p.records[physical].offset = off
		p.records[physical].parent = noParent
		p.addOrdinal(physical)
	}
	var prefixBuffer [PrefixOutputLimit]byte
	for n := uint32(0); n < count; n++ {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if err = p.readMetadataHeader(n, prefixBuffer[:]); err != nil {
			return nil, sourceError(err)
		}
	}
	if err = p.resolveMetadata(); err != nil {
		return nil, sourceError(err)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return p, nil
}

func sourceError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return errors.Join(ErrUnsupported, err)
}

// Lookup proves membership in this source pack only. The size/kind have not
// authenticated object contents. No borrowed mapping escapes the lifecycle lock.
func (p *Planner) Lookup(id [20]byte) (byte, int64, bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return 0, 0, false, ErrClosed
	}
	if err := p.ctx.Err(); err != nil {
		return 0, 0, false, err
	}
	n, found, _ := p.ordinal(id[:])
	if !found {
		return 0, 0, false, nil
	}
	r := p.records[n]
	return r.finalKind(), int64(r.size), true, nil
}

// ForEachBlob visits all local pack blobs, including unreachable ones. The
// callback must not reenter any Planner method; see ForEachObject's lifetime.
func (p *Planner) ForEachBlob(ctx context.Context, emit func([20]byte, int64) error) error {
	if emit == nil {
		return fmt.Errorf("nil blob callback")
	}
	return p.ForEachObject(ctx, func(id [20]byte, kind byte, size int64) error {
		if kind == 3 {
			return emit(id, size)
		}
		return nil
	})
}

// ForEachObject visits complete source metadata in physical order with no
// additional array or spool. Callback arguments are owned scalar copies. The
// planner owns all mappings and holds its lifecycle read lock for the visit;
// callbacks must not reenter Planner methods because Close may be pending.
func (p *Planner) ForEachObject(ctx context.Context, emit func([20]byte, byte, int64) error) error {
	if emit == nil {
		return fmt.Errorf("nil object callback")
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.ctx.Err(); err != nil {
		return err
	}
	for n := range p.records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := p.ctx.Err(); err != nil {
			return err
		}
		r := p.records[n]
		var id [20]byte
		copy(id[:], p.oid(uint32(n)))
		if err := emit(id, r.finalKind(), int64(r.size)); err != nil {
			return err
		}
	}
	return nil
}

func (p *Planner) readMetadataHeader(n uint32, prefixBuffer []byte) error {
	r := &p.records[n]
	pos, end := r.offset, p.end(n)
	p.stats.SourceHeaders++
	defer func() { p.stats.SourceHeaderBytes += pos - r.offset }()
	read := func() (byte, error) {
		if pos >= end {
			return 0, fmt.Errorf("%w: truncated header", ErrMalformed)
		}
		b := p.pack[pos]
		pos++
		return b, nil
	}
	b, err := read()
	if err != nil {
		return err
	}
	kind := b >> 4 & 7
	raw := uint64(b & 15)
	for shift := uint(4); b&128 != 0; shift += 7 {
		if shift >= 64 {
			return fmt.Errorf("%w: header size overflow", ErrMalformed)
		}
		b, err = read()
		if err != nil {
			return err
		}
		if uint64(b&127) > math.MaxUint64>>shift {
			return fmt.Errorf("%w: header size overflow", ErrMalformed)
		}
		raw |= uint64(b&127) << shift
	}
	if raw > math.MaxInt64 {
		return fmt.Errorf("%w: header size exceeds metadata API", ErrUnsupported)
	}
	r.raw = raw
	switch kind {
	case 1, 2, 3, 4:
		r.kind = kind
		r.size = raw
		p.stats.Roots++
		switch kind {
		case 1:
			p.stats.RootCommits++
		case 2:
			p.stats.RootTrees++
		case 3:
			p.stats.RootBlobs++
		case 4:
			p.stats.RootTags++
		}
	case 6:
		p.stats.OFSParents++
		b, err = read()
		if err != nil {
			return err
		}
		distance := uint64(b & 127)
		for b&128 != 0 {
			if distance >= math.MaxUint64>>7 {
				return fmt.Errorf("%w: OFS overflow", ErrMalformed)
			}
			b, err = read()
			if err != nil {
				return err
			}
			distance = ((distance + 1) << 7) | uint64(b&127)
		}
		if distance == 0 || distance > r.offset-12 {
			return fmt.Errorf("%w: OFS bounds", ErrMalformed)
		}
		base, found, probes := p.atOffset(r.offset - distance)
		p.stats.ParentLookups++
		p.stats.OffsetProbeSteps += probes
		if !found {
			return fmt.Errorf("%w: missing OFS base", ErrMalformed)
		}
		r.parent = base
	case 7:
		p.stats.REFParents++
		if end-pos < 20 {
			return fmt.Errorf("%w: truncated REF", ErrMalformed)
		}
		base, found, probes := p.ordinal(p.pack[pos : pos+20])
		pos += 20
		p.stats.ParentLookups++
		p.stats.OIDProbeSteps += probes
		if !found {
			return fmt.Errorf("%w: missing REF base", ErrMalformed)
		}
		r.parent = base
	default:
		return fmt.Errorf("%w: native kind", ErrMalformed)
	}
	if pos >= end || pos-r.offset > 32 {
		return fmt.Errorf("%w: header extent", ErrMalformed)
	}
	r.body = uint8(pos - r.offset)
	if kind == 6 || kind == 7 {
		p.stats.PrefixInflations++
		produced, consumed, e := metadataPrefix(prefixBuffer, p.pack[pos:end], raw)
		p.stats.PrefixInputBytes += consumed
		p.stats.PrefixOutputBytes += uint64(produced)
		if e != nil {
			if errors.Is(e, ErrUnsupported) {
				p.stats.PrefixInputLimitHits++
			}
			return e
		}
		prefix := prefixBuffer[:produced]
		baseSize, baseWidth := binary.Uvarint(prefix)
		if baseWidth <= 0 {
			return fmt.Errorf("%w: delta base varint", ErrMalformed)
		}
		targetSize, targetWidth := binary.Uvarint(prefix[baseWidth:])
		if targetWidth <= 0 {
			return fmt.Errorf("%w: delta target varint", ErrMalformed)
		}
		if baseSize > math.MaxInt64 || targetSize > math.MaxInt64 {
			return fmt.Errorf("%w: delta size exceeds metadata API", ErrUnsupported)
		}
		r.size = targetSize
		// Metadata can represent an overlong ten-byte encoding of a small size,
		// but the existing archive reader accepts at most nine bytes per varint.
		if baseWidth > 9 || targetWidth > 9 {
			r.reason = reasonProgram
		}
	}
	return nil
}

// Metadata type resolution never stops at a read-admission limit. Even a small
// child of an oversized ancestor needs its true inherited Git kind and size.
func (p *Planner) resolveMetadata() (retErr error) {
	if len(p.records) == 0 {
		return nil
	}
	nbytes := uint64(len(p.records)) * 4
	b, err := p.mapScratch("build-stack", nbytes)
	if err != nil {
		return err
	}
	p.stats.BuildStackBytes = nbytes
	stack := unsafe.Slice((*uint32)(unsafe.Pointer(&b[0])), len(p.records))
	defer func() {
		if err := unix.Munmap(b); err != nil {
			retErr = errors.Join(retErr, err)
		} else {
			p.mappings[len(p.mappings)-1] = nil
			p.stats.ScratchMappedBytes -= nbytes
		}
		retErr = errors.Join(retErr, os.Remove(filepath.Join(p.scratchDir, "build-stack")))
	}()
	for first := uint32(0); int(first) < len(p.records); first++ {
		if first&4095 == 0 {
			if err := p.ctx.Err(); err != nil {
				return err
			}
		}
		if p.records[first].state() == 2 {
			continue
		}
		used := 0
		for current := first; ; {
			if used&4095 == 0 {
				if err := p.ctx.Err(); err != nil {
					return err
				}
			}
			r := &p.records[current]
			if r.state() == 2 {
				break
			}
			if r.state() == 1 {
				return fmt.Errorf("%w: source dependency cycle", ErrMalformed)
			}
			if used == len(stack) {
				return fmt.Errorf("%w: dependency traversal", ErrMalformed)
			}
			r.setState(1)
			stack[used] = current
			used++
			p.stats.TypeDPNodeVisits++
			p.stats.DPNodeVisits++
			if r.parent == noParent {
				break
			}
			p.stats.TypeDPEdgeVisits++
			p.stats.DPEdgeVisits++
			current = r.parent
		}
		for used > 0 {
			used--
			n := stack[used]
			r := &p.records[n]
			if r.parent == noParent {
				r.root = n
				r.depth = 1
				if r.size > wire.ObjectLimit {
					r.reason = reasonSize
				} else {
					r.work = uint32(r.raw)
				}
			} else {
				base := &p.records[r.parent]
				if base.state() != 2 || base.finalKind() < 1 || base.finalKind() > 4 {
					return fmt.Errorf("%w: unresolved source type", ErrMalformed)
				}
				r.kind = r.kind&^7 | base.finalKind()
				r.root = base.root
				switch {
				case r.reason != reasonOK:
				case r.size > wire.ObjectLimit:
					r.reason = reasonSize
				case r.raw > wire.ProgramLimit:
					r.reason = reasonProgram
				case base.reason != reasonOK:
					r.reason = base.reason
				case base.depth >= wire.FrameLimit:
					r.reason = reasonDepth
				default:
					r.depth = base.depth + 1
					work := uint64(base.work) + r.raw + r.size
					if work > wire.WorkLimit {
						r.reason = reasonWork
					} else {
						r.work = uint32(work)
					}
				}
			}
			if p.end(n)-(r.offset+uint64(r.body)) > wire.PackedLimit {
				r.reason = reasonPacked
			}
			r.setState(2)
			p.stats.MetadataObjects++
			switch r.finalKind() {
			case 1:
				p.stats.MetadataCommits++
			case 2:
				p.stats.MetadataTrees++
			case 4:
				p.stats.MetadataTags++
			}
			if r.finalKind() == 3 {
				p.stats.MetadataBlobs++
				if math.MaxUint64-p.stats.MetadataBlobBytes < r.size {
					return fmt.Errorf("%w: aggregate blob size", ErrUnsupported)
				}
				p.stats.MetadataBlobBytes += r.size
			}
			if r.reason != reasonOK {
				p.stats.LimitNodes++
			}
		}
	}
	return nil
}
