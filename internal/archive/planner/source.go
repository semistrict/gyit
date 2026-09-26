package planner

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"unsafe"

	wire "gyit/internal/archive/wire"
	"golang.org/x/sys/unix"
)

// readHeader reads only the native record header, including its OFS distance or
// REF OID. Extents come from the reverse index, sizes/kinds from real inventory.
// It deliberately does not inspect zlib bytes, CRCs or delta size prefixes.
func (p *Planner) readHeader(n uint32, inventorySize int64, found bool) {
	r := &p.records[n]
	pos, end := r.offset, p.end(n)
	p.stats.SourceHeaders++
	defer func() { p.stats.SourceHeaderBytes += pos - r.offset }()
	malformed := func(reason uint8) { r.reason = reason }
	limit := func(reason uint8) {
		if r.reason == reasonOK {
			r.reason = reason
		}
	}
	read := func() (byte, bool) {
		if pos >= end {
			return 0, false
		}
		b := p.pack[pos]
		pos++
		return b, true
	}
	b, ok := read()
	if !ok {
		malformed(reasonHeader)
		return
	}
	nativeKind := b >> 4 & 7
	raw := uint64(b & 15)
	for shift := uint(4); b&128 != 0; shift += 7 {
		if shift >= 64 {
			malformed(reasonHeader)
			return
		}
		b, ok = read()
		if !ok || uint64(b&127) > math.MaxUint64>>shift {
			malformed(reasonHeader)
			return
		}
		raw |= uint64(b&127) << shift
	}
	switch nativeKind {
	case 1, 2, 3, 4:
		p.stats.Roots++
		if r.finalKind() != nativeKind {
			malformed(reasonKind)
		}
		if found && inventorySize >= 0 && raw != uint64(inventorySize) {
			malformed(reasonMetadata)
		}
		if raw > wire.ObjectLimit {
			limit(reasonSize)
		} else {
			r.raw = uint32(raw)
		}
	case 6:
		p.stats.OFSParents++
		b, ok = read()
		if !ok {
			malformed(reasonHeader)
			return
		}
		distance := uint64(b & 127)
		for b&128 != 0 {
			if distance >= math.MaxUint64>>7 {
				malformed(reasonHeader)
				return
			}
			b, ok = read()
			if !ok {
				malformed(reasonHeader)
				return
			}
			distance = ((distance + 1) << 7) | uint64(b&127)
		}
		if distance == 0 || distance > r.offset-12 {
			malformed(reasonParent)
			return
		}
		base, exists, probes := p.atOffset(r.offset - distance)
		p.stats.ParentLookups++
		p.stats.OffsetProbeSteps += probes
		if !exists {
			malformed(reasonParent)
			return
		}
		r.parent = base
	case 7:
		p.stats.REFParents++
		if end-pos < 20 {
			malformed(reasonHeader)
			return
		}
		base, exists, probes := p.ordinal(p.pack[pos : pos+20])
		pos += 20
		p.stats.ParentLookups++
		p.stats.OIDProbeSteps += probes
		if !exists {
			malformed(reasonParent)
			return
		}
		r.parent = base
	default:
		malformed(reasonHeader)
		return
	}
	if nativeKind == 6 || nativeKind == 7 {
		if raw < 2 {
			malformed(reasonHeader)
		} else if raw > wire.ProgramLimit {
			limit(reasonProgram)
		} else {
			r.raw = uint32(raw)
		}
	}
	if pos >= end || pos-r.offset > 32 {
		malformed(reasonHeader)
		return
	}
	r.body = uint8(pos - r.offset)
	if end-pos > wire.PackedLimit {
		limit(reasonPacked)
	}
}

// resolve touches each dependency node once, even for forward REF chains.
// Depth, decoded work and ancestor-size failures are monotone; range count is
// not (a child can fill a physical gap), so range admission stays in Recipe.
func (p *Planner) resolve() (retErr error) {
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
		// The construction stack is not retained by the immutable planner.
		if err := unix.Munmap(b); err != nil {
			if retErr == nil {
				retErr = err
			}
		} else {
			p.mappings[len(p.mappings)-1] = nil
			p.stats.ScratchMappedBytes -= nbytes
		}
		if err := os.Remove(filepath.Join(p.scratchDir, "build-stack")); err != nil && retErr == nil {
			retErr = err
		}
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
				// Every node on this path depends on the cycle, including the
				// acyclic prefix. Unrelated source families remain usable.
				for _, n := range stack[:used] {
					p.records[n].reason = reasonCycle
					p.records[n].setState(2)
				}
				used = 0
				break
			}
			if used == len(stack) {
				return fmt.Errorf("%w: dependency traversal", ErrMalformed)
			}
			r.setState(1)
			stack[used] = current
			used++
			p.stats.DPNodeVisits++
			if r.reason != reasonOK || r.parent == noParent {
				break
			}
			p.stats.DPEdgeVisits++
			current = r.parent
		}
		for used > 0 {
			used--
			n := stack[used]
			r := &p.records[n]
			if r.reason == reasonOK {
				if r.parent == noParent {
					r.root = n
					r.depth = 1
					r.work = r.raw
				} else {
					base := &p.records[r.parent]
					if base.state() != 2 {
						return fmt.Errorf("%w: unresolved dependency", ErrMalformed)
					}
					r.root = base.root
					switch {
					case base.reason != reasonOK:
						r.reason = base.reason
					case base.finalKind() != r.finalKind():
						r.reason = reasonKind
					case base.depth >= wire.FrameLimit:
						r.reason = reasonDepth
					default:
						r.depth = base.depth + 1
						work := uint64(base.work) + uint64(r.raw) + uint64(r.size)
						if work > wire.WorkLimit {
							r.reason = reasonWork
						} else {
							r.work = uint32(work)
						}
					}
				}
			}
			r.setState(2)
		}
	}
	for i := range p.records {
		if isLimit(p.records[i].reason) {
			p.stats.LimitNodes++
		} else if p.records[i].reason != reasonOK {
			p.stats.MalformedNodes++
		}
	}
	return nil
}
