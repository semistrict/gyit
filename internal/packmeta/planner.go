package planner

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"unsafe"

	wire "gyit/internal/archive/wire"
	"gyit/internal/gitdelta"
	"golang.org/x/sys/unix"
)

const MaxObjects = 12_000_000
const noParent = math.MaxUint32

var (
	ErrUnsupported = errors.New("source metadata fast path unsupported")
	ErrLimit       = errors.Join(gitdelta.ErrLimit, wire.ErrLimit)
	ErrMalformed   = errors.New("malformed archive source metadata")
	ErrClosed      = errors.New("archive source planner closed")
)

type Statistics struct {
	PrefixInputBytes, PrefixOutputBytes, PrefixInputLimitHits                  uint64
	MetadataObjects, MetadataBlobs, MetadataBlobBytes                          uint64
	MetadataCommits, MetadataTrees, MetadataTags                               uint64
	TypeDPNodeVisits, TypeDPEdgeVisits                                         uint64
	RootCommits, RootTrees, RootBlobs, RootTags                                uint64
	SourceObjects, InventoryLookups, SourceHeaders, SourceHeaderBytes          uint64
	Roots, OFSParents, REFParents, ParentLookups                               uint64
	OIDProbeSteps, OffsetProbeSteps, DPNodeVisits, DPEdgeVisits                uint64
	LimitNodes, MalformedNodes                                                 uint64
	SourceMappedBytes, ScratchMappedBytes, BuildPeakScratchMappedBytes         uint64
	RecordBytes, HashSlotBytes, BuildStackBytes                                uint64
	RecipeCalls, RecipeAccepted, RecipeLimitFallbacks, RecipeMalformedFailures uint64
	BlobAccepted, TreeAccepted, BlobFallbacks, TreeFallbacks                   uint64
	RecipeFrames, RecipeParentVisits, RecipeBytes, RecipePackedBytes           uint64
	RecipeFetchedBytes, RecipeWork                                             uint64
	// PrefixInflations counts bounded metadata-only C calls. Full inflation,
	// reconstruction and source-payload copying remain absent; those fields
	// exclude ordinary caller fallback and archive upload.
	PrefixInflations, FullInflations, FullInflatedBytes     uint64
	Reconstructions, ReconstructedBytes, PayloadCopiedBytes uint64
}

// One physical record retains complete metadata independently of read admission.
type record struct {
	offset, raw, size         uint64
	parent, root, work        uint32
	body, kind, reason, depth uint8
}

const (
	reasonOK uint8 = iota
	reasonSize
	reasonProgram
	reasonPacked
	reasonDepth
	reasonWork
	reasonMetadata
	reasonHeader
	reasonParent
	reasonKind
	reasonCycle
)

func isLimit(reason uint8) bool   { return reason >= reasonSize && reason <= reasonWork }
func (r *record) finalKind() byte { return r.kind & 7 }
func (r *record) state() byte     { return r.kind >> 3 }
func (r *record) setState(v byte) { r.kind = r.kind&7 | v<<3 }

type recipeCounters struct {
	calls, accepted, limits, malformed           atomic.Uint64
	blobs, trees, blobFallbacks, treeFallbacks   atomic.Uint64
	frames, visits, bytes, packed, fetched, work atomic.Uint64
}

type Planner struct {
	mu             sync.RWMutex
	closed         bool
	ctx            context.Context
	idx, rev, pack []byte
	scratchDir     string
	mappings       [][]byte
	records        []record
	oids, offsets  []uint32
	seed           maphash.Seed
	stats          Statistics
	counts         recipeCounters
}

func (p *Planner) Has(oid string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed || p.ctx.Err() != nil {
		return false
	}
	var id [20]byte
	if len(oid) != 40 {
		return false
	}
	if _, err := hex.Decode(id[:], []byte(oid)); err != nil {
		return false
	}
	_, ok, _ := p.ordinal(id[:])
	return ok
}

// Recipe emits scalar extents only. Unsupported targets, missing pack members
// and admission failures return ErrLimit for the caller's unchanged fallback.
// A malformed selected dependency family returns ErrMalformed. Neither case
// silently drops a requested target. Returned recipes own no mapped byte slices.
func (p *Planner) Recipe(oid string, archiveID [16]byte) (out wire.Recipe, retErr error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	p.counts.calls.Add(1)
	var kind byte
	defer func() {
		if errors.Is(retErr, ErrLimit) {
			p.counts.limits.Add(1)
			if kind == 2 {
				p.counts.treeFallbacks.Add(1)
			}
			if kind == 3 {
				p.counts.blobFallbacks.Add(1)
			}
		} else if retErr != nil {
			p.counts.malformed.Add(1)
		}
	}()
	if p.closed {
		return out, ErrClosed
	}
	if err := p.ctx.Err(); err != nil {
		return out, err
	}
	var id [20]byte
	if len(oid) != 40 {
		return out, fmt.Errorf("%w: target OID", ErrMalformed)
	}
	if _, err := hex.Decode(id[:], []byte(oid)); err != nil {
		return out, fmt.Errorf("%w: target OID", ErrMalformed)
	}
	ordinal, found, _ := p.ordinal(id[:])
	if !found {
		return out, ErrLimit
	}
	target := &p.records[ordinal]
	kind = target.finalKind()
	if target.reason != reasonOK {
		if isLimit(target.reason) {
			return out, ErrLimit
		}
		return out, fmt.Errorf("%w: dependency reason %d", ErrMalformed, target.reason)
	}
	if kind != 2 && kind != 3 {
		return out, ErrLimit
	}
	if kind == 2 && target.size > 64<<10 {
		return out, ErrLimit
	}
	// Admission uses the existing absolute read-work and byte limits.
	// The blob four-range limit remains below; no wire-format change is needed.
	var chain [wire.FrameLimit]uint32
	used := 0
	for n := ordinal; ; n = p.records[n].parent {
		if used == len(chain) {
			return out, fmt.Errorf("%w: inconsistent dependency depth", ErrMalformed)
		}
		chain[used] = n
		used++
		if p.records[n].parent == noParent {
			break
		}
	}
	p.counts.visits.Add(uint64(used - 1))
	out = wire.Recipe{ArchiveID: archiveID, PackSize: uint64(len(p.pack)), TargetOID: id, Frames: make([]wire.Frame, used)}
	var packed uint64
	for i := range out.Frames {
		n := chain[used-1-i]
		r := &p.records[n]
		f := wire.Frame{HeaderOffset: r.offset, Offset: r.offset + uint64(r.body), RawSize: uint32(r.raw), Size: uint32(r.size)}
		f.Length = uint32(p.end(n) - f.Offset)
		copy(f.OID[:], p.oid(n))
		out.Frames[i] = f
		packed += uint64(f.Length)
	}
	p.counts.frames.Add(uint64(used))
	n, err := wire.EncodedSize(out)
	if err != nil {
		if errors.Is(err, wire.ErrLimit) {
			return wire.Recipe{}, ErrLimit
		}
		return wire.Recipe{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	ranges, err := wire.Plan(out, -1)
	if err != nil {
		if errors.Is(err, wire.ErrLimit) {
			return wire.Recipe{}, ErrLimit
		}
		return wire.Recipe{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	// The expanded wire budget belongs only to tree reads. Blob admission
	// remains at four ranges, including the ordinary source-planner fallback.
	if kind == 3 && len(ranges) > 4 {
		return wire.Recipe{}, ErrLimit
	}
	var fetched uint64
	for _, r := range ranges {
		fetched += uint64(r.Length)
	}
	p.counts.accepted.Add(1)
	if kind == 2 {
		p.counts.trees.Add(1)
	} else {
		p.counts.blobs.Add(1)
	}
	p.counts.bytes.Add(uint64(n))
	p.counts.packed.Add(packed)
	p.counts.fetched.Add(fetched)
	p.counts.work.Add(uint64(target.work))
	return out, nil
}

func (p *Planner) Stats() Statistics {
	s := p.stats
	s.RecipeCalls = p.counts.calls.Load()
	s.RecipeAccepted = p.counts.accepted.Load()
	s.RecipeLimitFallbacks = p.counts.limits.Load()
	s.RecipeMalformedFailures = p.counts.malformed.Load()
	s.BlobAccepted = p.counts.blobs.Load()
	s.TreeAccepted = p.counts.trees.Load()
	s.BlobFallbacks = p.counts.blobFallbacks.Load()
	s.TreeFallbacks = p.counts.treeFallbacks.Load()
	s.RecipeFrames = p.counts.frames.Load()
	s.RecipeParentVisits = p.counts.visits.Load()
	s.RecipeBytes = p.counts.bytes.Load()
	s.RecipePackedBytes = p.counts.packed.Load()
	s.RecipeFetchedBytes = p.counts.fetched.Load()
	s.RecipeWork = p.counts.work.Load()
	return s
}

// Close waits for active Has/Recipe calls, releases all source/scratch mappings
// and removes the owned scratch directory. It is idempotent; Stats remains valid.
func (p *Planner) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	var err error
	for _, b := range p.mappings {
		if b != nil {
			err = errors.Join(err, unix.Munmap(b))
		}
	}
	for _, b := range [][]byte{p.idx, p.rev, p.pack} {
		if b != nil {
			err = errors.Join(err, unix.Munmap(b))
		}
	}
	p.mappings = nil
	p.records = nil
	p.oids = nil
	p.offsets = nil
	p.idx = nil
	p.rev = nil
	p.pack = nil
	if p.scratchDir != "" {
		err = errors.Join(err, os.RemoveAll(p.scratchDir))
	}
	return err
}

func mapSource(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Size() < 1 || uint64(st.Size()) > uint64(math.MaxInt) {
		return nil, fmt.Errorf("%w: source file size", ErrMalformed)
	}
	return unix.Mmap(int(f.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_PRIVATE)
}

func (p *Planner) mapScratch(name string, n uint64) ([]byte, error) {
	if n == 0 {
		return nil, nil
	}
	if n >= 2<<30 || n > uint64(math.MaxInt) {
		return nil, ErrLimit
	}
	f, err := os.OpenFile(filepath.Join(p.scratchDir, name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err = f.Truncate(int64(n)); err != nil {
		return nil, err
	}
	b, err := unix.Mmap(int(f.Fd()), 0, int(n), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	p.mappings = append(p.mappings, b)
	p.stats.ScratchMappedBytes += n
	p.stats.BuildPeakScratchMappedBytes = max(p.stats.BuildPeakScratchMappedBytes, p.stats.ScratchMappedBytes)
	return b, nil
}

func (p *Planner) allocate(count uint32) error {
	n := uint64(1)
	for n*3 < uint64(count)*4 {
		n <<= 1
	}
	p.stats.RecordBytes = uint64(count) * uint64(unsafe.Sizeof(record{}))
	p.stats.HashSlotBytes = n * 8
	b, err := p.mapScratch("records", p.stats.RecordBytes)
	if err != nil {
		return err
	}
	if len(b) > 0 {
		p.records = unsafe.Slice((*record)(unsafe.Pointer(&b[0])), int(count))
	}
	b, err = p.mapScratch("oid-slots", n*4)
	if err != nil {
		return err
	}
	p.oids = unsafe.Slice((*uint32)(unsafe.Pointer(&b[0])), int(n))
	b, err = p.mapScratch("offset-slots", n*4)
	if err != nil {
		return err
	}
	p.offsets = unsafe.Slice((*uint32)(unsafe.Pointer(&b[0])), int(n))
	return nil
}

func (p *Planner) oid(physical uint32) []byte {
	ordinal := binary.BigEndian.Uint32(p.rev[12+4*uint64(physical) : 16+4*uint64(physical)])
	pos := 1032 + 20*uint64(ordinal)
	return p.idx[pos : pos+20]
}
func mix(v uint64) uint64 {
	v ^= v >> 30
	v *= 0xbf58476d1ce4e5b9
	v ^= v >> 27
	v *= 0x94d049bb133111eb
	return v ^ (v >> 31)
}
func (p *Planner) addOrdinal(n uint32) {
	mask := uint64(len(p.oids) - 1)
	j := maphash.Bytes(p.seed, p.oid(n)) & mask
	for p.oids[j] != 0 {
		j = (j + 1) & mask
	}
	p.oids[j] = n + 1
	j = mix(p.records[n].offset) & mask
	for p.offsets[j] != 0 {
		j = (j + 1) & mask
	}
	p.offsets[j] = n + 1
}
func (p *Planner) ordinal(id []byte) (uint32, bool, uint64) {
	mask := uint64(len(p.oids) - 1)
	j := maphash.Bytes(p.seed, id) & mask
	for probes := uint64(1); ; probes++ {
		v := p.oids[j]
		if v == 0 {
			return 0, false, probes
		}
		if bytes.Equal(id, p.oid(v-1)) {
			return v - 1, true, probes
		}
		j = (j + 1) & mask
	}
}
func (p *Planner) atOffset(off uint64) (uint32, bool, uint64) {
	mask := uint64(len(p.offsets) - 1)
	j := mix(off) & mask
	for probes := uint64(1); ; probes++ {
		v := p.offsets[j]
		if v == 0 {
			return 0, false, probes
		}
		if p.records[v-1].offset == off {
			return v - 1, true, probes
		}
		j = (j + 1) & mask
	}
}
func (p *Planner) end(n uint32) uint64 {
	if int(n)+1 < len(p.records) {
		return p.records[n+1].offset
	}
	return uint64(len(p.pack) - 20)
}

func (p *Planner) validateSource() (uint32, error) {
	bad := func(s string) (uint32, error) { return 0, fmt.Errorf("%w: %s", ErrMalformed, s) }
	if len(p.idx) < 1072 || !bytes.Equal(p.idx[:8], []byte{255, 't', 'O', 'c', 0, 0, 0, 2}) || len(p.pack) < 32 || string(p.pack[:4]) != "PACK" {
		return bad("pack/index format")
	}
	version := binary.BigEndian.Uint32(p.pack[4:8])
	if version != 2 && version != 3 {
		return bad("pack version")
	}
	n := binary.BigEndian.Uint32(p.idx[1028:1032])
	if n > MaxObjects {
		return 0, ErrLimit
	}
	base := uint64(1072) + 28*uint64(n)
	if binary.BigEndian.Uint32(p.pack[8:12]) != n || uint64(len(p.idx)) < base || (uint64(len(p.idx))-base)%8 != 0 || !bytes.Equal(p.pack[len(p.pack)-20:], p.idx[len(p.idx)-40:len(p.idx)-20]) {
		return bad("index count/bounds/pack identity")
	}
	if uint64(len(p.rev)) != 52+4*uint64(n) || string(p.rev[:4]) != "RIDX" || binary.BigEndian.Uint32(p.rev[4:8]) != 1 || binary.BigEndian.Uint32(p.rev[8:12]) != 1 || !bytes.Equal(p.rev[len(p.rev)-40:len(p.rev)-20], p.pack[len(p.pack)-20:]) {
		return bad("reverse index format/identity")
	}
	var fanout [256]uint32
	for i := uint32(0); i < n; i++ {
		if i&4095 == 0 {
			if err := p.ctx.Err(); err != nil {
				return 0, err
			}
		}
		pos := 1032 + 20*uint64(i)
		id := p.idx[pos : pos+20]
		if i > 0 && bytes.Compare(p.idx[pos-20:pos], id) >= 0 {
			return bad("unsorted/duplicate index OID")
		}
		fanout[id[0]]++
	}
	var cumulative uint32
	for i, v := range fanout {
		cumulative += v
		if binary.BigEndian.Uint32(p.idx[8+4*i:12+4*i]) != cumulative {
			return bad("index fanout")
		}
	}
	return n, nil
}
func (p *Planner) indexOffset(i, count uint32) (uint64, error) {
	pos := 1032 + 24*uint64(count) + 4*uint64(i)
	off := uint64(binary.BigEndian.Uint32(p.idx[pos : pos+4]))
	if off&0x80000000 != 0 {
		pos = 1032 + 28*uint64(count) + 8*(off&0x7fffffff)
		if pos+8 > uint64(len(p.idx)-40) {
			return 0, fmt.Errorf("%w: large offset bounds", ErrMalformed)
		}
		off = binary.BigEndian.Uint64(p.idx[pos : pos+8])
	}
	if off < 12 || off >= uint64(len(p.pack)-20) {
		return 0, fmt.Errorf("%w: source offset bounds", ErrMalformed)
	}
	return off, nil
}
