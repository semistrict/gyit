package packrecipe

import (
	"encoding/hex"
	"errors"
	"fmt"

	probev1 "gat/internal/gen/gat/probe/v1"
	"gat/internal/gitdelta"
	"google.golang.org/protobuf/proto"
)

const (
	TreeTargetLimit  = 64 << 10
	TreePayloadLimit = 64 << 10
	TreeWorkLimit    = 4 << 20
)

// ConvertTree plans from the original pack's headers, reverse index and bounded
// size prefixes. It never reads a complete tree/program body or parses entries.
// Construct the reader with OpenDeferred. Unsupported admission uses
// ErrLimit so the caller can retain the existing eager directory representation.
// Output slices retain the ordinary Reader ownership lifetime (until Close).
func (r *Reader) ConvertTree(oid string, dst []byte) (out Output, retErr error) {
	r.conversion.TreeCalls++
	defer func() {
		if errors.Is(retErr, gitdelta.ErrLimit) {
			r.conversion.TreeLimitFallbacks++
		}
	}()
	if err := r.ctx.Err(); err != nil {
		return Output{}, err
	}
	if r.deferred == nil {
		return Output{}, fmt.Errorf("native tree planning requires deferred source")
	}
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
	var work, compressed int64
	for {
		if err := r.ctx.Err(); err != nil {
			return Output{}, err
		}
		if used == len(chain) {
			return Output{}, gitdelta.ErrLimit
		}
		for i := 0; i < used; i++ {
			if chain[i].header.offset == offset {
				return Output{}, fmt.Errorf("native tree dependency cycle")
			}
		}
		f, err := r.planFrame(offset)
		if err != nil {
			return Output{}, err
		}
		if used == 0 && f.objectSize > TreeTargetLimit {
			return Output{}, gitdelta.ErrLimit
		}
		if used > 0 && chain[used-1].baseSize != f.objectSize {
			return Output{}, fmt.Errorf("native tree ancestor size mismatch")
		}
		// One decoded original frame (root or program), plus one reconstructed
		// output for every delta. Root/final/intermediate objects count once.
		work += int64(f.header.size)
		if f.base != 0 {
			work += int64(f.objectSize)
		}
		compressed += int64(f.end - f.header.body)
		if work > TreeWorkLimit || compressed > TreePayloadLimit {
			return Output{}, gitdelta.ErrLimit
		}
		chain[used] = f
		used++
		if f.base == 0 {
			break
		}
		offset = f.base
	}
	target, root := chain[0], chain[used-1]
	if root.header.kind != 2 {
		return Output{}, gitdelta.ErrLimit
	}
	rootPacked := r.p.pack[root.header.body:root.end]
	out = Output{Hash: "git-tree-sha1:" + hex.EncodeToString(id), Size: target.objectSize}
	var copied int64
	if used == 1 {
		out.Data, out.Full = rootPacked, true
	} else {
		bundle := &probev1.NativeProgramBundle{Frames: make([]*probev1.NativeFrame, 0, used-1)}
		for i := used - 2; i >= 0; i-- {
			f := chain[i]
			packed := r.p.pack[f.header.body:f.end]
			copied += int64(len(packed))
			bundle.Frames = append(bundle.Frames, &probev1.NativeFrame{RawSize: uint32(f.header.size), Zlib: packed})
		}
		if len(rootPacked)+proto.Size(bundle) > TreePayloadLimit {
			return Output{}, gitdelta.ErrLimit
		}
		out.Data, err = proto.MarshalOptions{Deterministic: true}.MarshalAppend(dst[:0], bundle)
		if err != nil {
			return Output{}, err
		}
		out.Base = &Base{Packed: rootPacked, Hash: "git-tree-sha1:" + hex.EncodeToString(root.oid[:])}
	}
	payload := len(out.Data)
	if out.Base != nil {
		payload += len(out.Base.Packed)
	}
	r.conversion.TreeObjects++
	r.conversion.TreeRawBytes += int64(out.Size)
	r.conversion.TreeDecodedWork += work
	r.conversion.TreePayloadBytes += int64(payload)
	r.conversion.BundledFrameBytes += copied
	r.conversion.ReturnedRootBytes += int64(len(rootPacked))
	r.conversion.TreeFullInflations += r.p.work.FullInflations - before.FullInflations
	r.conversion.TreeFullInflatedBytes += r.p.work.FullInflatedBytes - before.FullInflatedBytes
	r.conversion.TreeReconstructions += r.p.work.Reconstructions - before.Reconstructions
	r.conversion.TreeReconstructedBytes += r.p.work.ReconstructedBytes - before.ReconstructedBytes
	r.conversion.TreeGetCalls += r.p.work.GetCalls - before.GetCalls
	return out, nil
}
