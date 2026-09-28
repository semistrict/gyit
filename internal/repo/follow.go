package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"

	"gyit/internal/store"
)

const renameBuckets = 107927
const renameReadBudget = 256 << 20

type renameSearch struct{ remaining int64 }

// Follow searches the parent tree only when the selected name first appears.
// Exact matches need metadata only, including for symlinks and large blobs.
// Edited regular files use Git's content-span similarity, with bounded buffers.
func (s *Snapshot) followSource(ctx context.Context, sha, parent, name string, budget *renameSearch) (string, error) {
	currentTree, err := s.commitTree(ctx, sha)
	if err != nil {
		return "", err
	}
	oldTree, err := s.commitTree(ctx, parent)
	if err != nil {
		return "", err
	}
	current := &Snapshot{progressive: s.progressive, idx: s.idx, Tree: currentTree}
	old := &Snapshot{progressive: s.progressive, idx: s.idx, Tree: oldTree}
	target, err := current.Resolve(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return name, nil
	}
	if err != nil {
		return "", err
	}
	if target.Mode == 0040000 {
		return "", invalidRevision("--follow requires a file, not a directory")
	}
	previous, err := old.Resolve(ctx, name)
	if err == nil && previous.Mode != 0040000 {
		return name, nil
	}
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return "", err
	}
	best := ""
	bestRank := -1
	identical := 0
	err = s.walkChanges(ctx, "", oldTree, nil, func(candidate string, _, e Entry) error {
		if e.OID != target.OID || ((e.Mode&0170000 != 0100000 || target.Mode&0170000 != 0100000) && e.Mode != target.Mode) {
			return nil
		}
		rank := 0
		if path.Base(candidate) == path.Base(name) {
			rank++
		}
		_, err := current.Resolve(ctx, candidate)
		if errors.Is(err, store.ErrNotFound) {
			rank++
		} else if err != nil {
			return err
		}
		// Exact candidates use Git's sorted path order to break ties.
		if rank > bestRank || (rank == bestRank && candidate < best) {
			best, bestRank = candidate, rank
		}
		identical++
		if rank == 2 || identical == 100 {
			return stopTreeWalk
		}
		return nil
	})
	if err != nil && !errors.Is(err, stopTreeWalk) {
		return "", err
	}
	if best != "" {
		return best, nil
	}
	if target.Size == 0 || target.Mode&0170000 != 0100000 {
		return name, nil
	}
	targetCounts := make([]uint64, renameBuckets)
	if err := s.renameSpans(ctx, target, targetCounts, budget); err != nil {
		return "", err
	}
	counts := make([]uint64, renameBuckets)
	bestScore, bestName := uint64(29999), false
	err = s.walkChanges(ctx, "", oldTree, nil, func(candidate string, _, e Entry) error {
		if e.Mode&0170000 != 0100000 || e.Size == 0 {
			return nil
		}
		size := max(e.Size, target.Size)
		if min(e.Size, target.Size)*2 < size {
			return nil
		}
		clear(counts)
		if err := s.renameSpans(ctx, e, counts, budget); err != nil {
			return err
		}
		copied := uint64(0)
		for i, n := range counts {
			copied += min(n, targetCounts[i])
		}
		score := copied * 60000 / uint64(size)
		sameName := path.Base(candidate) == path.Base(name)
		if score > bestScore || (score == bestScore && sameName && !bestName) {
			best, bestScore, bestName = candidate, score, sameName
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if best != "" {
		return best, nil
	}
	return name, nil
}

// Span histograms measure shared bytes in newline/64-byte pieces. uint32
// arithmetic and the modulus preserve Git's hash collisions and score rounding.
func (s *Snapshot) renameSpans(ctx context.Context, e Entry, counts []uint64, budget *renameSearch) error {
	if e.Size > budget.remaining {
		return fmt.Errorf("rename comparison exceeds the 256 MiB content-read budget")
	}
	budget.remaining -= e.Size
	buffer := make([]byte, 64<<10)
	var a, b uint32
	length := uint64(0)
	emit := func(c byte) {
		old := a
		a = (a << 7) ^ (b >> 25)
		b = (b << 7) ^ (old >> 25)
		a += uint32(c)
		length++
		if length == 64 || c == '\n' {
			counts[(a+b*0x61)%renameBuckets] += length
			a, b, length = 0, 0, 0
		}
	}
	text, pendingCR := true, false
	for off := int64(0); off < e.Size; {
		if err := ctx.Err(); err != nil {
			return err
		}
		want := min(int64(len(buffer)), e.Size-off)
		n, err := s.ReadAt(ctx, e.OID, buffer[:want], off)
		if err != nil && err != io.EOF {
			return err
		}
		if int64(n) != want {
			return io.ErrUnexpectedEOF
		}
		if off == 0 {
			text = !bytes.ContainsRune(buffer[:min(n, 8000)], 0)
		}
		for _, c := range buffer[:n] {
			if pendingCR {
				if c != '\n' {
					emit('\r')
				}
				pendingCR = false
			}
			if text && c == '\r' {
				pendingCR = true
				continue
			}
			emit(c)
		}
		off += int64(n)
	}
	if pendingCR {
		emit('\r')
	}
	if length > 0 {
		counts[(a+b*0x61)%renameBuckets] += length
	}
	return nil
}
