package repo

import "fmt"

// Parallelize only overloaded lanes: extra compressors and copied inputs cost
// memory, and a balanced import already has enough independent work. Weights
// are temporary scheduling estimates; they never affect encoding or routing.
func parallelBlobLanes(weights []uint64, workers, depth, candidates int) []bool {
	if workers < 2 || len(weights) != workers || depth != 1 || candidates != 4 {
		return nil
	}
	var total uint64
	for _, n := range weights {
		if n > ^uint64(0)-total {
			return nil
		}
		total += n
	}
	if total == 0 {
		return nil
	}
	enabled := make([]bool, workers)
	// Float conversion avoids multiplication overflow. This is a scheduling
	// heuristic, so rounding at a one-byte threshold cannot affect stored data.
	threshold := 1.5 * float64(total) / float64(workers)
	for i, n := range weights {
		enabled[i] = float64(n) > threshold
	}
	return enabled
}

func (s *blobSizes) addLaneBytes(lane int, n uint64) bool {
	if n > ^uint64(0)-s.laneBytes[lane] {
		// An unrepresentable estimate disables this optimization, not import.
		s.laneBytes = nil
		return false
	}
	s.laneBytes[lane] += n
	return true
}

func (s *blobSizes) noteBlobWork(hint string, size uint64, firstLane int) {
	if len(s.laneBytes) == 0 {
		return
	}
	if hint == "" {
		// These chunks rotate between lanes after sorting. Their exact rotation
		// is not known in this pass; distribute their estimated work evenly.
		n := uint64(len(s.laneBytes))
		for lane := range s.laneBytes {
			weight := size / n
			if uint64(lane) < size%n {
				weight++
			}
			if !s.addLaneBytes(lane, weight) {
				return
			}
		}
		return
	}
	// Avoid an unbounded new metadata loop for extraordinarily large blobs.
	// They remain supported by the original streaming importer.
	if size/ChunkSize > 1<<16 {
		s.laneBytes = nil
		return
	}
	for part, left := uint64(0), size; left > 0; part++ {
		lane := firstLane
		if part > 0 {
			lane = chunkWorker(fmt.Sprintf("%s/%016x", hint, part), len(s.laneBytes))
		}
		n := min(left, uint64(ChunkSize))
		if !s.addLaneBytes(lane, n) {
			return
		}
		left -= n
	}
}
