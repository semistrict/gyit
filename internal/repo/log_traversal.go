package repo

import (
	"container/heap"
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
)

const logTraversalMemoryBytes = 1 << 20
const logTraversalMemoryCandidates = 4096

// logTraversal keeps small walks in memory and spills BOTH membership and the
// priority queue when either window fills. Spilling changes neither comparison
// order nor duplicate suppression. Its data belongs to this query only.
type logTraversal struct {
	ctx                   context.Context
	temp                  string
	seen                  map[string]bool
	queue                 logQueue
	seenBytes, queueBytes int
	sequence              int
	disk                  logTraversalDisk
}

type logTraversalDisk interface {
	has(string) (bool, error)
	remember(string) error
	push(logCandidate) error
	pop() (logCandidate, bool, error)
	clearSeen() error
	close() error
}

func newLogTraversal(ctx context.Context, temp string) *logTraversal {
	return &logTraversal{ctx: ctx, temp: temp, seen: make(map[string]bool)}
}
func logSeenKey(sha, path string) string { return sha + "\x00" + path }
func logCandidateBytes(c logCandidate) int {
	return len(c.sha) + len(c.path) + 96 + proto.Size(c.location) + 192
}
func (w *logTraversal) has(sha, path string) (bool, error) {
	if err := w.ctx.Err(); err != nil {
		return false, err
	}
	key := logSeenKey(sha, path)
	if w.disk != nil {
		return w.disk.has(key)
	}
	return w.seen[key], nil
}
func (w *logTraversal) push(c logCandidate) error {
	found, err := w.has(c.sha, c.path)
	if err != nil || found {
		return err
	}
	key := logSeenKey(c.sha, c.path)
	cost := len(key) + 64 + logCandidateBytes(c)
	if w.disk == nil && (w.seenBytes+w.queueBytes+cost > logTraversalMemoryBytes || len(w.queue) >= logTraversalMemoryCandidates) {
		if err := w.spill(); err != nil {
			return err
		}
	}
	if w.sequence == int(^uint(0)>>1) {
		return fmt.Errorf("history traversal sequence exhausted")
	}
	c.order = w.sequence
	w.sequence++
	if w.disk != nil {
		if err := w.disk.remember(key); err != nil {
			return err
		}
		return w.disk.push(c)
	}
	w.seen[key] = true
	w.seenBytes += len(key) + 64
	w.queueBytes += logCandidateBytes(c)
	heap.Push(&w.queue, c)
	return nil
}
func (w *logTraversal) pop() (logCandidate, bool, error) {
	if err := w.ctx.Err(); err != nil {
		return logCandidate{}, false, err
	}
	if w.disk != nil {
		return w.disk.pop()
	}
	if len(w.queue) == 0 {
		return logCandidate{}, false, nil
	}
	c := heap.Pop(&w.queue).(logCandidate)
	w.queueBytes -= logCandidateBytes(c)
	return c, true, nil
}
func (w *logTraversal) clearSeen() error {
	if w.disk != nil {
		return w.disk.clearSeen()
	}
	clear(w.seen)
	w.seenBytes = 0
	return nil
}
func (w *logTraversal) close() error {
	if w.disk != nil {
		return w.disk.close()
	}
	return nil
}
func (w *logTraversal) spill() (err error) {
	disk, err := newLogTraversalDisk(w.temp)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = disk.close()
		}
	}()
	for key := range w.seen {
		if err = w.ctx.Err(); err != nil {
			return err
		}
		if err = disk.remember(key); err != nil {
			return err
		}
	}
	for _, c := range w.queue {
		if err = w.ctx.Err(); err != nil {
			return err
		}
		if err = disk.push(c); err != nil {
			return err
		}
	}
	w.disk = disk
	w.seen, w.queue = nil, nil
	w.seenBytes, w.queueBytes = 0, 0
	return nil
}
