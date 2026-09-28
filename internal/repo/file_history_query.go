package repo

import (
	"container/heap"
	"context"
	"fmt"

	pb "gyit/internal/gen/gyit/storage/v1"
)

type fileHistoryCandidate struct {
	cursor *pb.FileHistoryCursor
	order  int
}
type fileHistoryQueue []fileHistoryCandidate

func (q fileHistoryQueue) Len() int { return len(q) }
func (q fileHistoryQueue) Less(i, j int) bool {
	if q[i].cursor.Time == q[j].cursor.Time {
		return q[i].order < q[j].order
	}
	return q[i].cursor.Time > q[j].cursor.Time
}
func (q fileHistoryQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *fileHistoryQueue) Push(v any)   { *q = append(*q, v.(fileHistoryCandidate)) }
func (q *fileHistoryQueue) Pop() any {
	a := *q
	v := a[len(a)-1]
	a[len(a)-1] = fileHistoryCandidate{}
	*q = a[:len(a)-1]
	return v
}

// The index elides unchanged ancestry. At a fork, a timestamp gate proves when
// an entire continuation can outrank the other queue entries. Otherwise step
// through ingested commit/path metadata to preserve Git's exact ordering,
// including equal timestamps and skewed commit clocks. Never fetch native packs
// or create an index as a side effect of a query.
func (s *Snapshot) indexedFileLog(ctx context.Context, path string, opt LogOptions, emit func(LogEntry) error) error {
	p := s.progressive
	root, state, err := p.fileHistoryState(ctx, s.SHA, path)
	if err != nil {
		return err
	}
	queue := &fileHistoryQueue{}
	seen := map[string]bool{}
	processed := map[string]bool{}
	order := 0
	add := func(c *pb.FileHistoryCursor) error {
		if seen[c.Sha] || processed[c.Sha] {
			return nil
		}
		if len(seen) > maxLogVisited || len(*queue) >= maxLogFrontier {
			return fmt.Errorf("file history frontier exceeds traversal budget")
		}
		seen[c.Sha] = true
		heap.Push(queue, fileHistoryCandidate{cursor: c, order: order})
		order++
		return nil
	}
	if err = add(&pb.FileHistoryCursor{Sha: s.SHA, Time: root.Commit.Metadata.CommitTime, State: state}); err != nil {
		return err
	}
	for written := 0; queue.Len() > 0 && written < opt.Count; {
		if err := ctx.Err(); err != nil {
			return err
		}
		c := heap.Pop(queue).(fileHistoryCandidate).cursor
		if processed[c.Sha] {
			continue
		}
		processed[c.Sha] = true
		if c.State == nil {
			return fmt.Errorf("invalid file history cursor")
		}
		link := c.State.Normal
		if opt.FirstParent {
			link = c.State.FirstParent
		}
		if link == nil {
			continue
		}
		if link.Commit == c.Sha || queue.Len() == 0 || link.Gate > (*queue)[0].cursor.Time {
			if link.Commit != c.Sha && processed[link.Commit] {
				continue
			}
			processed[link.Commit] = true
			var event pb.FileHistoryEvent
			if err := p.readHistoryPage(ctx, link.Event, &event); err != nil {
				return err
			}
			var commit pb.FileHistoryCommit
			if err := p.readHistoryPage(ctx, event.Commit, &commit); err != nil {
				return err
			}
			if commit.Sha != link.Commit || commit.Metadata == nil || len(commit.Parents) > maxLogParents {
				return fmt.Errorf("invalid file history event")
			}
			m := commit.Metadata
			entry := LogEntry{SHA: commit.Sha, Parents: commit.Parents, Author: m.Author, Message: m.Message, AuthorTime: m.AuthorTime, AuthorOffset: m.AuthorOffsetMinutes, MessageTruncated: m.MessageTruncated, AuthorTruncated: m.AuthorTruncated}
			if !opt.FullCommitIDs {
				entry.ShortSHA, err = abbreviate(ctx, s.idx, &index{store: s.idx.store, cache: s.idx.cache}, entry.SHA)
				if err != nil {
					return err
				}
			}
			if err = s.abbreviateLogParents(ctx, &entry); err != nil {
				return err
			}
			if err = emit(entry); err != nil {
				return err
			}
			written++
			if written == opt.Count {
				break
			}
			next := event.Parents
			if opt.FirstParent && len(next) > 1 {
				next = next[:1]
			}
			for _, parent := range next {
				if err = add(parent); err != nil {
					return err
				}
			}
		} else {
			r, _, err := p.fileHistoryState(ctx, c.Sha, path)
			if err != nil {
				return err
			}
			matched := false
			for i, parent := range r.Commit.Parents {
				if opt.FirstParent && i > 0 {
					break
				}
				pr, ps, err := p.fileHistoryState(ctx, parent, path)
				if err != nil {
					return err
				}
				if c.State.Oid == ps.Oid && c.State.Mode == ps.Mode {
					if err = add(&pb.FileHistoryCursor{Sha: parent, Time: pr.Commit.Metadata.CommitTime, State: ps}); err != nil {
						return err
					}
					matched = true
					break
				}
			}
			if !matched {
				return fmt.Errorf("invalid elided file history continuation")
			}
		}
	}
	return nil
}
