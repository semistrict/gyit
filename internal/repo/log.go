package repo

import (
	"container/heap"
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"gyit/internal/pathspec"
	"gyit/internal/store"
)

const DefaultLogCount = 20
const MaxLogCount = 1000
const maxLogFrontier = 4096
const maxLogParents = 512
const maxLogVisited = 100000

var ErrLogMetadata = errors.New("commit display metadata is missing; re-run import")

type LogEntry struct {
	SHA, ShortSHA                     string
	Parents                           []string
	ShortParents                      []string
	Author, Message                   []byte
	AuthorTime                        int64
	AuthorOffset                      int32
	MessageTruncated, AuthorTruncated bool
}

type logCandidate struct {
	sha   string
	time  int64
	order int
	path  string
}
type logQueue []logCandidate

func (q logQueue) Len() int { return len(q) }
func (q logQueue) Less(i, j int) bool {
	if q[i].time == q[j].time {
		return q[i].order < q[j].order
	}
	return q[i].time > q[j].time
}
func (q logQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *logQueue) Push(x any)   { *q = append(*q, x.(logCandidate)) }
func (q *logQueue) Pop() any {
	old := *q
	v := old[len(old)-1]
	old[len(old)-1] = logCandidate{}
	*q = old[:len(old)-1]
	return v
}

// Log visits only enough ancestors to satisfy the requested count. Merge
// histories use committer-date priority, suppressing duplicate ancestors.
// The queue, visited set, record sizes and count all have hard upper bounds.
func (s *Snapshot) Log(ctx context.Context, count int, firstParent bool, emit func(LogEntry) error) error {
	return s.LogPaths(ctx, count, firstParent, nil, emit)
}

// LogPaths applies Git's default dense history simplification to literal paths.
// An unchanged merge follows only its first matching parent; --first-parent
// compares and follows only parent zero. Payload bytes are never fetched.
func (s *Snapshot) LogPaths(ctx context.Context, count int, firstParent bool, paths []string, emit func(LogEntry) error) error {
	return s.LogWithOptions(ctx, LogOptions{Count: count, FirstParent: firstParent, Paths: paths}, emit)
}

type LogOptions struct {
	Count               int
	FirstParent, Follow bool
	Paths               []string
	Prefix              string
	// Attribute rules come from the mounted checkout, even for historical queries.
	AttributeSource *Snapshot
}

func (s *Snapshot) LogWithOptions(ctx context.Context, opt LogOptions, emit func(LogEntry) error) error {
	count, firstParent := opt.Count, opt.FirstParent
	matcher, err := pathspec.Compile(opt.Paths, opt.Prefix)
	if err != nil {
		return invalidRevision(err.Error())
	}
	paths := matcher.LiteralPaths()
	followPath := ""
	if opt.Follow {
		followPath, err = matcher.FollowPath()
		if err != nil {
			return invalidRevision(err.Error())
		}
		if e, err := s.Resolve(ctx, followPath); err == nil && e.Mode == 0040000 {
			return invalidRevision("--follow requires a file")
		}
	}
	renameBudget := &renameSearch{remaining: renameReadBudget}
	attrs := opt.AttributeSource
	if attrs == nil {
		attrs = s
	}

	if count < 0 || count > MaxLogCount {
		return invalidRevision("log count must be between 0 and 1000")
	}
	if count == 0 {
		return nil
	}
	queue := &logQueue{}
	cursor := &historyCursor{idx: s.history}
	seen := map[string]bool{}
	seenBytes := 0
	key := func(sha, name string) string {
		if opt.Follow {
			return sha + "\x00" + name
		}
		return sha
	}
	mark := func(sha, name string) error {
		k := key(sha, name)
		if seen[k] {
			return nil
		}
		if len(seen) >= maxLogVisited || seenBytes+len(k)+32 > 16<<20 {
			return fmt.Errorf("log visited-history budget exceeded")
		}
		seen[k] = true
		seenBytes += len(k) + 32
		return nil
	}
	add := func(sha, name string) error {
		if seen[key(sha, name)] {
			return nil
		}
		if len(*queue) >= maxLogFrontier || len(seen) >= maxLogVisited {
			return fmt.Errorf("log history frontier exceeds bounded traversal limit; use --first-parent")
		}
		var info commitInfo
		if err := s.idx.get(ctx, "c/"+sha, &info); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return ErrLogMetadata
			}
			return err
		}
		heap.Push(queue, logCandidate{sha: sha, time: info.CommitTime, order: len(seen), path: name})
		if err := mark(sha, name); err != nil {
			return err
		}
		return nil
	}
	if err := add(s.SHA, followPath); err != nil {
		return err
	}
	for written := 0; queue.Len() > 0 && written < count; {
		if err := ctx.Err(); err != nil {
			return err
		}
		candidate := heap.Pop(queue).(logCandidate)
		if opt.Follow {
			paths = []string{candidate.path}
		}
		// With no competing frontier, skipping unchanged first-parent steps cannot
		// reorder output, even when commit clocks run backwards. At a branch frontier
		// keep ordinary date-priority traversal instead.
		if len(paths) > 0 && queue.Len() == 0 && s.history != nil && s.history.root != (pageRef{}) {
			sha, err := s.advanceUnchangedLog(ctx, candidate.sha, paths, cursor)
			if err != nil {
				return err
			}
			if sha != candidate.sha {
				if seen[key(sha, candidate.path)] {
					continue
				}
				if err := mark(sha, candidate.path); err != nil {
					return err
				}
				candidate.sha = sha
			}
		}

		var p parents
		if err := s.idx.get(ctx, "p/"+candidate.sha, &p); err != nil {
			return err
		}
		if len(p.Parents) > maxLogParents {
			return fmt.Errorf("commit has too many parents for bounded log display")
		}
		next := p.Parents
		if firstParent && len(next) > 1 {
			next = next[:1]
		}
		show := true
		if len(matcher.Patterns) > 0 {
			var err error
			if paths != nil {
				show, next, err = s.simplifyLogPaths(ctx, candidate.sha, next, paths)
			} else {
				show, next, err = s.simplifyLogMatcher(ctx, candidate.sha, next, matcher, attrs)
			}
			if err != nil {
				return err
			}
		}
		// Git's --follow uses single-parent diffs: ordinary merge commits are
		// traversed but omitted unless --first-parent selects their first-parent diff.
		if opt.Follow && !firstParent && len(p.Parents) > 1 {
			show = false
		}
		if show {
			// Only commit/blob/tree collisions are relevant to these displayed IDs.
			emptyRefs := &index{store: s.idx.store, cache: s.idx.cache}
			short, err := abbreviate(ctx, s.idx, emptyRefs, candidate.sha)
			if err != nil {
				return err
			}
			var info commitInfo
			if err := s.idx.get(ctx, "c/"+candidate.sha, &info); err != nil {
				return err
			}
			entry := LogEntry{SHA: candidate.sha, ShortSHA: short, Parents: p.Parents, Author: info.Author, Message: info.Message, AuthorTime: info.AuthorTime, AuthorOffset: info.AuthorOffset, MessageTruncated: info.MessageTruncated, AuthorTruncated: info.AuthorTruncated}

			if err := s.abbreviateLogParents(ctx, &entry); err != nil {
				return err
			}
			if err := emit(entry); err != nil {
				return err
			}
			written++
			if written == count {
				break
			}
		}
		for _, parent := range next {
			name := candidate.path
			if opt.Follow {
				var err error
				name, err = s.followSource(ctx, candidate.sha, parent, name, renameBudget)
				if err != nil {
					return err
				}
			}
			if err := add(parent, name); err != nil {
				return err
			}
		}
	}
	return nil
}

// A missing path is an empty entry, which makes deletion and root commits work
// without special cases. Comparing directory tree IDs also includes descendants.
func (s *Snapshot) logPathEntries(ctx context.Context, sha string, paths []string) ([]Entry, error) {
	var o object
	if err := s.idx.get(ctx, "o/"+sha, &o); err != nil {
		return nil, err
	}
	snapshot := &Snapshot{idx: s.idx, Tree: o.Tree}
	entries := make([]Entry, len(paths))
	for i, path := range paths {
		if path == "." {
			path = ""
		}
		e, err := snapshot.Resolve(ctx, path)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		entries[i] = e
	}
	return entries, nil
}

func (s *Snapshot) simplifyLogPaths(ctx context.Context, sha string, parents, paths []string) (bool, []string, error) {
	entries, err := s.logPathEntries(ctx, sha, paths)
	if err != nil {
		return false, nil, err
	}
	if len(parents) == 0 {
		for _, e := range entries {
			if e.OID != "" {
				return true, nil, nil
			}
		}
		return false, nil, nil
	}
	for _, parent := range parents {
		previous, err := s.logPathEntries(ctx, parent, paths)
		if err != nil {
			return false, nil, err
		}
		same := true
		for i, e := range entries {
			if e.OID != previous[i].OID || e.Mode != previous[i].Mode {
				same = false
				break
			}
		}
		if same {
			return false, []string{parent}, nil
		}
	}
	return true, parents, nil
}

// Changed-path filters contain full file names, not directory prefixes. Only
// skip while every selected entry is a present non-directory. A transition to
// absence/directory changes that exact name and stops the skip at a Bloom hit.
func (s *Snapshot) advanceUnchangedLog(ctx context.Context, sha string, paths []string, cursor *historyCursor) (string, error) {
	entries, err := s.logPathEntries(ctx, sha, paths)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if e.OID == "" || e.Mode == 0040000 {
			return sha, nil
		}
	}
	var position historyPosition
	if err := s.history.get(ctx, "g/"+sha, &position); err != nil {
		// Complete local commit metadata can outlive the reachable-only
		// history accelerator. Ordinary path comparison remains authoritative.
		if errors.Is(err, store.ErrNotFound) {
			return sha, nil
		}
		return "", err
	}
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		node, err := cursor.get(ctx, uint64(position))
		if err != nil {
			return "", err
		}
		sha = hex.EncodeToString(node.Oid)
		if len(node.Parents) == 0 {
			return sha, nil
		}
		for _, path := range paths {
			if mayChangePath(node.ChangedPaths, path) {
				return sha, nil
			}
		}
		position = historyPosition(node.Parents[0])
	}
}
