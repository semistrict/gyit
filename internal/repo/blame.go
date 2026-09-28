package repo

import (
	"context"
	"fmt"
	"strings"
)

const maxBlameCommits = 100000
const maxBlameTasks = 4096
const maxBlameLine = 24 << 10

type BlameOptions struct {
	Path             string
	Start, End       int
	FirstParent      bool
	IncludeCommitter bool
}
type BlameLine struct {
	SHA                                         string
	Author, Message, Content                    []byte
	AuthorTime                                  int64
	AuthorOffset                                int32
	OriginalLine, FinalLine                     int
	Boundary, AuthorTruncated, MessageTruncated bool
	Path                                        string
	Committer                                   []byte
	CommitterTime                               int64
	CommitterOffset                             int32
	CommitterTruncated, HasCommitter            bool
	PreviousSHA, PreviousPath                   string
	GroupLines                                  int
}
type lineOrigin struct {
	sha      string
	line     int
	boundary bool
	previous string
}
type lineLink struct{ current, final int }
type blameTask struct {
	sha   string
	lines []lineLink
}

func (s *Snapshot) atCommit(ctx context.Context, sha string) (*Snapshot, error) {
	var o object
	if err := s.idx.get(ctx, "o/"+sha, &o); err != nil {
		return nil, err
	}
	if o.Kind != "commit" {
		return nil, fmt.Errorf("history target is not a commit")
	}
	return &Snapshot{progressive: s.progressive, idx: s.idx, SHA: sha, Tree: o.Tree}, nil
}

// Blame passes surviving lines to matching parent lines, following all parents
// at merges. Tasks only retain line coordinates; file versions are read as needed.
// This initial implementation follows the same path, not renames or moved code.
func (s *Snapshot) Blame(ctx context.Context, opts BlameOptions, emit func(BlameLine) error) error {
	if err := validHistoryPath(opts.Path); err != nil {
		return err
	}
	if opts.Path == "" || opts.Path == "." {
		return invalidRevision("blame requires a file path")
	}
	entry, err := s.Resolve(ctx, opts.Path)
	if err != nil {
		return err
	}
	if entry.Mode == 0040000 || entry.Mode == 0160000 {
		return invalidRevision("blame requires a file, not a directory or submodule")
	}
	content, err := s.historyBlob(ctx, entry)
	if err != nil {
		return err
	}
	original, err := textLines(content)
	if err != nil {
		return err
	}
	start, end := opts.Start, opts.End
	if start == 0 {
		start = 1
	}
	if end == 0 {
		end = len(original)
	}
	if start < 1 || end < 0 || (len(original) > 0 && (end < start || end > len(original))) || (len(original) == 0 && (start != 1 || end != 0)) {
		return invalidRevision("blame line range is outside the file")
	}
	if len(original) == 0 {
		return nil
	}
	for _, line := range original[start-1 : end] {
		if len(line) > maxBlameLine {
			return fmt.Errorf("blame line exceeds the 24 KiB limit")
		}
	}
	origins := make([]lineOrigin, len(original))
	links := make([]lineLink, 0, end-start+1)
	for i := start - 1; i < end; i++ {
		links = append(links, lineLink{i, i})
	}
	tasks := []blameTask{{sha: s.SHA, lines: links}}
	visited := 0
	for len(tasks) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		task := tasks[len(tasks)-1]
		tasks[len(tasks)-1] = blameTask{}
		tasks = tasks[:len(tasks)-1]
		cur, err := s.atCommit(ctx, task.sha)
		if err != nil {
			return err
		}
		var p parents
		if err = s.idx.get(ctx, "p/"+task.sha, &p); err != nil {
			return err
		}
		visited++
		if visited > maxBlameCommits {
			return fmt.Errorf("blame exceeds 100000 candidate commit visits; narrow the line range")
		}
		ce, err := cur.Resolve(ctx, opts.Path)
		if err != nil {
			return err
		}
		if len(p.Parents) > maxLogParents {
			return fmt.Errorf("commit exceeds bounded parent limit")
		}
		next := p.Parents
		if opts.FirstParent && len(next) > 1 {
			next = next[:1]
		}
		remaining := task.lines
		var currentLines []string
		previous := ""
		for _, sha := range next {
			if len(remaining) == 0 {
				break
			}
			parent, err := s.atCommit(ctx, sha)
			if err != nil {
				return err
			}
			pe, err := parent.Resolve(ctx, opts.Path)
			if IsNotFound(err) {
				continue
			}
			if err != nil {
				return err
			}
			if pe.Mode == 0040000 || pe.Mode == 0160000 {
				continue
			}
			// A type change creates a new origin even when the blob bytes match.
			// Permission-only changes on regular files keep their ancestry.
			if ce.Mode&0170000 != pe.Mode&0170000 {
				continue
			}
			// Git reports the first usable parent origin, even when no line is
			// inherited from it. The supported blame mode keeps the same path.
			if previous == "" {
				previous = sha
			}
			var transfer []lineLink
			if ce.OID == pe.OID {
				transfer = remaining
				remaining = nil
			} else {
				if currentLines == nil {
					data, err := cur.historyBlob(ctx, ce)
					if err != nil {
						return err
					}
					currentLines, err = textLines(data)
					if err != nil {
						return err
					}
				}
				data, err := parent.historyBlob(ctx, pe)
				if err != nil {
					return err
				}
				// A binary parent supplies no text lines.
				if strings.IndexByte(string(data), 0) >= 0 {
					continue
				}
				parentLines, err := textLines(data)
				if err != nil {
					return err
				}
				edits, err := lineDiff(ctx, parentLines, currentLines)
				if err != nil {
					return err
				}
				matches := make([]int, len(currentLines))
				for i := range matches {
					matches[i] = -1
				}
				for _, e := range edits {
					if e.kind == ' ' {
						matches[e.b] = e.a
					}
				}
				kept := make([]lineLink, 0, len(remaining))
				for _, link := range remaining {
					if matches[link.current] >= 0 {
						transfer = append(transfer, lineLink{matches[link.current], link.final})
					} else {
						kept = append(kept, link)
					}
				}
				remaining = kept
			}
			if len(transfer) > 0 {
				if len(tasks) >= maxBlameTasks {
					return fmt.Errorf("blame exceeds bounded history frontier")
				}
				tasks = append(tasks, blameTask{sha: sha, lines: transfer})
			}
		}
		for _, link := range remaining {
			origins[link.final] = lineOrigin{sha: task.sha, line: link.current, boundary: len(p.Parents) == 0, previous: previous}
		}
	}
	lastSHA := ""
	var info commitInfo
	groupEnd := start - 1
	for i := start - 1; i < end; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		origin := origins[i]
		if origin.sha == "" {
			return fmt.Errorf("incomplete blame attribution")
		}
		if origin.sha != lastSHA {
			if err := s.idx.get(ctx, "c/"+origin.sha, &info); err != nil {
				return err
			}
			lastSHA = origin.sha
		}
		if opts.IncludeCommitter && !info.HasCommitter {
			return fmt.Errorf("commit committer metadata is missing; re-import this store for porcelain blame")
		}
		groupLines := 0
		if i == groupEnd {
			groupEnd = i + 1
			for groupEnd < end {
				next := origins[groupEnd]
				if next.sha != origin.sha || next.line != origin.line+groupEnd-i || next.previous != origin.previous {
					break
				}
				groupEnd++
			}
			groupLines = groupEnd - i
		}
		e := BlameLine{SHA: origin.sha, Author: info.Author, Message: info.Message, Content: []byte(original[i]), AuthorTime: info.AuthorTime, AuthorOffset: info.AuthorOffset, OriginalLine: origin.line + 1, FinalLine: i + 1, Boundary: origin.boundary, Path: opts.Path, AuthorTruncated: info.AuthorTruncated, MessageTruncated: info.MessageTruncated}
		e.Committer, e.CommitterTime, e.CommitterOffset = info.Committer, info.CommitTime, info.CommitterOffset
		e.CommitterTruncated, e.HasCommitter = info.CommitterTruncated, info.HasCommitter
		e.PreviousSHA, e.GroupLines = origin.previous, groupLines
		if origin.previous != "" {
			e.PreviousPath = opts.Path
		}
		if err := emit(e); err != nil {
			return err
		}
	}
	return nil
}
