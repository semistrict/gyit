// Package control implements the mount's protobuf control protocol.
package control

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	pb "gyit/internal/gen/gyit/control/v1"
	"gyit/internal/pathspec"
	"gyit/internal/repo"
	"gyit/internal/store"
)

// Controller owns the current snapshot. File handles retain their own snapshot.
// Switching performs only the repository's metadata lookup before publication.
type Controller struct {
	repository *repo.Repository
	current    atomic.Pointer[repo.Snapshot]
	writer     chan struct{}
	history    chan struct{}
	previous   *repo.Snapshot
	validate   func(context.Context, *repo.Snapshot) error
}

func New(repository *repo.Repository, initial *repo.Snapshot) *Controller {
	return NewWithValidator(repository, initial, nil)
}

// NewWithValidator checks mount-specific constraints before installing a version.
// The caller validates initial before constructing the controller.
func NewWithValidator(repository *repo.Repository, initial *repo.Snapshot, validate func(context.Context, *repo.Snapshot) error) *Controller {
	c := &Controller{repository: repository, writer: make(chan struct{}, 1), history: make(chan struct{}, 2), validate: validate}
	c.current.Store(initial)
	return c
}

func (c *Controller) Current() *repo.Snapshot { return c.current.Load() }

func failure(code pb.ErrorCode, message string) *pb.Response {
	return &pb.Response{Version: Version, Result: &pb.Response_Error{Error: &pb.Error{Code: code, Message: message}}}
}

func snapshot(s *repo.Snapshot) *pb.Response {
	return &pb.Response{Version: Version, Result: &pb.Response_Snapshot{Snapshot: &pb.Snapshot{Sha: s.SHA, Tree: s.Tree, Branch: s.Branch, DetachedAt: s.DetachedAt}}}
}

func contextFailure(err error) *pb.Response {
	code := pb.ErrorCode_ERROR_CODE_CANCELED
	if errors.Is(err, context.DeadlineExceeded) {
		code = pb.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED
	}
	return failure(code, err.Error())
}

func (c *Controller) Handle(ctx context.Context, req *pb.Request) *pb.Response {
	if req.Version != Version {
		return failure(pb.ErrorCode_ERROR_CODE_UNSUPPORTED_VERSION, "unsupported control protocol version")
	}
	switch op := req.Operation.(type) {
	case *pb.Request_Status:
		return snapshot(c.Current())
	case *pb.Request_Switch:
		revision := op.Switch.GetRevision()
		if revision != "" && op.Switch.GetSha() != "" {
			return failure(pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, "provide revision or legacy sha, not both")
		}
		if revision == "" {
			revision = op.Switch.GetSha()
		}
		if revision == "" {
			return failure(pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, "expected a revision")
		}
		// Serialize writers without blocking status or filesystem readers.
		select {
		case c.writer <- struct{}{}:
			defer func() { <-c.writer }()
		case <-ctx.Done():
			return contextFailure(ctx.Err())
		}
		current := c.Current()
		var next *repo.Snapshot
		var err error
		switch revision {
		case "HEAD", "@":
			next = current
		case "-", "@{-1}":
			if c.previous == nil {
				return failure(pb.ErrorCode_ERROR_CODE_NOT_FOUND, "no previous version in this mount")
			}
			next = c.previous
		default:
			next, err = c.repository.OpenRevision(ctx, revision, current.SHA)
		}
		if err == nil && next != current && c.validate != nil {
			err = c.validate(ctx, next)
		}
		if err != nil {
			if ctx.Err() != nil {
				return contextFailure(ctx.Err())
			}
			if errors.Is(err, repo.ErrInvalidRevision) {
				return failure(pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, err.Error())
			}
			if repo.IsNotFound(err) {
				return failure(pb.ErrorCode_ERROR_CODE_NOT_FOUND, err.Error())
			}
			return failure(pb.ErrorCode_ERROR_CODE_INTERNAL, err.Error())
		}
		if ctx.Err() != nil {
			return contextFailure(ctx.Err())
		}
		if current.SHA != next.SHA || current.Branch != next.Branch {
			c.previous = current
		} else if current.Branch == "" && next != current {
			// Like checkout, a no-op detached switch retains its original display name.
			next.DetachedAt = current.DetachedAt
		}
		c.current.Store(next)
		return snapshot(next)
	default:
		return failure(pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, "missing or unknown operation")
	}
}

// Serve captures one selected commit for the entire stream, even if another
// control client switches this mount while log output is being consumed.
func (c *Controller) Serve(ctx context.Context, req *pb.Request, send func(*pb.Response) error) error {
	if req.Version == Version && (req.GetDiff() != nil || req.GetBlame() != nil || req.GetView() != nil) {
		return c.serveHistory(ctx, req, send)
	}
	log := req.GetLog()
	if log == nil {
		log = req.GetPathLog()
	}
	if log == nil {
		log = req.GetHistoryLog()
	}
	if log == nil || req.Version != Version {
		return send(c.Handle(ctx, req))
	}
	count := int(log.GetMaxCount())
	if count == 0 {
		count = repo.DefaultLogCount
	}
	if count > repo.MaxLogCount {
		return send(failure(pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, "log count exceeds 1000"))
	}
	current := c.Current()
	revision := log.GetRevision()
	if revision == "" || revision == "HEAD" || revision == "@" {
		revision = current.SHA
	}
	// Resolve using one latest publication: commit metadata can have been added
	// after mounting, while the selected commit and file view stay unchanged.
	selected, err := c.repository.OpenRevision(ctx, revision, current.SHA)
	paths := make([]string, 0, len(log.Paths)+1)
	for _, p := range log.Paths {
		paths = append(paths, string(p))
	}
	if len(log.PossiblePath) > 0 {
		path := string(log.PossiblePath)
		matches, pathErr := current.MatchesPathspec(ctx, path, string(log.PathPrefix))
		if pathErr == nil && !matches && !pathspec.LooksLike(path) {
			pathErr = store.ErrNotFound
		}
		if pathErr == nil {
			if err == nil {
				err = fmt.Errorf("%w: ambiguous argument %q: both revision and filename; use -- to separate paths", repo.ErrInvalidRevision, log.Revision)
			} else if repo.IsNotFound(err) || errors.Is(err, repo.ErrInvalidRevision) {
				selected, err = c.repository.OpenRevision(ctx, current.SHA, current.SHA)
				paths = append([]string{path}, paths...)
			}
		} else if !repo.IsNotFound(pathErr) {
			err = pathErr
		}
	}
	if err == nil && len(log.PossiblePath) > 0 {
		for _, path := range paths {
			if path == "." || pathspec.LooksLike(path) {
				continue
			}
			if matches, pathErr := current.MatchesPathspec(ctx, path, string(log.PathPrefix)); pathErr != nil || !matches {
				err = fmt.Errorf("%w: path %q is not in the mounted tree; use -- for historical paths", repo.ErrInvalidRevision, path)
				break
			}
		}
	}

	if err == nil {
		err = selected.LogWithOptions(ctx, repo.LogOptions{Count: count, FirstParent: log.GetFirstParent(), Follow: log.Follow, Paths: paths, Prefix: string(log.PathPrefix), AttributeSource: current}, func(e repo.LogEntry) error {
			return send(&pb.Response{Version: Version, Result: &pb.Response_LogEntry{LogEntry: &pb.LogEntry{Sha: e.SHA, ShortSha: e.ShortSHA, Parents: e.Parents, ParentAbbrevLengths: logParentLengths(e.ShortParents), Author: e.Author, AuthorTime: e.AuthorTime, AuthorOffsetMinutes: e.AuthorOffset, Message: e.Message, MessageTruncated: e.MessageTruncated, AuthorTruncated: e.AuthorTruncated}}})
		})
	}
	if err != nil {
		if ctx.Err() != nil {
			return send(contextFailure(ctx.Err()))
		}
		code := pb.ErrorCode_ERROR_CODE_INTERNAL
		if errors.Is(err, repo.ErrInvalidRevision) {
			code = pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT
		}
		if repo.IsNotFound(err) || errors.Is(err, repo.ErrLogMetadata) {
			code = pb.ErrorCode_ERROR_CODE_NOT_FOUND
		}
		return send(failure(code, err.Error()))
	}
	return send(&pb.Response{Version: Version, Result: &pb.Response_LogEnd{LogEnd: &pb.LogEnd{}}})
}
