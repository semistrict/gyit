package githubfs

import (
	"context"
	"errors"
	"fmt"

	"gyit/internal/control"
	pb "gyit/internal/gen/gyit/control/v1"
	"gyit/internal/repo"
)

func commandError(code pb.ErrorCode, message string) *pb.Response {
	return &pb.Response{Version: control.Version, Result: &pb.Response_Error{Error: &pb.Error{Code: code, Message: message}}}
}
func commandSnapshot(s *repo.Snapshot) *pb.Response {
	return &pb.Response{Version: control.Version, Result: &pb.Response_Snapshot{Snapshot: &pb.Snapshot{Sha: s.SHA, Tree: s.Tree, Branch: s.Branch, DetachedAt: s.DetachedAt}}}
}

// Only explicit update requests advance a mounted identifier. Existing readers
// retain their snapshot while acquisition and metadata preparation take place.
func (f *FS) update(ctx context.Context, j *job) (*repo.Snapshot, error) {
	if j.update == nil {
		return nil, fmt.Errorf("this mount has no refreshable upstream")
	}
	select {
	case j.update <- struct{}{}:
		defer func() { <-j.update }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	current, _, _ := j.status()
	if current == nil {
		return nil, fmt.Errorf("repository setup is not complete")
	}
	if fullSHA(j.target.Revision) {
		return current, nil
	}

	p, next, err := f.prepareProgressive(ctx, j.target, j.progress)
	if err != nil {
		return nil, err
	}
	if next.SHA == current.SHA {
		return current, nil
	}
	if err = p.reader.PrepareSnapshot(ctx, next.SHA); err != nil {
		return nil, err
	}
	if ready, e := p.reader.HasHistoryIndex(ctx); e != nil {
		return nil, e
	} else if ready {
		if e = p.reader.IngestHistory(ctx, next.SHA, p.source, p.historySource); e != nil && !errors.Is(e, repo.ErrHistoryIndexPending) {
			return nil, e
		}
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	j.mu.Lock()
	j.snapshot = next
	j.generation++
	j.mu.Unlock()
	f.changed(j.display)
	f.startProgressiveBackground(p, next, j)
	return next, nil
}
