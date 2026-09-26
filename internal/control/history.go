package control

import (
	"bufio"
	"context"
	"errors"
	"strings"

	pb "gyit/internal/gen/gyit/control/v1"
	"gyit/internal/repo"
)

type patchWriter struct{ send func(*pb.Response) error }

func (w patchWriter) Write(data []byte) (int, error) {
	n := 0
	for len(data) > 0 {
		size := min(len(data), 32<<10)
		if err := w.send(&pb.Response{Version: Version, Result: &pb.Response_DiffChunk{DiffChunk: &pb.DiffChunk{Data: data[:size]}}}); err != nil {
			return n, err
		}
		data = data[size:]
		n += size
	}
	return n, nil
}
func historyFailure(ctx context.Context, err error) *pb.Response {
	if ctx.Err() != nil {
		return contextFailure(ctx.Err())
	}
	code := pb.ErrorCode_ERROR_CODE_INTERNAL
	if errors.Is(err, repo.ErrInvalidRevision) {
		code = pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT
	}
	if repo.IsNotFound(err) || errors.Is(err, repo.ErrLogMetadata) {
		code = pb.ErrorCode_ERROR_CODE_NOT_FOUND
	}
	if errors.Is(err, repo.ErrViewNoMatch) {
		code = pb.ErrorCode_ERROR_CODE_NO_MATCH
	}
	return failure(code, err.Error())
}
func (c *Controller) serveHistory(ctx context.Context, req *pb.Request, send func(*pb.Response) error) error {
	// History has bounded working buffers independent of the FUSE data cache.
	// Limit their concurrency while status, switching and file reads stay available.
	select {
	case c.history <- struct{}{}:
		defer func() { <-c.history }()
	case <-ctx.Done():
		return send(contextFailure(ctx.Err()))
	}
	current := c.Current()
	var err error
	if v := req.GetView(); v != nil {
		args := make([]string, len(v.Arguments))
		for i, a := range v.Arguments {
			args[i] = string(a)
		}
		out := bufio.NewWriterSize(patchWriter{send}, 32<<10)
		err = c.repository.View(ctx, current, repo.ViewOptions{Command: v.Command, Args: args, Prefix: string(v.PathPrefix), MountRoot: string(v.MountRoot)}, out)
		// Even a quiet nonzero result can follow valid partial output.
		if flushErr := out.Flush(); flushErr != nil {
			err = flushErr
		}
	} else if d := req.GetDiff(); d != nil {
		from, to := d.FromRevision, d.ToRevision
		if from == "" {
			from = current.SHA
		}
		if to == "" {
			to = current.SHA
		}
		var ends []*repo.Snapshot
		ends, err = c.repository.OpenRevisions(ctx, []string{from, to}, current.SHA)
		if err == nil {
			paths := make([]string, len(d.Paths))
			for i, p := range d.Paths {
				paths[i] = string(p)
			}
			out := bufio.NewWriterSize(patchWriter{send}, 32<<10)
			err = ends[0].Diff(ctx, ends[1], repo.DiffOptions{Paths: paths, NameOnly: d.NameOnly, NameStatus: d.NameStatus, Context: int(d.ContextLines)}, out)
			if err == nil {
				err = out.Flush()
			}
		}
	} else if b := req.GetBlame(); b != nil {
		revision := b.Revision
		if revision == "" {
			revision = current.SHA
		}
		var selected *repo.Snapshot
		selected, err = c.repository.OpenRevision(ctx, revision, current.SHA)
		if err == nil {
			err = selected.Blame(ctx, repo.BlameOptions{Path: string(b.Path), Start: int(b.StartLine), End: int(b.EndLine), FirstParent: b.FirstParent, IncludeCommitter: b.IncludeCommitter}, func(e repo.BlameLine) error {
				return send(blameResponse(e))
			})
		}
	}
	if err != nil {
		return send(historyFailure(ctx, err))
	}
	return send(&pb.Response{Version: Version, Result: &pb.Response_StreamEnd{StreamEnd: &pb.StreamEnd{}}})
}

func blameResponse(e repo.BlameLine) *pb.Response {
	summary := strings.SplitN(string(e.Message), "\n", 2)[0]
	previousPath := e.PreviousPath
	// The same path would consume another 4 KiB at the permitted limit. Keep
	// room for all bounded identities, summary, content and protobuf framing.
	if previousPath == e.Path {
		previousPath = ""
	}
	return &pb.Response{Version: Version, Result: &pb.Response_BlameLine{BlameLine: &pb.BlameLine{Sha: e.SHA, Author: e.Author, AuthorTime: e.AuthorTime, AuthorOffsetMinutes: e.AuthorOffset, OriginalLine: uint32(e.OriginalLine), FinalLine: uint32(e.FinalLine), Content: e.Content, Boundary: e.Boundary, Path: []byte(e.Path), Summary: []byte(summary), AuthorTruncated: e.AuthorTruncated, MessageTruncated: e.MessageTruncated, Committer: e.Committer, CommitterTime: e.CommitterTime, CommitterOffsetMinutes: e.CommitterOffset, CommitterTruncated: e.CommitterTruncated, HasCommitter: e.HasCommitter, PreviousSha: e.PreviousSHA, PreviousPath: []byte(previousPath), GroupLines: uint32(e.GroupLines)}}}
}
