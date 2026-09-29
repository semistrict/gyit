package control

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"gyit/internal/repo"
	"time"

	pb "gyit/internal/gen/gyit/control/v1"
)

// RemoteError preserves the server's typed protobuf error code.
type RemoteError struct {
	Code    pb.ErrorCode
	Message string
}

func (e *RemoteError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

// Endpoint is a discovered FUSE control file or an explicit legacy Unix socket.
type Client struct{ Endpoint string }

func (c Client) Status(ctx context.Context) (*pb.Snapshot, error) {
	return c.call(ctx, &pb.Request{Version: Version, Operation: &pb.Request_Status{Status: &pb.StatusRequest{}}})
}
func (c Client) Update(ctx context.Context) (*pb.Snapshot, error) {
	return c.call(ctx, &pb.Request{Version: Version, Operation: &pb.Request_Update{Update: &pb.UpdateRequest{}}})
}
func (c Client) Switch(ctx context.Context, revision string) (*pb.Snapshot, error) {
	return c.call(ctx, &pb.Request{Version: Version, Operation: &pb.Request_Switch{Switch: &pb.SwitchRequest{Revision: revision}}})
}

// A transport error can occur after a switch committed. Query Status before
// retrying when the caller needs to resolve an ambiguous result.
func (c Client) call(ctx context.Context, req *pb.Request) (snapshot *pb.Snapshot, err error) {
	err = c.exchange(ctx, req, func(resp *pb.Response) (bool, error) {
		if result, ok := resp.Result.(*pb.Response_Snapshot); ok {
			snapshot = result.Snapshot
			return true, nil
		}
		return false, fmt.Errorf("unexpected control response")
	})
	return snapshot, err
}

func (c Client) Log(ctx context.Context, revision string, count int, firstParent bool, emit func(*pb.LogEntry) error) error {
	if count < 0 || count > repo.MaxLogCount {
		return fmt.Errorf("log count must be between 0 and %d", repo.MaxLogCount)
	}
	return c.LogPaths(ctx, &pb.LogRequest{Revision: revision, MaxCount: uint32(count), FirstParent: firstParent}, emit)
}

func (c Client) LogPaths(ctx context.Context, log *pb.LogRequest, emit func(*pb.LogEntry) error) error {
	if log.MaxCount > repo.MaxLogCount {
		return fmt.Errorf("log count must be between 0 and %d", repo.MaxLogCount)
	}
	if log.MaxCount == 0 && !log.Unlimited {
		return nil
	}
	req := &pb.Request{Version: Version, Operation: &pb.Request_Log{Log: log}}
	if len(log.Paths) > 0 || len(log.PossiblePath) > 0 || log.Follow {
		req.Operation = &pb.Request_HistoryLog{HistoryLog: log}
	}
	return c.exchange(ctx, req, func(resp *pb.Response) (bool, error) {
		switch result := resp.Result.(type) {
		case *pb.Response_LogEntry:
			return false, emit(result.LogEntry)
		case *pb.Response_LogEnd:
			return true, nil
		default:
			return false, fmt.Errorf("unexpected log response")
		}
	})
}

func (c Client) exchange(ctx context.Context, req *pb.Request, consume func(*pb.Response) (bool, error)) (err error) {
	if timeout := operationTimeout(req); timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout+5*time.Second)
		defer cancel()
	}
	defer func() {
		if err != nil {
			if ctx.Err() != nil {
				err = ctx.Err()
			} else if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
				err = context.DeadlineExceeded
			}
		}
	}()
	conn, err := dialEndpoint(ctx, c.Endpoint)
	if err != nil {
		return err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if err := writeFrame(conn, req); err != nil {
		return err
	}
	// Batch available frames into one filesystem read; individual header/body
	// reads otherwise require two guest/host crossings per entry.
	reader := bufio.NewReaderSize(conn, MaxFrameSize+4)
	for {
		resp := new(pb.Response)
		if err := readFrame(reader, resp); err != nil {
			return err
		}
		if resp.Version != Version {
			return fmt.Errorf("unsupported control response version %d", resp.Version)
		}
		if remote := resp.GetError(); remote != nil {
			return &RemoteError{Code: remote.Code, Message: remote.Message}
		}
		done, err := consume(resp)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func (c Client) Diff(ctx context.Context, req *pb.DiffRequest, emit func([]byte) error) error {
	return c.exchange(ctx, &pb.Request{Version: Version, Operation: &pb.Request_Diff{Diff: req}}, func(resp *pb.Response) (bool, error) {
		switch r := resp.Result.(type) {
		case *pb.Response_DiffChunk:
			return false, emit(r.DiffChunk.Data)
		case *pb.Response_StreamEnd:
			return true, nil
		default:
			return false, fmt.Errorf("unexpected diff response")
		}
	})
}
func (c Client) Blame(ctx context.Context, req *pb.BlameRequest, emit func(*pb.BlameLine) error) error {
	return c.exchange(ctx, &pb.Request{Version: Version, Operation: &pb.Request_Blame{Blame: req}}, func(resp *pb.Response) (bool, error) {
		switch r := resp.Result.(type) {
		case *pb.Response_BlameLine:
			return false, emit(r.BlameLine)
		case *pb.Response_StreamEnd:
			return true, nil
		default:
			return false, fmt.Errorf("unexpected blame response")
		}
	})
}

func (c Client) View(ctx context.Context, req *pb.ViewRequest, emit func([]byte) error) error {
	err := c.exchange(ctx, &pb.Request{Version: Version, Operation: &pb.Request_View{View: req}}, func(resp *pb.Response) (bool, error) {
		switch v := resp.Result.(type) {
		case *pb.Response_DiffChunk:
			return false, emit(v.DiffChunk.Data)
		case *pb.Response_StreamEnd:
			return true, nil
		default:
			return false, fmt.Errorf("unexpected view response")
		}
	})
	var remote *RemoteError
	if errors.As(err, &remote) && remote.Code == pb.ErrorCode_ERROR_CODE_NO_MATCH {
		return repo.ErrViewNoMatch
	}
	return err
}
