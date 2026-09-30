package control

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "gyit/internal/gen/gyit/control/v1"
)

const requestTimeout = 30 * time.Second
const maxConnections = 16
const maxStreams = 64
const historyTimeout = 5 * time.Minute

func operationTimeout(req *pb.Request) time.Duration {
	if req.GetLog() != nil || req.GetPathLog() != nil || req.GetHistoryLog() != nil {
		return 0 // A paused pager or a coverage gap is not a stalled request.
	}
	if req.GetUpdate() != nil || req.GetDiff() != nil || req.GetBlame() != nil || req.GetView() != nil {
		return historyTimeout
	}
	return requestTimeout
}

// admission bounds concurrent control requests. Each connection is admitted
// with a request slot. An open-ended stream (a paused pager may hold one
// indefinitely) trades it for a stream slot once its request is read, so
// streams never block status or update. With no stream slot free, it keeps
// its request slot.
type admission struct {
	requests chan struct{}
	streams  chan struct{}
}

func newAdmission() *admission {
	return &admission{requests: make(chan struct{}, maxConnections), streams: make(chan struct{}, maxStreams)}
}

type Handler func(context.Context, *pb.Request) *pb.Response

// StreamHandler emits bounded protobuf frames; logs end with an explicit LogEnd.
type StreamHandler func(context.Context, *pb.Request, func(*pb.Response) error) error

type Server struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Listen requires a private directory so the socket is inaccessible even during
// creation. Existing paths are never removed or replaced, including stale sockets.
func Listen(ctx context.Context, path string, handler Handler) (*Server, error) {
	return ListenStream(ctx, path, func(ctx context.Context, req *pb.Request, send func(*pb.Response) error) error {
		return send(handler(ctx, req))
	})
}

func ListenStream(ctx context.Context, path string, handler StreamHandler) (*Server, error) {
	dir, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if !dir.IsDir() || dir.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("control socket directory must be private (mode 0700)")
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	listener.SetUnlinkOnClose(false)
	info, err := os.Lstat(path)
	if err != nil {
		listener.Close()
		return nil, err
	}
	cleanup := func() {
		listener.Close()
		if current, err := os.Lstat(path); err == nil && os.SameFile(info, current) {
			_ = os.Remove(path)
		}
	}
	if err := os.Chmod(path, 0600); err != nil {
		cleanup()
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	s := &Server{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		defer cleanup()
		stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
		defer stop()
		var workers sync.WaitGroup
		defer workers.Wait()
		admit := newAdmission()
		for {
			select {
			case admit.requests <- struct{}{}:
			case <-ctx.Done():
				return
			}
			conn, err := listener.Accept()
			if err != nil {
				<-admit.requests
				cancel()
				return
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				serveConnection(ctx, conn, handler, admit)
			}()
		}
	}()
	return s, nil
}

// Close cancels in-flight operations, closes their connections, and joins workers.
func (s *Server) Close() { s.cancel(); <-s.done }

// serveConnection applies identical framing, deadlines and cancellation to both
// transports. The caller has taken a request slot; serveConnection releases it.
func serveConnection(ctx context.Context, conn net.Conn, handler StreamHandler, admit *admission) {
	slot := admit.requests
	defer func() { <-slot }()
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	_ = conn.SetDeadline(time.Now().Add(requestTimeout))
	req := new(pb.Request)
	send := func(resp *pb.Response) error { return writeFrame(conn, resp) }
	if err := readFrame(conn, req); err != nil {
		_ = send(failure(pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, "malformed control request"))
		return
	}
	timeout := operationTimeout(req)
	var reqCtx context.Context
	var reqCancel context.CancelFunc
	if timeout == 0 {
		select {
		case admit.streams <- struct{}{}:
			<-slot
			slot = admit.streams
		default:
		}
		reqCtx, reqCancel = context.WithCancel(ctx)
		_ = conn.SetDeadline(time.Time{})
	} else {
		reqCtx, reqCancel = context.WithTimeout(ctx, timeout)
		_ = conn.SetDeadline(time.Now().Add(timeout))
	}
	defer reqCancel()
	peerDone := make(chan struct{})
	go func() {
		var extra [1]byte
		_, _ = conn.Read(extra[:])
		reqCancel()
		close(peerDone)
	}()
	defer func() { _ = conn.Close(); <-peerDone }()
	stopRequest := context.AfterFunc(reqCtx, func() { _ = conn.Close() })
	defer stopRequest()
	_ = handler(reqCtx, req, send)
}
