package control

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "gat/internal/gen/gat/control/v1"
)

const requestTimeout = 30 * time.Second
const maxConnections = 16
const historyTimeout = 5 * time.Minute

func operationTimeout(req *pb.Request) time.Duration {
	if req.GetDiff() != nil || req.GetBlame() != nil || req.GetView() != nil {
		return historyTimeout
	}
	return requestTimeout
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
		slots := make(chan struct{}, maxConnections)
		for {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			conn, err := listener.Accept()
			if err != nil {
				<-slots
				cancel()
				return
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer func() { <-slots }()
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
				reqCtx, reqCancel := context.WithTimeout(ctx, timeout)
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
				_ = conn.SetDeadline(time.Now().Add(timeout))
				_ = handler(reqCtx, req, send)
			}()
		}
	}()
	return s, nil
}

// Close cancels in-flight operations, closes their connections, and joins workers.
func (s *Server) Close() { s.cancel(); <-s.done }
