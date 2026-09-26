package control

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

const ControlFileName = ".gyit.control"
const controlFilePrefix = "fuse:"

// FileServer serves one bounded protocol exchange per open FUSE handle. Pipes
// apply backpressure without retaining a complete command response in memory.
type FileServer struct {
	ctx     context.Context
	cancel  context.CancelFunc
	handler StreamHandler
	mu      sync.Mutex
	closed  bool
	slots   chan struct{}
	workers sync.WaitGroup
}

func NewFileServer(ctx context.Context, handler StreamHandler) *FileServer {
	ctx, cancel := context.WithCancel(ctx)
	return &FileServer{ctx: ctx, cancel: cancel, handler: handler, slots: make(chan struct{}, maxConnections)}
}

func (s *FileServer) Open() (net.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.ctx.Err() != nil {
		return nil, net.ErrClosed
	}
	select {
	case s.slots <- struct{}{}:
	default:
		return nil, fmt.Errorf("too many open control requests")
	}
	client, server := net.Pipe()
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		defer func() { <-s.slots }()
		serveConnection(s.ctx, server, s.handler)
	}()
	return client, nil
}

func (s *FileServer) Close() {
	s.mu.Lock()
	s.closed = true
	s.cancel()
	s.mu.Unlock()
	s.workers.Wait()
}

func dialEndpoint(ctx context.Context, endpoint string) (io.ReadWriteCloser, error) {
	if path, ok := strings.CutPrefix(endpoint, controlFilePrefix); ok {
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		return &controlFile{File: f, ctx: ctx}, nil
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", endpoint)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	return conn, nil
}

// FUSE reads wait only briefly for a response and return EAGAIN while work is
// pending. Regular files cannot implement SetDeadline: retrying here makes
// caller deadlines and cancellation work without stranding a blocked read.
type controlFile struct {
	*os.File
	ctx context.Context
}

func (f *controlFile) retry(operation func([]byte) (int, error), data []byte) (int, error) {
	for {
		if err := f.ctx.Err(); err != nil {
			return 0, err
		}
		n, err := operation(data)
		if !errors.Is(err, syscall.EAGAIN) {
			return n, err
		}
		if n > 0 {
			return n, nil
		}
		select {
		case <-f.ctx.Done():
			return 0, f.ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}
func (f *controlFile) Read(p []byte) (int, error)  { return f.retry(f.File.Read, p) }
func (f *controlFile) Write(p []byte) (int, error) { return f.retry(f.File.Write, p) }
