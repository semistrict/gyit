package control

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "gyit/internal/gen/gyit/control/v1"
	"gyit/internal/repo"
)

func TestLogOperationsAllowColdHistoryAcquisition(t *testing.T) {
	for name, req := range map[string]*pb.Request{
		"log":     {Operation: &pb.Request_Log{Log: &pb.LogRequest{}}},
		"path":    {Operation: &pb.Request_PathLog{PathLog: &pb.LogRequest{}}},
		"history": {Operation: &pb.Request_HistoryLog{HistoryLog: &pb.LogRequest{}}},
	} {
		t.Run(name, func(t *testing.T) {
			if got := operationTimeout(req); got != 0 {
				t.Fatalf("paged history has an implicit cutoff of %s", got)
			}
		})
	}
}

// A connected pager may wait indefinitely for another publication. Only its
// caller, disconnection, or server shutdown should end the log request.
func TestStreamingLogUsesCallerCancellation(t *testing.T) {
	dir, err := os.MkdirTemp("", "gyit-stream-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	entered := make(chan bool, 1)
	stopped := make(chan struct{})
	server, err := ListenStream(t.Context(), filepath.Join(dir, "control.sock"), func(ctx context.Context, req *pb.Request, send func(*pb.Response) error) error {
		_, hasDeadline := ctx.Deadline()
		entered <- hasDeadline
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (Client{Endpoint: filepath.Join(dir, "control.sock")}).LogPaths(ctx, &pb.LogRequest{Unlimited: true}, func(*pb.LogEntry) error { return nil })
	}()
	select {
	case deadline := <-entered:
		if deadline {
			t.Error("streaming handler has an implicit deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("log request did not reach handler")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("caller cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("client did not cancel")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("disconnected reader left handler running")
	}
}

// Log streams are open-ended: a paused pager holds one indefinitely. Any
// number of them up to the stream budget must leave the request budget free,
// so status and update still work.
func TestPausedLogStreamsDoNotBlockShortRequests(t *testing.T) {
	dir, err := os.MkdirTemp("", "gyit-stream-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	c := New(nil, &repo.Snapshot{SHA: strings.Repeat("a", 40), Tree: "tree"})
	paused := make(chan struct{}, maxStreams)
	endpoint := filepath.Join(dir, "control.sock")
	server, err := ListenStream(t.Context(), endpoint, func(ctx context.Context, req *pb.Request, send func(*pb.Response) error) error {
		if req.GetLog() == nil {
			return send(c.Handle(ctx, req))
		}
		paused <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client := Client{Endpoint: endpoint}
	var pagers sync.WaitGroup
	defer pagers.Wait()
	defer cancel()
	for range maxStreams {
		pagers.Add(1)
		go func() {
			defer pagers.Done()
			_ = client.LogPaths(ctx, &pb.LogRequest{Unlimited: true}, func(*pb.LogEntry) error { return nil })
		}()
	}
	for range maxStreams {
		select {
		case <-paused:
		case <-time.After(5 * time.Second):
			t.Fatal("log streams were not all admitted")
		}
	}
	statusCtx, stop := context.WithTimeout(t.Context(), 2*time.Second)
	defer stop()
	s, err := client.Status(statusCtx)
	if err != nil || s.Sha != c.Current().SHA {
		t.Fatalf("status with %d paused log streams: %v %v", maxStreams, s, err)
	}
}
