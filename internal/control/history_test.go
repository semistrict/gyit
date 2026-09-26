package control

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "gyit/internal/gen/gyit/control/v1"
)

func TestHistoryStreamsRequireTerminator(t *testing.T) {
	for _, kind := range []string{"diff", "blame"} {
		for _, complete := range []bool{true, false} {
			dir, err := os.MkdirTemp("", "gyit-history-wire-")
			if err != nil {
				t.Fatal(err)
			}
			socket := filepath.Join(dir, "control.sock")
			server, err := ListenStream(context.Background(), socket, func(ctx context.Context, req *pb.Request, send func(*pb.Response) error) error {
				for i := 0; i < 12; i++ {
					resp := &pb.Response{Version: Version}
					if kind == "diff" {
						resp.Result = &pb.Response_DiffChunk{DiffChunk: &pb.DiffChunk{Data: make([]byte, 16000)}}
					} else {
						resp.Result = &pb.Response_BlameLine{BlameLine: &pb.BlameLine{Content: make([]byte, 16000)}}
					}
					if err := send(resp); err != nil {
						return err
					}
				}
				if complete {
					return send(&pb.Response{Version: Version, Result: &pb.Response_StreamEnd{StreamEnd: &pb.StreamEnd{}}})
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			client := Client{Endpoint: socket}
			n := 0
			if kind == "diff" {
				err = client.Diff(context.Background(), &pb.DiffRequest{}, func([]byte) error { n++; return nil })
			} else {
				err = client.Blame(context.Background(), &pb.BlameRequest{}, func(*pb.BlameLine) error { n++; return nil })
			}
			if n != 12 || (complete && err != nil) || (!complete && !errors.Is(err, io.EOF)) {
				t.Fatal("stream completion", kind, complete, n, err)
			}
			server.Close()
			os.RemoveAll(dir)
		}
	}
}
func TestHistoryCanceledClientStopsServerWork(t *testing.T) {
	dir, err := os.MkdirTemp("", "gyit-history-cancel-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	started, stopped := make(chan struct{}), make(chan struct{})
	server, err := ListenStream(context.Background(), filepath.Join(dir, "s"), func(ctx context.Context, req *pb.Request, send func(*pb.Response) error) error {
		close(started)
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (Client{Endpoint: filepath.Join(dir, "s")}).Blame(ctx, &pb.BlameRequest{}, func(*pb.BlameLine) error { return nil })
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("server kept working after disconnect")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
