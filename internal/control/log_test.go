package control

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	pb "gat/internal/gen/gat/control/v1"
)

func TestLogStreamingAndMissingTerminator(t *testing.T) {
	for _, complete := range []bool{true, false} {
		dir, err := os.MkdirTemp("", "gat-log-wire-")
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "control.sock")
		server, err := ListenStream(context.Background(), path, func(ctx context.Context, req *pb.Request, send func(*pb.Response) error) error {
			for i := 0; i < 12; i++ {
				if err := send(&pb.Response{Version: Version, Result: &pb.Response_LogEntry{LogEntry: &pb.LogEntry{Sha: "entry", Message: make([]byte, 16<<10)}}}); err != nil {
					return err
				}
			}
			if complete {
				return send(&pb.Response{Version: Version, Result: &pb.Response_LogEnd{LogEnd: &pb.LogEnd{}}})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		client := Client{Endpoint: path}
		count := 0
		err = client.Log(context.Background(), "", 20, false, func(*pb.LogEntry) error { count++; return nil })
		if count != 12 || (complete && err != nil) || (!complete && !errors.Is(err, io.EOF)) {
			t.Fatal("stream completion", complete, count, err)
		}
		stop := errors.New("stop consuming")
		if err := client.Log(context.Background(), "", 20, false, func(*pb.LogEntry) error { return stop }); !errors.Is(err, stop) {
			t.Fatal("consumer cancellation", err)
		}
		server.Close()
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}
}
