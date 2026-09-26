package control

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	pb "gyit/internal/gen/gyit/control/v1"
)

func TestControlFileStreamsResponses(t *testing.T) {
	server := NewFileServer(t.Context(), func(ctx context.Context, req *pb.Request, send func(*pb.Response) error) error {
		for i := 0; i < 8; i++ {
			data := []byte(fmt.Sprintf("chunk %d: ", i) + strings.Repeat("x", 32000))
			if err := send(&pb.Response{Version: Version, Result: &pb.Response_DiffChunk{DiffChunk: &pb.DiffChunk{Data: data}}}); err != nil {
				return err
			}
		}
		return send(&pb.Response{Version: Version, Result: &pb.Response_StreamEnd{StreamEnd: &pb.StreamEnd{}}})
	})
	defer server.Close()
	conn, err := server.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if err := writeFrame(conn, statusRequest()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		resp := new(pb.Response)
		if err := readFrame(conn, resp); err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("chunk %d: ", i) + strings.Repeat("x", 32000)
		if string(resp.GetDiffChunk().GetData()) != want {
			t.Fatalf("stream chunk %d differs", i)
		}
	}
	end := new(pb.Response)
	if err := readFrame(conn, end); err != nil || end.GetStreamEnd() == nil {
		t.Fatalf("stream end: %v %v", end, err)
	}
}

func TestControlFileCloseCancelsBlockedResponse(t *testing.T) {
	for _, closeServer := range []bool{false, true} {
		t.Run(fmt.Sprint("server=", closeServer), func(t *testing.T) {
			started, stopped := make(chan struct{}), make(chan struct{})
			server := NewFileServer(t.Context(), func(ctx context.Context, _ *pb.Request, send func(*pb.Response) error) error {
				defer close(stopped)
				close(started)
				err := send(&pb.Response{Version: Version, Result: &pb.Response_StreamEnd{StreamEnd: &pb.StreamEnd{}}})
				<-ctx.Done()
				return err
			})
			defer server.Close()
			conn, err := server.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
			if err := writeFrame(conn, statusRequest()); err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("request not started")
			}
			if closeServer {
				server.Close()
			} else {
				_ = conn.Close()
			}
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("closing did not cancel handler")
			}
		})
	}
}

func TestControlFileOpenBudgetAndShutdown(t *testing.T) {
	server := NewFileServer(t.Context(), nil)
	defer server.Close()
	var handles []net.Conn
	for range 16 {
		conn, err := server.Open()
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, conn)
		defer conn.Close()
	}
	if extra, err := server.Open(); err == nil {
		extra.Close()
		t.Fatal("accepted more than 16 active control requests")
	}
	server.Close()
	if extra, err := server.Open(); err == nil {
		extra.Close()
		t.Fatal("opened a closed server")
	}
	for _, conn := range handles {
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		var b [1]byte
		if _, err := conn.Read(b[:]); err == nil {
			t.Fatal("idle handle remains open after shutdown")
		}
	}
}
