package controlcli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gyit/internal/control"
	pb "gyit/internal/gen/gyit/control/v1"
	"gyit/internal/repo"
)

func TestStatusAndArgumentErrors(t *testing.T) {
	dir, err := os.MkdirTemp("", "gyitcli-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "control.sock")
	c := control.New(nil, &repo.Snapshot{SHA: strings.Repeat("a", 40), Tree: "tree"})
	server, err := control.Listen(context.Background(), socket, c.Handle)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	var stdout, stderr bytes.Buffer
	if err := Run(context.Background(), []string{"status", "--socket", socket}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "HEAD detached at aaaaaaa\nnothing to commit, working tree clean\n" {
		t.Fatal(stdout.String())
	}
	for _, args := range [][]string{
		{}, {"unknown"}, {"status"}, {"status", "--socket", socket, "extra"},
		{"switch", "--socket", socket}, {"status", "--socket", socket, "--timeout", "0s"},
		{"switch", "--socket", socket, "main", "extra"},
		{"switch", "--socket", socket, "--sha", "main", "extra"},
	} {
		stdout.Reset()
		if err := Run(context.Background(), args, &stdout, &stderr); err == nil {
			t.Fatal("accepted invalid command", args)
		}
		if stdout.Len() != 0 {
			t.Fatal("failed command printed success", stdout.String())
		}
	}
}

func TestPositionalRevisionAndInterspersedFlags(t *testing.T) {
	dir, err := os.MkdirTemp("", "gyitcli-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "control.sock")
	requests := make(chan *pb.Request, 1)
	server, err := control.Listen(context.Background(), socket, func(_ context.Context, req *pb.Request) *pb.Response {
		requests <- req
		return &pb.Response{Version: control.Version, Result: &pb.Response_Snapshot{Snapshot: &pb.Snapshot{Sha: strings.Repeat("a", 40)}}}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	for _, revision := range []string{"main", "UpperCase", "v1.0", "HEAD~3^2", "-", "@{-1}", "deadbeef"} {
		for _, args := range [][]string{
			{"checkout", "--socket", socket, revision},
			{"checkout", revision, "--socket", socket},
			{"switch", "--socket", socket, revision},
			{"switch", revision, "--socket", socket},
			{"switch", "--socket=" + socket, "--", revision},
			{"switch", "--socket", socket, "--sha", revision},
		} {
			var stdout, stderr bytes.Buffer
			if err := Run(context.Background(), args, &stdout, &stderr); err != nil {
				t.Fatal(args, err)
			}
			req := <-requests
			if req.GetSwitch().GetRevision() != revision || req.GetSwitch().GetSha() != "" {
				t.Fatal("revision changed in transit", req)
			}
		}
	}
}
