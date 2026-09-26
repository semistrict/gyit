package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gyit/internal/control"
	pb "gyit/internal/gen/gyit/control/v1"
)

func TestRequiresRecognizedSubcommand(t *testing.T) {
	for _, args := range [][]string{nil, {"--store", "/tmp/store"}, {"unknown"}, {"checkuot", "main"}, {"unknown", "--store", "/tmp/store"}} {
		err := run(t.Context(), args)
		if err == nil {
			t.Fatalf("accepted missing or unknown subcommand: %v", args)
		}
		want := "unknown subcommand"
		if len(args) == 0 {
			want = "usage: gyit"
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("%v: expected %q, got %v", args, want, err)
		}
	}
}

func TestCheckoutDispatchesToMountedRepository(t *testing.T) {
	dir, err := os.MkdirTemp("", "gyit-checkout-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	requests := make(chan string, 1)
	socket := filepath.Join(dir, "control.sock")
	server, err := control.Listen(t.Context(), socket, func(_ context.Context, req *pb.Request) *pb.Response {
		requests <- req.GetSwitch().GetRevision()
		return &pb.Response{Version: control.Version, Result: &pb.Response_Snapshot{Snapshot: &pb.Snapshot{Sha: strings.Repeat("a", 40)}}}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	for _, revision := range []string{"main", "HEAD~3", "-", "0a885f68d0e90dcef77e575b09104df7263e2aca"} {
		if err := run(t.Context(), []string{"checkout", "--socket", socket, revision}); err != nil {
			t.Fatal(err)
		}
		if got := <-requests; got != revision {
			t.Fatalf("revision = %q, want %q", got, revision)
		}
	}
}

func TestMountRejectsLegacyStoreMode(t *testing.T) {
	for _, args := range [][]string{{"mount", "--store", "/tmp/store"}, {"mount", "--sha", "HEAD"}} {
		if err := run(t.Context(), args); err == nil {
			t.Fatalf("accepted legacy mount flags: %v", args)
		}
	}
}
