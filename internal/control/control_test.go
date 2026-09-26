package control

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "gyit/internal/gen/gyit/control/v1"
	"gyit/internal/repo"
	"gyit/internal/store"
	"google.golang.org/protobuf/proto"
)

func startServer(t *testing.T, handler Handler) (Client, *Server) {
	t.Helper()
	dir, err := os.MkdirTemp("", "gyit-control-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "control.sock")
	server, err := Listen(context.Background(), path, handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	return Client{Endpoint: path}, server
}

func statusRequest() *pb.Request {
	return &pb.Request{Version: Version, Operation: &pb.Request_Status{Status: &pb.StatusRequest{}}}
}

func TestWireBoundsAndFragments(t *testing.T) {
	req := statusRequest()
	var wire bytes.Buffer
	if err := writeFrame(&wire, req); err != nil {
		t.Fatal(err)
	}
	data := append([]byte(nil), wire.Bytes()...)
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		for _, b := range data {
			if _, err := w.Write([]byte{b}); err != nil {
				return
			}
		}
	}()
	defer r.Close()
	got := new(pb.Request)
	if err := readFrame(r, got); err != nil || !proto.Equal(req, got) {
		t.Fatal(got, err)
	}
	for _, size := range []uint32{0, maxFrame + 1, ^uint32(0)} {
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], size)
		if readFrame(bytes.NewReader(header[:]), new(pb.Request)) == nil {
			t.Fatal("accepted invalid length", size)
		}
	}
	for _, end := range []int{1, 3, len(data) - 1} {
		if readFrame(bytes.NewReader(data[:end]), new(pb.Request)) == nil {
			t.Fatal("accepted truncated frame")
		}
	}
}

func TestSocketConcurrencyValidationAndCleanup(t *testing.T) {
	c := New(nil, &repo.Snapshot{SHA: strings.Repeat("a", 40), Tree: "tree"})
	client, server := startServer(t, c.Handle)
	info, err := os.Stat(client.Endpoint)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("socket permissions", info, err)
	}
	idle, err := net.Dial("unix", client.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	if _, err := idle.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s, err := client.Status(ctx)
	if err != nil || s.Sha != c.Current().SHA {
		t.Fatal("idle client blocked status", s, err)
	}
	for _, tc := range []struct {
		req  *pb.Request
		code pb.ErrorCode
	}{
		{&pb.Request{Version: 100}, pb.ErrorCode_ERROR_CODE_UNSUPPORTED_VERSION},
		{&pb.Request{Version: Version}, pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
	} {
		_, err := client.call(ctx, tc.req)
		var remote *RemoteError
		if !errors.As(err, &remote) || remote.Code != tc.code {
			t.Fatal("typed error", err)
		}
	}
	conn, err := net.Dial("unix", client.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	_, _ = conn.Write([]byte{0xff, 0xff, 0xff, 0xff})
	response := new(pb.Response)
	if err := readFrame(conn, response); err != nil || response.GetError().GetCode() != pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
		t.Fatal("oversized request", response, err)
	}
	server.Close()
	if _, err := os.Stat(client.Endpoint); !os.IsNotExist(err) {
		t.Fatal("socket not removed", err)
	}
	_ = idle.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := idle.Read(make([]byte, 1)); err == nil {
		t.Fatal("idle connection left open")
	}
}

func TestSocketPathProtection(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "control.sock")
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if server, err := Listen(context.Background(), path, nil); err == nil {
		server.Close()
		t.Fatal("replaced existing path")
	}
	if b, _ := os.ReadFile(path); string(b) != "keep" {
		t.Fatal("existing file modified")
	}
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if server, err := Listen(context.Background(), filepath.Join(dir, "new.sock"), nil); err == nil {
		server.Close()
		t.Fatal("accepted public directory")
	}
}

func TestCloseCancelsHandlerAndClientDeadline(t *testing.T) {
	entered := make(chan struct{}, 2)
	client, server := startServer(t, func(ctx context.Context, _ *pb.Request) *pb.Response {
		entered <- struct{}{}
		<-ctx.Done()
		return contextFailure(ctx.Err())
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := client.Status(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("client deadline", err)
	}
	<-entered
	done := make(chan struct{})
	go func() { server.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel handler")
	}
}

func TestPublishedVersionSwitch(t *testing.T) {
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
		b, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git: %s: %v", b, err)
		}
		return strings.TrimSpace(string(b))
	}
	commit := func(contents string) string {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "file"), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		git("add", ".")
		git("commit", "-qm", "fixture")
		return git("rev-parse", "HEAD")
	}
	git("init", "-q")
	first := commit("first")
	git("branch", "-M", "main")
	storage, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	publish := func() {
		t.Helper()
		if _, err := repo.Import(context.Background(), storage, repo.ImportOptions{Repo: dir}); err != nil {
			t.Fatal(err)
		}
	}
	publish()
	repository, err := repo.New(storage, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := repository.Open(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	c := New(repository, initial)
	client, _ := startServer(t, c.Handle)
	second := commit("second")
	publish()
	ctx := context.Background()
	if s, err := client.Status(ctx); err != nil || s.Sha != first {
		t.Fatal("publication switched mounted reader", s, err)
	}
	for _, tc := range []struct {
		sha  string
		code pb.ErrorCode
	}{
		{"missing-branch", pb.ErrorCode_ERROR_CODE_NOT_FOUND},
		{strings.Repeat("g", 40), pb.ErrorCode_ERROR_CODE_NOT_FOUND},
		{strings.Repeat("0", 40), pb.ErrorCode_ERROR_CODE_NOT_FOUND},
	} {
		_, err := client.Switch(ctx, tc.sha)
		var remote *RemoteError
		if !errors.As(err, &remote) || remote.Code != tc.code {
			t.Fatal("switch error", err)
		}
		if c.Current() != initial {
			t.Fatal("failed switch changed current snapshot")
		}
	}
	if s, err := client.Switch(ctx, strings.ToUpper(second)); err != nil || s.Sha != second || c.Current().SHA != second {
		t.Fatal("switch", s, err)
	}
	for _, tc := range []struct{ revision, want string }{
		{"HEAD~1", first}, {"-", second}, {"@{-1}", first}, {"main", second}, {second[:8], second},
	} {
		if s, err := client.Switch(ctx, tc.revision); err != nil || s.Sha != tc.want {
			t.Fatal("revision switch", tc, s, err)
		}
	}
	if initial.SHA != first {
		t.Fatal("old snapshot mutated")
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sha := first
			if i%2 == 0 {
				sha = second
			}
			if s, err := client.Switch(ctx, sha); err != nil || s.Sha != sha {
				t.Errorf("concurrent switch: %v %v", s, err)
			}
		}(i)
	}
	wg.Wait()
}
