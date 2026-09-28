package controlfuse

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"gyit/internal/control"
	pb "gyit/internal/gen/gyit/control/v1"
)

type delayedRoot struct {
	fs.Inode
	server *control.FileServer
}

func (n *delayedRoot) OnAdd(ctx context.Context) {
	n.AddChild("control", n.NewPersistentInode(ctx, &delayedNode{server: n.server}, fs.StableAttr{Mode: syscall.S_IFREG}), false)
}

type delayedNode struct {
	fs.Inode
	server *control.FileServer
}

func (n *delayedNode) Open(context.Context, uint32) (fs.FileHandle, uint32, syscall.Errno) {
	conn, err := n.server.Open()
	if err != nil {
		return nil, 0, syscall.EIO
	}
	return NewHandle(conn), fuse.FOPEN_DIRECT_IO, 0
}
func (n *delayedNode) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFREG | 0600
	out.Nlink = 1
	return 0
}

func TestMountedControlResumesAfterDelayedFrame(t *testing.T) {
	if os.Getenv("GYIT_FUSE_TEST") != "1" {
		t.Skip("set GYIT_FUSE_TEST=1 on a Linux FUSE host")
	}
	service := control.NewFileServer(t.Context(), func(ctx context.Context, _ *pb.Request, send func(*pb.Response) error) error {
		for i := 0; i < 2; i++ {
			if i > 0 {
				select {
				case <-time.After(100 * time.Millisecond):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if err := send(&pb.Response{Version: control.Version, Result: &pb.Response_LogEntry{LogEntry: &pb.LogEntry{Sha: "1234567890"}}}); err != nil {
				return err
			}
		}
		return send(&pb.Response{Version: control.Version, Result: &pb.Response_LogEnd{LogEnd: &pb.LogEnd{}}})
	})
	defer service.Close()
	mountpoint := t.TempDir()
	mount, err := fs.Mount(mountpoint, &delayedRoot{server: service}, &fs.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer mount.Unmount()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	count := 0
	err = (control.Client{Endpoint: "fuse:" + mountpoint + "/control"}).Log(ctx, "", 2, false, func(*pb.LogEntry) error { count++; return nil })
	if err != nil || count != 2 {
		t.Fatalf("delayed log delivered %d entries: %v", count, err)
	}
}
