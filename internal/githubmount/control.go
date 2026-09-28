//go:build linux || gyit_virtiofs

package githubmount

import (
	"context"
	"fmt"
	"net"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/protobuf/proto"
	"gyit/internal/control"
	"gyit/internal/controlfuse"
	pb "gyit/internal/gen/gyit/control/v1"
	"gyit/internal/macfs"
	"gyit/internal/repo"
)

// The host's Unix socket cannot be dialed by a virtio-fs guest. Advertise a
// mount-relative control file and proxy its bounded protobuf stream on the host.
func (n *node) controlSocket(ctx context.Context) (string, error) {
	if strings.Count(n.path, "/") != 2 {
		return "", nil
	}
	if _, err := n.source.Lookup(ctx, n.path+"/"+control.ControlFileName); err == nil {
		return "", fmt.Errorf("%s is reserved for mount control", control.ControlFileName)
	} else if !repo.IsNotFound(err) {
		return "", err
	}
	b, err := n.source.Endpoint(ctx, n.path)
	if err != nil || b == nil {
		return "", err
	}
	var endpoint pb.MountEndpoint
	if err = proto.Unmarshal(b, &endpoint); err != nil {
		return "", err
	}
	if endpoint.Version != control.Version || endpoint.Socket == "" {
		return "", fmt.Errorf("invalid host control endpoint")
	}
	return endpoint.Socket, nil
}
func (n *node) controlAttribute(ctx context.Context) ([]byte, error) {
	socket, err := n.controlSocket(ctx)
	if err != nil || socket == "" {
		return nil, err
	}
	return proto.Marshal(&pb.MountEndpoint{Version: control.Version, ControlFile: control.ControlFileName})
}
func (n *node) controlLookup(ctx context.Context, path string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	socket, err := n.controlSocket(ctx)
	if err != nil {
		return nil, errno(err)
	}
	if socket == "" {
		return nil, syscall.ENOENT
	}
	child := &commandNode{socket: socket, owner: n.owner, ino: macfs.Inode(path)}
	child.attributes(&out.Attr)
	return n.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG, Ino: child.ino}), 0
}

type commandNode struct {
	fs.Inode
	controlfuse.ReadOnlyMutations
	socket string
	owner  fuse.Owner
	ino    uint64
}

func (n *commandNode) attributes(out *fuse.Attr) {
	out.Ino = n.ino
	out.Mode = syscall.S_IFREG | 0600
	out.Nlink = 1
	out.Owner = n.owner
}
func (n *commandNode) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	n.attributes(&out.Attr)
	return 0
}
func (n *commandNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_ACCMODE != syscall.O_RDWR || flags&(syscall.O_TRUNC|syscall.O_APPEND) != 0 {
		return nil, 0, syscall.EACCES
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", n.socket)
	if err != nil {
		return nil, 0, errno(err)
	}
	return controlfuse.NewHandle(conn), fuse.FOPEN_DIRECT_IO, 0
}
