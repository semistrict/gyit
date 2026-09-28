//go:build linux

package mount

import (
	"context"
	"fmt"
	"os"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"gyit/internal/control"
	"gyit/internal/controlfuse"
	"gyit/internal/repo"
)

func validateControlPath(ctx context.Context, snapshot *repo.Snapshot) error {
	_, err := snapshot.Lookup(ctx, snapshot.Tree, control.ControlFileName)
	if repo.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: %s is reserved for mount control", repo.ErrInvalidRevision, control.ControlFileName)
}

type controlNode struct {
	fs.Inode
	readOnlyMutations
	server *control.FileServer
}

func (n *controlNode) attributes(out *fuse.Attr) {
	out.Ino, out.Mode, out.Nlink = 2, syscall.S_IFREG|0600, 1
	out.Uid, out.Gid = uint32(os.Getuid()), uint32(os.Getgid())
}
func (n *controlNode) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	n.attributes(&out.Attr)
	return 0
}
func (n *controlNode) Open(_ context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_ACCMODE != syscall.O_RDWR || flags&(syscall.O_TRUNC|syscall.O_APPEND) != 0 {
		return nil, 0, syscall.EACCES
	}
	conn, err := n.server.Open()
	if err != nil {
		return nil, 0, syscall.EMFILE
	}
	return controlfuse.NewHandle(conn), fuse.FOPEN_DIRECT_IO, 0
}

type readOnlyMutations = controlfuse.ReadOnlyMutations
