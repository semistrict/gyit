//go:build linux

package mount

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"gat/internal/control"
	"gat/internal/repo"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
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
	return &controlHandle{conn: conn}, fuse.FOPEN_DIRECT_IO, 0
}

type controlHandle struct {
	conn            net.Conn
	readMu, writeMu sync.Mutex
	written         int64
}

func (f *controlHandle) Read(ctx context.Context, dest []byte, _ int64) (fuse.ReadResult, syscall.Errno) {
	f.readMu.Lock()
	defer f.readMu.Unlock()
	stop := context.AfterFunc(ctx, func() { _ = f.conn.Close() })
	defer stop()
	_ = f.conn.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
	n, err := f.conn.Read(dest)
	if n > 0 || errors.Is(err, io.EOF) {
		return fuse.ReadResultData(dest[:n]), 0
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return nil, syscall.EAGAIN
	}
	if err != nil {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}
func (f *controlHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	if off != f.written {
		return 0, syscall.EINVAL
	}
	if len(data) > control.MaxFrameSize+4 || off > int64(control.MaxFrameSize+4-len(data)) {
		return 0, syscall.EFBIG
	}
	stop := context.AfterFunc(ctx, func() { _ = f.conn.Close() })
	defer stop()
	_ = f.conn.SetWriteDeadline(time.Now().Add(25 * time.Millisecond))
	n, err := f.conn.Write(data)
	f.written += int64(n)
	if n > 0 {
		return uint32(n), 0
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return 0, syscall.EAGAIN
	}
	if err != nil {
		return 0, syscall.EPIPE
	}
	return uint32(n), 0
}
func (f *controlHandle) Release(context.Context) syscall.Errno { _ = f.conn.Close(); return 0 }

// Repository data and inode metadata remain immutable even though the control
// file accepts protocol writes. In particular go-fuse defaults Unlink/Rmdir to
// success, so these must not rely on its defaults once the kernel ro flag is gone.
type readOnlyMutations struct{}

func (readOnlyMutations) Setattr(context.Context, fs.FileHandle, *fuse.SetAttrIn, *fuse.AttrOut) syscall.Errno {
	return syscall.EROFS
}
func (readOnlyMutations) Setxattr(context.Context, string, []byte, uint32) syscall.Errno {
	return syscall.EROFS
}
func (readOnlyMutations) Removexattr(context.Context, string) syscall.Errno { return syscall.EROFS }
func (readOnlyMutations) Unlink(context.Context, string) syscall.Errno      { return syscall.EROFS }
func (readOnlyMutations) Rmdir(context.Context, string) syscall.Errno       { return syscall.EROFS }
func (readOnlyMutations) Rename(context.Context, string, fs.InodeEmbedder, string, uint32) syscall.Errno {
	return syscall.EROFS
}
func (readOnlyMutations) Mkdir(context.Context, string, uint32, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EROFS
}
func (readOnlyMutations) Mknod(context.Context, string, uint32, uint32, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EROFS
}
func (readOnlyMutations) Link(context.Context, fs.InodeEmbedder, string, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EROFS
}
func (readOnlyMutations) Symlink(context.Context, string, string, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EROFS
}
