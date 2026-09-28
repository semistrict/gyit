//go:build linux || gyit_virtiofs

// Package controlfuse transports the existing protobuf stream through FUSE files.
package controlfuse

import (
	"context"
	"errors"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"gyit/internal/control"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"time"
)

func NewHandle(conn net.Conn) *Handle { return &Handle{conn: conn} }

type Handle struct {
	conn            net.Conn
	readMu, writeMu sync.Mutex
	written         int64
}

func (f *Handle) Read(ctx context.Context, dest []byte, _ int64) (fuse.ReadResult, syscall.Errno) {
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
func (f *Handle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
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
func (f *Handle) Release(context.Context) syscall.Errno { _ = f.conn.Close(); return 0 }

// Repository data and inode metadata remain immutable even though the control
// file accepts protocol writes. In particular go-fuse defaults Unlink/Rmdir to
// success, so these must not rely on its defaults once the kernel ro flag is gone.
type ReadOnlyMutations struct{}

func (ReadOnlyMutations) Setattr(context.Context, fs.FileHandle, *fuse.SetAttrIn, *fuse.AttrOut) syscall.Errno {
	return syscall.EROFS
}
func (ReadOnlyMutations) Setxattr(context.Context, string, []byte, uint32) syscall.Errno {
	return syscall.EROFS
}
func (ReadOnlyMutations) Removexattr(context.Context, string) syscall.Errno { return syscall.EROFS }
func (ReadOnlyMutations) Unlink(context.Context, string) syscall.Errno      { return syscall.EROFS }
func (ReadOnlyMutations) Rmdir(context.Context, string) syscall.Errno       { return syscall.EROFS }
func (ReadOnlyMutations) Rename(context.Context, string, fs.InodeEmbedder, string, uint32) syscall.Errno {
	return syscall.EROFS
}
func (ReadOnlyMutations) Mkdir(context.Context, string, uint32, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EROFS
}
func (ReadOnlyMutations) Mknod(context.Context, string, uint32, uint32, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EROFS
}
func (ReadOnlyMutations) Link(context.Context, fs.InodeEmbedder, string, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EROFS
}
func (ReadOnlyMutations) Symlink(context.Context, string, string, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EROFS
}
