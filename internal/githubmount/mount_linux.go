//go:build linux

// Package githubmount adapts the lazy GitHub namespace to Linux FUSE.
package githubmount

import (
	"context"
	"errors"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"gyit/internal/githubfs"
	"gyit/internal/macfs"
	"gyit/internal/repo"
)

type node struct {
	fs.Inode
	source githubfs.Namespace
	path   string
}

func errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if repo.IsNotFound(err) {
		return syscall.ENOENT
	}
	var e syscall.Errno
	if errors.As(err, &e) {
		return e
	}
	if errors.Is(err, context.Canceled) {
		return syscall.EINTR
	}
	return syscall.EIO
}
func mode(e repo.Entry) uint32 {
	switch e.Mode {
	case 0040000, 0160000:
		return syscall.S_IFDIR | 0555
	case 0120000:
		return syscall.S_IFLNK | 0777
	default:
		return syscall.S_IFREG | (e.Mode & 0555)
	}
}
func attr(a *fuse.Attr, e repo.Entry, path string) {
	a.Uid = uint32(os.Getuid())
	a.Gid = uint32(os.Getgid())
	a.Ino = macfs.Inode(path)
	a.Mode = mode(e)
	a.Size = uint64(e.Size)
	a.Nlink = 1
	a.Blksize = 4096
	if a.Mode&syscall.S_IFMT == syscall.S_IFDIR {
		a.Nlink = 2
	}
}
func (n *node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return nil, syscall.EINVAL
	}
	p := name
	if n.path != "" {
		p = n.path + "/" + name
	}
	e, err := n.source.Lookup(ctx, p)
	if err != nil {
		return nil, errno(err)
	}
	attr(&out.Attr, e, p)
	return n.NewInode(ctx, &node{source: n.source, path: p}, fs.StableAttr{Mode: mode(e), Ino: macfs.Inode(p)}), 0
}
func (n *node) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	e, err := n.source.Lookup(ctx, n.path)
	if err != nil {
		return errno(err)
	}
	attr(&out.Attr, e, n.path)
	return 0
}

type directory struct {
	ctx     context.Context
	n       *node
	after   string
	entries []fuse.DirEntry
	err     syscall.Errno
	done    bool
}

func (d *directory) HasNext() bool {
	if len(d.entries) > 0 || d.err != 0 {
		return true
	}
	if d.done {
		return false
	}
	entries, err := d.n.source.ReadDir(d.ctx, d.n.path, d.after, 128)
	if err != nil {
		d.err = errno(err)
		return true
	}
	d.done = len(entries) < 128
	for _, e := range entries {
		p := e.Name
		if d.n.path != "" {
			p = d.n.path + "/" + e.Name
		}
		d.entries = append(d.entries, fuse.DirEntry{Name: e.Name, Mode: mode(e), Ino: macfs.Inode(p)})
		d.after = e.Name
	}
	return len(d.entries) > 0
}
func (d *directory) Next() (fuse.DirEntry, syscall.Errno) {
	if d.err != 0 {
		e := d.err
		d.err = 0
		d.done = true
		return fuse.DirEntry{}, e
	}
	e := d.entries[0]
	d.entries = d.entries[1:]
	return e, 0
}
func (d *directory) Close() { d.entries = nil; d.done = true }
func (n *node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return &directory{ctx: ctx, n: n}, 0
}
func (n *node) Open(_ context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_ACCMODE != syscall.O_RDONLY {
		return nil, 0, syscall.EROFS
	}
	return nil, fuse.FOPEN_DIRECT_IO, 0
}
func (n *node) Read(ctx context.Context, _ fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	count, err := n.source.Read(ctx, n.path, dest, off)
	if err != nil {
		return nil, errno(err)
	}
	return fuse.ReadResultData(dest[:count]), 0
}
func (n *node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	e, err := n.source.Lookup(ctx, n.path)
	if err != nil {
		return nil, errno(err)
	}
	if e.Mode != 0120000 || e.Size > 1<<20 {
		return nil, syscall.EINVAL
	}
	b := make([]byte, e.Size)
	count, err := n.source.Read(ctx, n.path, b, 0)
	return b[:count], errno(err)
}
func Run(ctx context.Context, source *githubfs.FS, mountpoint string) error {
	zero := time.Duration(0)
	server, err := fs.Mount(mountpoint, &node{source: githubfs.Namespace{FS: source}}, &fs.Options{MountOptions: fuse.MountOptions{Options: []string{"default_permissions"}, Name: "gyit", FsName: "github.com", MaxBackground: 16}, EntryTimeout: &zero, AttrTimeout: &zero, NegativeTimeout: &zero})
	if err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = server.Unmount()
		case <-done:
		}
	}()
	server.Wait()
	close(done)
	return nil
}

func (n *node) Setattr(ctx context.Context, h fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	const times = fuse.FATTR_ATIME | fuse.FATTR_MTIME | fuse.FATTR_ATIME_NOW | fuse.FATTR_MTIME_NOW | fuse.FATTR_CTIME
	if in.Valid & ^uint32(times|fuse.FATTR_FH|fuse.FATTR_LOCKOWNER) != 0 || in.Valid&times == 0 {
		return syscall.EROFS
	}
	if err := n.source.Retry(n.path); err != nil {
		return errno(err)
	}
	return n.Getattr(ctx, h, out)
}
