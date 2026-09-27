//go:build linux || gyit_virtiofs

// Package githubmount adapts the lazy GitHub namespace to the FUSE protocol.
package githubmount

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
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
	source       githubfs.Namespace
	path         string
	owner        fuse.Owner
	immutableTTL time.Duration
	item         atomic.Pointer[worktreeItem]
}

type worktreeItem struct {
	snapshot *repo.Snapshot
	entry    repo.Entry
}

func (n *node) worktree(ctx context.Context) (*worktreeItem, error) {
	if item := n.item.Load(); item != nil {
		return item, nil
	}
	snapshot, entry, err := n.source.ReadyWorktree(ctx, n.path)
	if err != nil || snapshot == nil {
		return nil, err
	}
	item := &worktreeItem{snapshot: snapshot, entry: entry}
	n.item.CompareAndSwap(nil, item)
	return n.item.Load(), nil
}

// Namespace directories retain their identity and attributes while children are
// added. Cache positive lookups, never their changing directory listings or
// negative names. The setup NOTICE remains uncached until publication.
func (n *node) stableMetadata(path string, e repo.Entry, generation uint64) bool {
	return (e.Mode == 0040000 && strings.Count(path, "/") <= 2) ||
		(generation > 1 && generation == n.source.Generation(path))
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
func attr(a *fuse.Attr, e repo.Entry, path string, owner fuse.Owner) {
	a.Owner = owner
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
	generation := n.source.Generation(p)
	var e repo.Entry
	item, err := n.worktree(ctx)
	if err != nil {
		return nil, errno(err)
	}
	if item != nil && !(strings.Count(n.path, "/") == 2 && name == ".git") {
		if item.entry.Mode == 0160000 {
			return nil, syscall.ENOENT
		}
		e, err = item.snapshot.Lookup(ctx, item.entry.OID, name)
	} else {
		e, err = n.source.Lookup(ctx, p)
	}
	if err != nil {
		return nil, errno(err)
	}
	return n.child(ctx, p, e, generation, out), 0
}

func (n *node) child(ctx context.Context, p string, e repo.Entry, generation uint64, out *fuse.EntryOut) *fs.Inode {
	attr(&out.Attr, e, p, n.owner)
	if n.immutableTTL > 0 && n.stableMetadata(p, e, generation) {
		out.SetEntryTimeout(n.immutableTTL)
		out.SetAttrTimeout(n.immutableTTL)
	}
	child := &node{source: n.source, path: p, immutableTTL: n.immutableTTL, owner: n.owner}
	if e.OID != "" {
		if parent, err := n.worktree(ctx); err == nil && parent != nil {
			child.item.Store(&worktreeItem{snapshot: parent.snapshot, entry: e})
		}
	}
	return n.NewInode(ctx, child, fs.StableAttr{Mode: mode(e), Ino: macfs.Inode(p)})
}
func (n *node) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	generation := n.source.Generation(n.path)
	e, err := n.entry(ctx)
	if err != nil {
		return errno(err)
	}
	attr(&out.Attr, e, n.path, n.owner)
	if n.immutableTTL > 0 && n.stableMetadata(n.path, e, generation) {
		out.SetTimeout(n.immutableTTL)
	}
	return 0
}

func (n *node) entry(ctx context.Context) (repo.Entry, error) {
	if err := ctx.Err(); err != nil {
		return repo.Entry{}, err
	}
	item, err := n.worktree(ctx)
	if err != nil {
		return repo.Entry{}, err
	}
	if item != nil {
		return item.entry, nil
	}
	return n.source.Lookup(ctx, n.path)
}

type directory struct {
	ctx        context.Context
	n          *node
	after      string
	entries    []repo.Entry
	last       repo.Entry
	generation uint64
	offset     uint64
	err        syscall.Errno
	done       bool
}

func (d *directory) HasNext() bool {
	if len(d.entries) > 0 || d.err != 0 {
		return true
	}
	if d.done {
		return false
	}
	d.generation = d.n.source.Generation(d.n.path)
	var entries []repo.Entry
	item, err := d.n.worktree(d.ctx)
	if err == nil && item != nil && strings.Count(d.n.path, "/") > 2 {
		if item.entry.Mode == 0160000 {
			entries = nil
		} else {
			entries, err = item.snapshot.ReadDir(d.ctx, item.entry.OID, d.after, 128)
		}
	} else if err == nil {
		entries, err = d.n.source.ReadDir(d.ctx, d.n.path, d.after, 128)
	}
	if err != nil {
		d.err = errno(err)
		return true
	}
	d.done = len(entries) < 128
	d.entries = entries
	if len(entries) > 0 {
		d.after = entries[len(entries)-1].Name
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
	d.last = e
	d.offset++
	p := e.Name
	if d.n.path != "" {
		p = d.n.path + "/" + e.Name
	}
	return fuse.DirEntry{Name: e.Name, Mode: mode(e), Ino: macfs.Inode(p), Off: d.offset}, 0
}
func (d *directory) Close() { d.entries = nil; d.done = true }
func (n *node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return &directory{ctx: ctx, n: n}, 0
}

// READDIRPLUS already has each entry's attributes in its bounded directory
// page. Reuse them instead of resolving every name again from the repo root.
// Go-FUSE serializes calls to a directory handle. Interrupted responses may
// replay older names; those safely fall back to the ordinary lookup path.
func (d *directory) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if name != d.last.Name || d.generation <= 1 || d.generation != d.n.source.Generation(d.n.path) {
		return d.n.Lookup(ctx, name, out)
	}
	p := name
	if d.n.path != "" {
		p = d.n.path + "/" + name
	}
	return d.n.child(ctx, p, d.last, d.generation, out), 0
}
func (n *node) OpendirHandle(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return &directory{ctx: ctx, n: n}, 0, 0
}
func (d *directory) Readdirent(ctx context.Context) (*fuse.DirEntry, syscall.Errno) {
	d.ctx = ctx
	if !d.HasNext() {
		return nil, 0
	}
	e, err := d.Next()
	return &e, err
}
func (d *directory) Seekdir(ctx context.Context, off uint64) syscall.Errno {
	*d = directory{ctx: ctx, n: d.n}
	for d.offset < off {
		if !d.HasNext() {
			return syscall.EINVAL
		}
		if _, err := d.Next(); err != 0 {
			return err
		}
	}
	return 0
}
func (d *directory) Releasedir(context.Context, uint32) { d.Close() }

func (n *node) Open(_ context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_ACCMODE != syscall.O_RDONLY {
		return nil, 0, syscall.EROFS
	}
	// Native Git mmaps pack/index files. These files are immutable for the
	// lifetime of a pinned repository, so kernel caching is safe here.
	parts := strings.SplitN(n.path, "/", 5)
	if len(parts) == 5 && parts[0] == "github.com" && parts[3] == ".git" {
		return nil, fuse.FOPEN_KEEP_CACHE, 0
	}
	return nil, fuse.FOPEN_DIRECT_IO, 0
}
func (n *node) Read(ctx context.Context, _ fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	item, err := n.worktree(ctx)
	if err != nil {
		return nil, errno(err)
	}
	var count int
	if item != nil {
		count, err = item.snapshot.ReadAt(ctx, item.entry.OID, dest, off)
		if err == io.EOF {
			err = nil
		}
	} else {
		count, err = n.source.Read(ctx, n.path, dest, off)
	}
	if err != nil {
		return nil, errno(err)
	}
	return fuse.ReadResultData(dest[:count]), 0
}
func (n *node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	e, err := n.entry(ctx)
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

func (n *node) Getxattr(ctx context.Context, name string, dest []byte) (uint32, syscall.Errno) {
	if name != "user.gyit.control" {
		return 0, syscall.ENODATA
	}
	b, err := n.source.Endpoint(ctx, n.path)
	if err != nil {
		return 0, errno(err)
	}
	if b == nil {
		return 0, syscall.ENODATA
	}
	if len(dest) == 0 {
		return uint32(len(b)), 0
	}
	if len(dest) < len(b) {
		return uint32(len(b)), syscall.ERANGE
	}
	copy(dest, b)
	return uint32(len(b)), 0
}
