//go:build linux

// Package mount implements a read-only FUSE view with lazy path resolution.
package mount

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"syscall"
	"time"

	"gat/internal/control"
	pb "gat/internal/gen/gat/control/v1"
	"gat/internal/repo"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/protobuf/proto"
)

type node struct {
	fs.Inode
	state           *control.Controller
	path            string
	controlEndpoint []byte
}
type file struct {
	snapshot *repo.Snapshot
	entry    repo.Entry
}

func inode(path string) uint64 {
	if path == "" {
		return 1
	}
	h := sha256.Sum256([]byte(path))
	n := binary.LittleEndian.Uint64(h[:8])
	if n < 2 {
		n += 2
	}
	return n
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
func errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if repo.IsNotFound(err) {
		return syscall.ENOENT
	}
	if errors.Is(err, context.Canceled) {
		return syscall.EINTR
	}
	return syscall.EIO
}
func attr(out *fuse.Attr, e repo.Entry, path string) {
	out.Ino = inode(path)
	out.Mode = mode(e)
	out.Size = uint64(e.Size)
	out.Nlink = 1
	out.Blksize = 4096
	out.Blocks = (out.Size + 511) / 512
	if out.Mode&syscall.S_IFMT == syscall.S_IFDIR {
		out.Nlink = 2
	}
}

// Getxattr advertises only the mount root's endpoint, without repository reads.
func (n *node) Getxattr(_ context.Context, name string, dest []byte) (uint32, syscall.Errno) {
	if n.path != "" || name != control.EndpointAttribute || len(n.controlEndpoint) == 0 {
		return 0, syscall.ENODATA
	}
	size := uint32(len(n.controlEndpoint))
	if len(dest) == 0 {
		return size, 0
	}
	if len(dest) < len(n.controlEndpoint) {
		return size, syscall.ERANGE
	}
	copy(dest, n.controlEndpoint)
	return size, 0
}

func (n *node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	s := n.state.Current()
	parent, err := s.Resolve(ctx, n.path)
	if err != nil {
		return nil, errno(err)
	}
	if parent.Mode == 0160000 {
		return nil, syscall.ENOENT
	}
	if parent.Mode != 0040000 {
		return nil, syscall.ENOTDIR
	}
	e, err := s.Lookup(ctx, parent.OID, name)
	if err != nil {
		return nil, errno(err)
	}
	path := name
	if n.path != "" {
		path = n.path + "/" + name
	}
	attr(&out.Attr, e, path)
	child := &node{state: n.state, path: path}
	return n.NewInode(ctx, child, fs.StableAttr{Mode: mode(e) & syscall.S_IFMT, Ino: inode(path)}), 0
}
func (n *node) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// go-fuse may supply any open handle even for a path stat with no Fh.
	// Its NodeGetattrer API does not expose that distinction. Inode attributes
	// therefore follow the selected path; only Read uses the handle's pinned
	// snapshot. Using the supplied handle here makes lstat depend on open order.
	e, err := n.state.Current().Resolve(ctx, n.path)
	if err != nil {
		return errno(err)
	}
	attr(&out.Attr, e, n.path)
	return 0
}
func (n *node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_TRUNC|syscall.O_APPEND) != 0 {
		return nil, 0, syscall.EROFS
	}
	s := n.state.Current()
	e, err := s.Resolve(ctx, n.path)
	if err != nil {
		return nil, 0, errno(err)
	}
	if mode(e)&syscall.S_IFMT != syscall.S_IFREG {
		return nil, 0, syscall.EISDIR
	}
	// Direct I/O prevents an old generation's kernel page cache being reused
	// under the same inode after switching. The bounded repository cache is shared.
	return &file{snapshot: s, entry: e}, fuse.FOPEN_DIRECT_IO, 0
}
func (f *file) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, err := f.snapshot.ReadAt(ctx, f.entry.OID, dest, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, errno(err)
	}
	return fuse.ReadResultData(dest[:n]), 0
}
func (n *node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	s := n.state.Current()
	e, err := s.Resolve(ctx, n.path)
	if err != nil {
		return nil, errno(err)
	}
	if e.Mode != 0120000 {
		return nil, syscall.EINVAL
	}
	if e.Size > 4096 {
		return nil, syscall.ENAMETOOLONG
	}
	b := make([]byte, e.Size)
	_, err = s.ReadAt(ctx, e.OID, b, 0)
	return b, errno(err)
}

type directory struct {
	snapshot          *repo.Snapshot
	tree, after, path string
	entries           []repo.Entry
	done              bool
	err               error
}

func (d *directory) HasNext() bool {
	if len(d.entries) > 0 || d.err != nil {
		return true
	}
	if d.done {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	d.entries, d.err = d.snapshot.ReadDir(ctx, d.tree, d.after, 128)
	if len(d.entries) < 128 {
		d.done = true
	}
	return len(d.entries) > 0 || d.err != nil
}
func (d *directory) Next() (fuse.DirEntry, syscall.Errno) {
	if !d.HasNext() {
		return fuse.DirEntry{}, syscall.ENOENT
	}
	if d.err != nil {
		err := d.err
		d.err = nil
		d.done = true
		return fuse.DirEntry{}, errno(err)
	}
	e := d.entries[0]
	d.entries = d.entries[1:]
	d.after = e.Name
	path := e.Name
	if d.path != "" {
		path = d.path + "/" + e.Name
	}
	return fuse.DirEntry{Name: e.Name, Mode: mode(e), Ino: inode(path)}, 0
}
func (d *directory) Close() { d.entries = nil; d.done = true }
func (n *node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	s := n.state.Current()
	e, err := s.Resolve(ctx, n.path)
	if err != nil {
		return nil, errno(err)
	}
	if e.Mode == 0160000 {
		return fs.NewListDirStream(nil), 0
	}
	if e.Mode != 0040000 {
		return nil, syscall.ENOTDIR
	}
	return &directory{snapshot: s, tree: e.OID, path: n.path}, 0
}

// Run serves until unmount or cancellation. Its protobuf control socket supports
// status and switching to an imported commit without materializing a checkout.
func Run(ctx context.Context, r *repo.Repository, sha, mountpoint, socket string) error {
	s, err := r.OpenRevision(ctx, sha, "")
	if err != nil {
		return err
	}
	socket, err = filepath.Abs(socket)
	if err != nil {
		return err
	}
	endpoint, err := proto.Marshal(&pb.MountEndpoint{Version: control.Version, Socket: socket})
	if err != nil {
		return err
	}
	st := control.New(r, s)
	zero := time.Duration(0)
	server, err := fs.Mount(mountpoint, &node{state: st, controlEndpoint: endpoint}, &fs.Options{
		MountOptions: fuse.MountOptions{Options: []string{"ro", "default_permissions"}, Name: "gat", FsName: "gat", MaxBackground: 16},
		EntryTimeout: &zero, AttrTimeout: &zero, NegativeTimeout: &zero,
		RootStableAttr: &fs.StableAttr{Ino: 1},
	})
	if err != nil {
		return err
	}
	defer server.Unmount()
	controlServer, err := control.ListenStream(ctx, socket, st.Serve)
	if err != nil {
		return fmt.Errorf("control socket: %w", err)
	}
	defer controlServer.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = server.Unmount()
		case <-done:
		}
	}()
	server.Wait()
	return nil
}
