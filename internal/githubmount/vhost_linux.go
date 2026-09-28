//go:build linux && gyit_virtiofs

package githubmount

import (
	"os"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/virtiofs"
	"gyit/internal/githubfs"
)

// ServeVirtio serves the ordinary namespace directly to a Linux virtio-fs guest,
// without mounting a host FUSE filesystem. The caller owns source and socket.
func ServeVirtio(source *githubfs.FS, socket string, ttl time.Duration) {
	zero := time.Duration(0)
	opts := &fs.Options{EntryTimeout: &zero, AttrTimeout: &zero, NegativeTimeout: &zero}
	opts.ExtraCapabilities |= fuse.CAP_NO_OPENDIR_SUPPORT
	root := &node{owner: fuse.Owner{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())}, source: githubfs.Namespace{FS: source}, immutableTTL: ttl}
	virtiofs.ServeFS(socket, statelessDirectories{fs.NewNodeFS(root, opts)}, &opts.MountOptions)
}
