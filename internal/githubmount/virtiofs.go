//go:build gyit_virtiofs

package githubmount

import (
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"gyit/internal/githubfs"
	"os"
	"time"
)

// Protocol exposes the same namespace as the Linux mount without /dev/fuse.
// The caller supplies Virtio transport and owns the namespace lifetime.
func Protocol(source *githubfs.FS, immutableTTL time.Duration) *fuse.ProtocolServer {
	zero := time.Duration(0)
	opts := &fs.Options{EntryTimeout: &zero, AttrTimeout: &zero, NegativeTimeout: &zero}
	return fuse.NewProtocolServer(fs.NewNodeFS(&node{owner: fuse.Owner{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())}, source: githubfs.Namespace{FS: source}, immutableTTL: immutableTTL}, opts), &opts.MountOptions)
}
