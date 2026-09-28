//go:build linux

package githubmount

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"gyit/internal/githubfs"
	"os"
	"time"
)

func Run(ctx context.Context, source *githubfs.FS, mountpoint string) error {
	zero := time.Duration(0)
	root := &node{owner: fuse.Owner{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())}, source: githubfs.Namespace{FS: source}, immutableTTL: time.Second}
	server, err := fs.Mount(mountpoint, root, &fs.Options{MountOptions: fuse.MountOptions{Options: []string{"default_permissions"}, Name: "gyit", FsName: "github.com", MaxBackground: 16}, EntryTimeout: &zero, AttrTimeout: &zero, NegativeTimeout: &zero})
	if err != nil {
		return err
	}
	stop := root.watchChanges(ctx)
	defer stop()
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
