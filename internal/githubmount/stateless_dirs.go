//go:build gyit_virtiofs

package githubmount

import "github.com/hanwen/go-fuse/v2/fuse"

// statelessDirectories uses Linux's NO_OPENDIR_SUPPORT optimization. Directory
// offsets already support replay, so handles need exist only during READDIR.
// This removes two guest round trips per directory without extending any TTL.
// Linux retains CACHE_DIR/KEEP_CACHE and revalidates mtime in this mode.
type statelessDirectories struct{ fuse.RawFileSystem }

func (s statelessDirectories) OpenDir(_ <-chan struct{}, _ *fuse.OpenIn, _ *fuse.OpenOut) fuse.Status {
	return fuse.ENOSYS
}

func (s statelessDirectories) ReadDir(cancel <-chan struct{}, in *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return s.read(cancel, in, out, false)
}

func (s statelessDirectories) ReadDirPlus(cancel <-chan struct{}, in *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return s.read(cancel, in, out, true)
}

func (s statelessDirectories) read(cancel <-chan struct{}, in *fuse.ReadIn, out *fuse.DirEntryList, plus bool) fuse.Status {
	var opened fuse.OpenOut
	status := s.RawFileSystem.OpenDir(cancel, &fuse.OpenIn{InHeader: in.InHeader, Flags: in.Flags}, &opened)
	if status != fuse.OK {
		return status
	}
	defer s.RawFileSystem.ReleaseDir(&fuse.ReleaseIn{InHeader: in.InHeader, Fh: opened.Fh, Flags: in.Flags})
	request := *in
	request.Fh = opened.Fh
	if plus {
		return s.RawFileSystem.ReadDirPlus(cancel, &request, out)
	}
	return s.RawFileSystem.ReadDir(cancel, &request, out)
}
