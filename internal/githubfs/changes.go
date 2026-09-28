package githubfs

import "strings"

// SubscribeChanges observes namespace publication. Callbacks must only enqueue
// work: kernel invalidation can block and must not run on a filesystem request.
// Paths are relative to github.com; an empty path means its directory listing.
func (f *FS) SubscribeChanges(fn func(string)) func() {
	f.changeMu.Lock()
	if f.changes == nil {
		f.changes = make(map[uint64]func(string))
	}
	f.changeID++
	id := f.changeID
	f.changes[id] = fn
	f.changeMu.Unlock()
	return func() {
		f.changeMu.Lock()
		delete(f.changes, id)
		f.changeMu.Unlock()
	}
}

func (f *FS) changed(path string) {
	f.changeMu.Lock()
	defer f.changeMu.Unlock()
	if f.directoryVersions == nil {
		f.directoryVersions = make(map[string]uint64)
	}
	f.directoryVersions[path]++
	for _, fn := range f.changes {
		fn(path)
	}
}

// DirectoryVersion supplies an mtime change token for kernel readdir caches.
// Repository descendants share the publication generation; namespace listings
// advance when discovery changes or a cached remote listing expires.
func (n Namespace) DirectoryVersion(path string) uint64 {
	p, err := namespacePath(path)
	if err != nil {
		return 1
	}
	if strings.Contains(p, "/") {
		return n.FS.Generation(p)
	}
	n.FS.changeMu.Lock()
	defer n.FS.changeMu.Unlock()
	return 1 + n.FS.directoryVersions[p]
}
