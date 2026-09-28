//go:build !js

package repo

import (
	"context"
	"gyit/internal/store"
	"os"
)

// NewProgressive uses the platform disk cache when supplied. Readers themselves
// depend only on decoded byte leases; import and publication remain native.
func NewProgressive(ctx context.Context, backend store.Store, disk *store.DiskCache, temp string) (*Progressive, error) {
	var decoded decodedCache
	if disk != nil {
		decoded = disk
	}
	return newProgressive(ctx, backend, decoded, temp)
}
func NewDisk(s store.Store, dir, identity string, budget int64) (*Repository, error) {
	disk, err := store.NewDiskCache(nil, dir, identity, budget)
	if err != nil {
		return nil, err
	}
	r, err := NewSharedDisk(s, disk)
	if err != nil {
		disk.Close()
		return nil, err
	}
	r.borrowedDisk = false
	return r, nil
}
func NewSharedDisk(s store.Store, disk *store.DiskCache) (*Repository, error) {
	p, err := NewProgressive(context.Background(), s, disk, os.TempDir())
	if err != nil {
		return nil, err
	}
	r := p.HistoryRepository()
	r.refresh = true
	return r, nil
}
