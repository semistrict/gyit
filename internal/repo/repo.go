// Package repo stores immutable Git packs and exposes lazy, pinned snapshots.
package repo

import (
	"context"
	"errors"
	"fmt"
	"gyit/internal/store"
	"os"
	"strings"
)

const ChunkSize = 1 << 20
const DefaultCacheBytes = 32 << 20

// manifest is a query view, not a persisted storage format.
type manifest struct {
	Format        string
	Refs          pageRef
	RevisionGraph bool
}
type reference struct{ Commit, ObjectID, SymbolicTarget string }
type parents struct{ Parents []string }
type object struct {
	Kind string
	Size int64
	Tree string
}
type Entry struct {
	Name, OID string
	Mode      uint32
	Size      int64
	RawMode   uint32
	// Immutable prepared metadata carried with a directory entry. Readers can
	// descend without looking its tree ID up in the global object index again.
	directory pageRef
}
type Repository struct {
	progressive  *Progressive
	store        store.Store
	cache        *cache
	refresh      bool
	borrowedDisk bool
}
type Snapshot struct {
	progressive                   *Progressive
	idx                           *index
	SHA, Tree, Branch, DetachedAt string
}

func New(s store.Store, cacheBytes int) (*Repository, error) {
	if cacheBytes < 0 {
		return nil, fmt.Errorf("cache size cannot be negative")
	}
	p, err := NewProgressive(context.Background(), s, nil, os.TempDir())
	if err != nil {
		return nil, err
	}
	p.cache = newCache(cacheBytes)
	r := p.HistoryRepository()
	r.refresh = true
	return r, nil
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
func (r *Repository) Close() error {
	if r.cache.disk != nil && !r.borrowedDisk {
		return r.cache.disk.Close()
	}
	return nil
}
func (r *Repository) Open(ctx context.Context, sha string) (*Snapshot, error) {
	if err := r.refreshRoot(ctx); err != nil {
		return nil, err
	}
	return r.progressive.Open(ctx, sha)
}
func (r *Repository) queryManifest(ctx context.Context) (manifest, error) {
	if err := r.refreshRoot(ctx); err != nil {
		return manifest{}, err
	}
	var refs pageRef
	err := r.progressive.index().get(ctx, "refs-root", &refs)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return manifest{}, err
	}
	return manifest{Format: "sha1", RevisionGraph: true, Refs: refs}, nil
}
func (s *Snapshot) Lookup(ctx context.Context, tree, name string) (Entry, error) {
	return s.progressive.lookup(ctx, tree, name)
}
func (s *Snapshot) ReadDir(ctx context.Context, tree, after string, limit int) ([]Entry, error) {
	return s.progressive.readDir(ctx, tree, after, limit)
}

// ReadDirectory lists an entry returned by Resolve, Lookup, or ReadDir.
func (s *Snapshot) ReadDirectory(ctx context.Context, dir Entry, after string, limit int) ([]Entry, error) {
	if dir.Mode != 0040000 {
		return nil, store.ErrNotFound
	}
	return s.progressive.readDirAt(ctx, dir.OID, dir.directory, after, limit)
}

// LookupDirectory uses the immutable metadata pointer carried by dir, when ready.
func (s *Snapshot) LookupDirectory(ctx context.Context, dir Entry, name string) (Entry, error) {
	if dir.Mode != 0040000 {
		return Entry{}, store.ErrNotFound
	}
	return s.progressive.lookupAt(ctx, dir.OID, dir.directory, name)
}
func (s *Snapshot) ReadAt(ctx context.Context, oid string, dest []byte, off int64) (int, error) {
	return s.progressive.readAt(ctx, oid, dest, off)
}
func IsNotFound(err error) bool { return errors.Is(err, store.ErrNotFound) }
func (s *Snapshot) Resolve(ctx context.Context, path string) (Entry, error) {
	e := Entry{OID: s.Tree, Mode: 0040000}
	if path == "" {
		return e, nil
	}
	for _, name := range strings.Split(path, "/") {
		if e.Mode != 0040000 {
			return Entry{}, store.ErrNotFound
		}
		var err error
		e, err = s.LookupDirectory(ctx, e, name)
		if err != nil {
			return Entry{}, err
		}
	}
	return e, nil
}

// Standalone readers observe new publications; mount readers share the writer's
// live object pool and already receive those updates directly.
func (r *Repository) refreshRoot(ctx context.Context) error {
	if !r.refresh {
		return nil
	}
	p := r.progressive
	p.writer.Lock()
	defer p.writer.Unlock()
	latest, err := NewProgressive(ctx, r.store, r.cache.disk, p.temp)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.root, p.token = latest.root, latest.token
	p.mu.Unlock()
	return nil
}
