// Package repo imports immutable Git objects and exposes lazy, pinned snapshots.
package repo

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"gyit/internal/store"
)

const ChunkSize = 1 << 20
const PackSize = 64 << 20
const DefaultCacheBytes = 32 << 20

type manifest struct {
	Version        int
	Format         string
	Root           pageRef
	Tips           []string
	Refs           pageRef
	RevisionGraph  bool
	RefsHash       string
	CommitMetadata bool
	History        pageRef
	HistoryCount   uint64
	Blobs          pageRef
}
type reference struct{ Commit, ObjectID, SymbolicTarget string }
type parents struct{ Parents []string }

type object struct {
	Kind      string
	Size      int64
	Tree      string
	Directory pageRef
}
type anchorRecord struct{ Candidates []chunkBase }

type chunkBase struct {
	Base           *chunkBase
	Pack           string
	Offset, Length int64
	Hash           string
}

type chunk struct {
	ArchiveRecipe string
	Base          *chunkBase
	Pack          string
	Offset        int64
	Length        int64
	Hash          string
}

type Entry struct {
	Name string
	OID  string
	Mode uint32
	Size int64
	// RawMode preserves historical tree permissions for raw object reads.
	// Zero means the original mode was already canonical.
	RawMode uint32
}

type Repository struct {
	readerOwner          *Repository
	borrowedDisk         bool
	globalMu             sync.Mutex
	globalSizes          *globalSizeSlot
	configuredCacheBytes int
	store                store.Store
	cache                *cache
}

func New(s store.Store, cacheBytes int) (*Repository, error) {
	if cacheBytes < 0 {
		return nil, fmt.Errorf("cache size cannot be negative")
	}
	return &Repository{store: s, cache: newCache(cacheBytes), configuredCacheBytes: cacheBytes}, nil
}

// withStore changes the metadata view without allocating another cache or
// global-size slot. The owner also serializes first use across concurrent views.
func (r *Repository) withStore(s store.Store) *Repository {
	owner := r
	if r.readerOwner != nil {
		owner = r.readerOwner
	}
	return &Repository{store: s, cache: r.cache,
		configuredCacheBytes: r.configuredCacheBytes, readerOwner: owner}
}

func readHead(ctx context.Context, s store.Store) (manifest, string, error) {
	b, token, err := s.Get(ctx, "HEAD", 0, -1)
	var m manifest
	if err != nil {
		return m, "", err
	}
	if err = unmarshal(b, &m); err != nil {
		return m, "", err
	}
	if !supportedFormat(m.Version) || (m.Format != "sha1" && m.Format != "sha256") || m.Root == (pageRef{}) {
		return m, "", fmt.Errorf("unsupported or invalid repository manifest")
	}
	return m, token, nil
}

type Snapshot struct {
	globalSizes   *globalSizeSlot
	globalSizeRef globalSizeRef
	idx           *index
	history       *index
	SHA, Tree     string
	// Checkout identity is immutable and travels with the selected snapshot.
	Branch, DetachedAt string
}

func (r *Repository) Open(ctx context.Context, sha string) (*Snapshot, error) {
	m, _, err := readHead(ctx, r.store)
	if err != nil {
		return nil, err
	}
	want := 40
	if m.Format == "sha256" {
		want = 64
	}
	if len(sha) != want {
		return nil, fmt.Errorf("use a full %s commit ID", m.Format)
	}
	if _, err := hex.DecodeString(sha); err != nil {
		return nil, fmt.Errorf("invalid commit ID: %w", err)
	}
	sha = strings.ToLower(sha)
	idx := &index{store: r.store, cache: r.cache, root: m.Root, blobRoot: m.Blobs}
	if err := r.bindGlobalIndex(ctx, m, idx); err != nil {
		return nil, err
	}
	var o object
	if err := idx.get(ctx, "o/"+sha, &o); err != nil {
		return nil, fmt.Errorf("commit %s: %w", sha, err)
	}
	if o.Kind != "commit" {
		return nil, fmt.Errorf("%s is not a commit", sha)
	}
	return &Snapshot{idx: idx, history: &index{store: r.store, cache: r.cache, root: m.History}, SHA: sha, Tree: o.Tree}, nil
}

func treePrefix(tree string) string       { return "t/" + tree + "/" }
func treeKey(tree, name string) string    { return treePrefix(tree) + hex.EncodeToString([]byte(name)) }
func chunkKey(oid string, n int64) string { return fmt.Sprintf("b/%s/%016x", oid, n) }

func (s *Snapshot) Lookup(ctx context.Context, tree, name string) (Entry, error) {
	var o object
	if err := s.idx.get(ctx, "o/"+tree, &o); err != nil {
		return Entry{}, err
	}
	if o.Kind != "tree" {
		return Entry{}, fmt.Errorf("%s is not a tree", tree)
	}
	if isNativeTree(o.Directory) {
		e, err := s.lookupNativeTreeName(ctx, tree, o, name)
		if err != nil {
			return Entry{}, err
		}
		return s.hydrateDirent(ctx, e)
	}
	if o.Directory != (pageRef{}) {
		return s.idx.lookupDirectory(ctx, o.Directory, name)
	}
	var e Entry
	err := s.idx.get(ctx, treeKey(tree, name), &e)
	e.Name = name
	return e, err
}

// ReadDir returns at most limit entries, ordered by raw filename bytes.
// Pass the last returned Name as after to continue without loading the directory.
func (s *Snapshot) ReadDir(ctx context.Context, tree, after string, limit int) ([]Entry, error) {
	if limit < 1 || limit > fanout {
		return nil, fmt.Errorf("directory batch must be 1..%d", fanout)
	}
	var o object
	if err := s.idx.get(ctx, "o/"+tree, &o); err != nil {
		return nil, err
	}
	if o.Kind != "tree" {
		return nil, fmt.Errorf("%s is not a tree", tree)
	}
	if isNativeTree(o.Directory) {
		names, err := s.readNativeTreeNames(ctx, tree, o, after, limit)
		if err != nil {
			return nil, err
		}
		return s.hydrateDirents(ctx, names)
	}
	if o.Directory != (pageRef{}) {
		return s.idx.readDirectory(ctx, o.Directory, after, limit)
	}
	prefix := treePrefix(tree)
	cursor := ""
	if after != "" {
		cursor = treeKey(tree, after)
	}
	items, err := s.idx.scan(ctx, prefix, cursor, limit)
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(items))
	for _, v := range items {
		var e Entry
		if err := unmarshal(v.Value, &e); err != nil {
			return nil, err
		}
		name, err := hex.DecodeString(strings.TrimPrefix(v.Key, prefix))
		if err != nil {
			return nil, err
		}
		e.Name = string(name)
		entries = append(entries, e)
	}
	return entries, nil
}

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
		e, err = s.Lookup(ctx, e.OID, name)
		if err != nil {
			return Entry{}, err
		}
	}
	return e, nil
}

func (s *Snapshot) ReadAt(ctx context.Context, oid string, dest []byte, off int64) (int, error) {
	if s.idx.blobRoot == (pageRef{}) {
		return s.readAtLegacy(ctx, oid, dest, off)
	}
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	if len(dest) == 0 {
		return 0, nil
	}
	n := 0
	var fullSize int64 = -1
	for n < len(dest) {
		part := off / ChunkSize
		size, c, err := s.readBlobPart(ctx, oid, part)
		if err != nil {
			return n, err
		}
		if fullSize >= 0 && size != fullSize {
			return n, fmt.Errorf("inconsistent direct blob size")
		}
		fullSize = size
		if off >= size {
			break
		}
		b, release, err := s.borrowChunk(ctx, c)
		if err != nil {
			release()
			return n, err
		}
		expected := min(int64(ChunkSize), size-part*ChunkSize)
		if int64(len(b)) != expected {
			release()
			return n, fmt.Errorf("invalid chunk size")
		}
		copied := copy(dest[n:], b[off%ChunkSize:])
		release()
		n += copied
		off += int64(copied)
	}
	if n < len(dest) {
		return n, io.EOF
	}
	return n, nil
}

func (s *Snapshot) readAtLegacy(ctx context.Context, oid string, dest []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	if len(dest) == 0 {
		return 0, nil
	}
	var o object
	if err := s.idx.get(ctx, "o/"+oid, &o); err != nil {
		return 0, err
	}
	if o.Kind != "blob" {
		return 0, fmt.Errorf("%s is not a blob", oid)
	}
	if off >= o.Size {
		return 0, io.EOF
	}
	n := 0
	for n < len(dest) && off < o.Size {
		part := off / ChunkSize
		var c chunk
		if err := s.idx.get(ctx, chunkKey(oid, part), &c); err != nil {
			return n, err
		}
		if c.ArchiveRecipe != "" {
			if _, err := checkedArchiveBlob(c, oid, o.Size, part); err != nil {
				return n, err
			}
		}
		b, release, err := s.borrowChunk(ctx, c)
		if err != nil {
			release()
			return n, err
		}
		expected := int64(ChunkSize)
		if remain := o.Size - part*ChunkSize; remain < expected {
			expected = remain
		}
		if int64(len(b)) != expected {
			release()
			return n, fmt.Errorf("invalid chunk size")
		}
		copied := copy(dest[n:], b[off%ChunkSize:])
		release()
		n += copied
		off += int64(copied)
	}
	if n < len(dest) {
		return n, io.EOF
	}
	return n, nil
}

func IsNotFound(err error) bool { return errors.Is(err, store.ErrNotFound) }

// NewDisk uses one persistent decoded-data cache. No compressed store wrapper or
// retained RAM byte cache is installed. Small decode bookkeeping remains in RAM.
func NewDisk(s store.Store, dir, identity string, budget int64) (*Repository, error) {
	disk, err := store.NewDiskCache(nil, dir, identity, budget)
	if err != nil {
		return nil, err
	}
	r, err := New(s, DefaultCacheBytes)
	if err != nil {
		disk.Close()
		return nil, err
	}
	r.cache.max = 0
	r.cache.disk = disk
	return r, nil
}

func (r *Repository) Close() error {
	r.globalMu.Lock()
	defer r.globalMu.Unlock()
	if r.globalSizes != nil {
		_ = r.globalSizes.clear(context.Background())
	}
	if r.cache.disk != nil && !r.borrowedDisk {
		return r.cache.disk.Close()
	}
	return nil
}

// NewSharedDisk borrows one decoded cache across immutable repositories.
// Cache keys identify content, so equal objects can share decoded bytes.
// The caller must close repositories before closing disk.
func NewSharedDisk(s store.Store, disk *store.DiskCache) (*Repository, error) {
	r, err := New(s, 0)
	if err != nil {
		return nil, err
	}
	r.cache.disk = disk
	r.borrowedDisk = true
	return r, nil
}
