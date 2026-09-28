//go:build !js

package store

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

type diskEntry struct {
	name       string
	size, cost int64
	pins       int
	mapping    []byte
	touched    time.Time
}

// DiskCache retains immutable byte entries as mmap files. Each directory
// namespace has a single process owner. Mappings live until eviction or close;
// live leases pin eviction candidates. The OS manages resident mapped pages.
// Whole-object reads (including mutable HEAD) and writes bypass the cache.
type DiskCache struct {
	Store
	mu        sync.Mutex
	dir       string
	lock      *os.File
	max, used int64
	limit     int64
	space     func() (int64, int64, error)
	stop      chan struct{}
	stopped   chan struct{}
	items     map[string]*list.Element
	pending   map[string]int64 // in-flight writes reserve bytes and entry slots
	lru       *list.List
	closed    bool
	leases    int
}

// NewDiskCache opens a persistent, bounded cache. identity must uniquely identify
// the backing repository, including endpoint and bucket for object storage.
// The byte budget accounts for a minimum 4 KiB allocation per range; filesystem
// directory metadata is additional. At most 65536 ranges are retained.
func NewDiskCache(s Store, dir, identity string, max int64) (*DiskCache, error) {
	return newDiskCache(s, dir, identity, max, nil)
}

func newDiskCache(s Store, dir, identity string, max int64, space func() (int64, int64, error)) (*DiskCache, error) {
	if dir == "" || identity == "" || max < 0 || max > 1<<60 {
		return nil, fmt.Errorf("disk cache requires directory, repository identity, and nonnegative budget")
	}
	dir = filepath.Join(dir, fmt.Sprintf("%x", sha256.Sum256([]byte(identity))))
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("disk cache already in use; select a different --disk-cache-dir: %w", err)
	}
	c := &DiskCache{Store: s, dir: dir, lock: lock, max: max, limit: max, items: make(map[string]*list.Element), pending: make(map[string]int64), lru: list.New()}
	entries, err := os.ReadDir(dir)
	if err != nil {
		c.Close()
		return nil, err
	}
	type existing struct {
		name string
		info os.FileInfo
	}
	var files []existing
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".range-") {
			_ = os.Remove(filepath.Join(dir, name))
			continue
		}
		if len(name) != 64 {
			continue
		}
		if _, err := hex.DecodeString(name); err != nil {
			continue
		}
		info, e := entry.Info()
		if e != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
			continue
		}
		files = append(files, existing{name, info})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].info.ModTime().Before(files[j].info.ModTime()) })
	for _, f := range files {
		cost := diskCost(f.info.Size())
		c.items[f.name] = c.lru.PushFront(&diskEntry{name: f.name, size: f.info.Size(), cost: cost})
		c.used += cost
	}
	c.space = func() (int64, int64, error) {
		var stat unix.Statfs_t
		if err := unix.Statfs(dir, &stat); err != nil {
			return 0, 0, err
		}
		return int64(stat.Blocks) * int64(stat.Bsize), int64(stat.Bavail) * int64(stat.Bsize), nil
	}
	if space != nil {
		c.space = space
	}
	c.refreshLimit()
	if !c.makeRoom(0, 0) {
		c.Close()
		return nil, fmt.Errorf("cannot trim disk cache to its budget")
	}
	c.stop = make(chan struct{})
	c.stopped = make(chan struct{})
	go func() {
		defer close(c.stopped)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-c.stop:
				return
			case <-tick.C:
				c.mu.Lock()
				c.refreshLimit()
				c.makeRoom(0, 0)
				c.mu.Unlock()
			}
		}
	}()
	return c, nil
}

func diskCost(n int64) int64 { return (n + 4095) / 4096 * 4096 }

// Caller holds mu (or is constructing the cache).
func (c *DiskCache) makeRoom(cost int64, count int) bool {
	for e := c.lru.Back(); (c.used+cost > c.limit || len(c.items)+len(c.pending)+count > 65536) && e != nil; {
		prev := e.Prev()
		v := e.Value.(*diskEntry)
		if v.pins == 0 {
			if err := os.Remove(filepath.Join(c.dir, v.name)); err == nil || errors.Is(err, os.ErrNotExist) {
				v.unmap()
				c.used -= v.cost
				delete(c.items, v.name)
				c.lru.Remove(e)
			}
		}
		e = prev
	}
	return c.used+cost <= c.limit && len(c.items)+len(c.pending)+count <= 65536
}

func (c *DiskCache) mapped(name string, n int64) ([]byte, func(), bool) {
	e := c.items[name]
	if e == nil {
		return nil, nil, false
	}
	v := e.Value.(*diskEntry)
	if n >= 0 && v.size != n {
		return nil, nil, false
	}
	if v.mapping == nil {
		f, err := os.Open(filepath.Join(c.dir, name))
		if err != nil {
			return nil, nil, false
		}
		info, err := f.Stat()
		if err != nil || info.Size() != v.size {
			f.Close()
			return nil, nil, false
		}
		mapping, err := unix.Mmap(int(f.Fd()), 0, int(v.size), unix.PROT_READ, unix.MAP_SHARED)
		f.Close()
		if err != nil {
			return nil, nil, false
		}
		v.mapping = mapping
	}
	b := v.mapping
	v.pins++
	c.leases++
	c.lru.MoveToFront(e)
	// Persist an approximate restart LRU without a metadata write on every hit.
	now := time.Now()
	if now.Sub(v.touched) >= time.Minute {
		_ = os.Chtimes(filepath.Join(c.dir, name), now, now)
		v.touched = now
	}
	var once sync.Once
	return b, func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			v.pins--
			c.leases--
			if c.closed && v.pins == 0 {
				v.unmap()
			}
			c.makeRoom(0, 0)
			if c.closed && c.leases == 0 && len(c.pending) == 0 {
				c.unlock()
			}
		})
	}, true
}

// Called with the cache lock held, and only after all borrowers have released.
func (v *diskEntry) unmap() {
	if v.mapping != nil {
		_ = unix.Munmap(v.mapping)
		v.mapping = nil
	}
}

func (c *DiskCache) Acquire(ctx context.Context, key string, off, n int64) ([]byte, func(), error) {
	noop := func() {}
	if err := ctx.Err(); err != nil {
		return nil, noop, err
	}
	if err := validKey(key); err != nil {
		return nil, noop, err
	}
	if off < 0 || n < -1 || (n == -1 && off != 0) {
		return nil, noop, fmt.Errorf("invalid range")
	}
	// HEAD is mutable even for range reads. Large reads never populate the cache.
	if key == "HEAD" || n <= 0 || n > c.max || n > int64(int(^uint(0)>>1)) {
		b, _, err := c.Store.Get(ctx, key, off, n)
		return b, noop, err
	}
	name := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%d", key, off, n))))
	b, release, _, err := c.load(ctx, name, n, func() ([]byte, error) {
		b, _, err := c.Store.Get(ctx, key, off, n)
		return b, err
	})
	return b, release, err
}

// Load caches immutable decoded data and lends a mapping until release.
func (c *DiskCache) Load(ctx context.Context, key string, fn func() ([]byte, error)) ([]byte, func(), error) {
	name := fmt.Sprintf("%x", sha256.Sum256([]byte("decoded-v1\x00"+key)))
	b, release, _, err := c.load(ctx, name, -1, fn)
	return b, release, err
}

func (c *DiskCache) load(ctx context.Context, name string, n int64, fn func() ([]byte, error)) ([]byte, func(), bool, error) {
	noop := func() {}
	if err := ctx.Err(); err != nil {
		return nil, noop, false, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, noop, false, os.ErrClosed
	}
	b, release, ok := c.mapped(name, n)
	c.mu.Unlock()
	if ok {
		return b, release, true, nil
	}
	b, err := fn()
	if err != nil {
		return nil, noop, false, err
	}
	if n >= 0 && int64(len(b)) != n {
		return nil, noop, false, fmt.Errorf("short storage range")
	}
	n = int64(len(b))
	if n == 0 || n > c.max {
		return b, noop, false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.pending[name] != 0 {
		return b, noop, false, nil
	}
	if data, release, ok := c.mapped(name, n); ok {
		return data, release, true, nil
	}
	// A corrupt/unreadable cached entry is never overwritten while borrowed.
	if e := c.items[name]; e != nil {
		v := e.Value.(*diskEntry)
		if v.pins != 0 {
			return b, noop, false, nil
		}
		if err := os.Remove(filepath.Join(c.dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return b, noop, false, nil
		}
		v.unmap()
		c.used -= v.cost
		delete(c.items, name)
		c.lru.Remove(e)
	}
	cost := diskCost(n)
	c.refreshLimit()
	if !c.makeRoom(cost, 1) {
		return b, noop, false, nil
	}
	// Reserve before dropping the lock: temporary files count against the same
	// byte/entry budget as published files. Different keys can write in parallel;
	// another reader of this key may use its decoded bytes without another write.
	c.pending[name] = cost
	c.used += cost
	c.mu.Unlock()
	tmp, mapping, err := writeCacheFile(c.dir, b)
	c.mu.Lock()
	delete(c.pending, name)
	published := false
	defer func() {
		if !published {
			if mapping != nil {
				_ = unix.Munmap(mapping)
			}
			if tmp != "" {
				_ = os.Remove(tmp)
			}
			c.used -= cost
		}
		c.makeRoom(0, 0)
		if c.closed && c.leases == 0 && len(c.pending) == 0 {
			c.unlock()
		}
	}()
	if err != nil || c.closed || ctx.Err() != nil {
		return b, noop, false, nil
	}
	if err = os.Rename(tmp, filepath.Join(c.dir, name)); err != nil {
		return b, noop, false, nil
	}
	published = true
	// Its write already set mtime; no extra Chtimes is needed on the first hit.
	c.items[name] = c.lru.PushFront(&diskEntry{name: name, size: n, cost: cost, mapping: mapping, touched: time.Now()})
	if data, release, ok := c.mapped(name, n); ok {
		return data, release, true, nil
	}
	return b, noop, false, nil
}

// The caller reserves budget and holds the owner lock until it removes or
// publishes this temporary file. No cache mutex is held during the write.
func writeCacheFile(dir string, b []byte) (string, []byte, error) {
	f, err := os.CreateTemp(dir, ".range-")
	if err != nil {
		return "", nil, err
	}
	if _, err = f.Write(b); err != nil {
		_ = f.Close()
		return f.Name(), nil, err
	}
	// Reuse the write descriptor instead of reopening and restatting the file.
	mapping, _ := unix.Mmap(int(f.Fd()), 0, len(b), unix.PROT_READ, unix.MAP_SHARED)
	return f.Name(), mapping, f.Close()
}

// Get preserves Store ownership semantics. Lease-aware readers call Acquire to
// decode directly from mmap without this copy.
func (c *DiskCache) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if key == "HEAD" || n == -1 {
		return c.Store.Get(ctx, key, off, n)
	}
	b, release, err := c.Acquire(ctx, key, off, n)
	defer release()
	return append([]byte(nil), b...), "", err
}

func (c *DiskCache) unlock() {
	if c.lock != nil {
		_ = unix.Flock(int(c.lock.Fd()), unix.LOCK_UN)
		_ = c.lock.Close()
		c.lock = nil
	}
}

// Close prevents new leases; existing leases remain valid until released.
func (c *DiskCache) Close() error {
	c.mu.Lock()
	if !c.closed && c.stop != nil {
		close(c.stop)
	}
	c.closed = true
	for _, e := range c.items {
		v := e.Value.(*diskEntry)
		if v.pins == 0 {
			v.unmap()
		}
	}
	if c.leases == 0 && len(c.pending) == 0 {
		c.unlock()
	}
	stopped := c.stopped
	c.mu.Unlock()
	if stopped != nil {
		<-stopped
	}
	return nil
}

// ReadScope owns concurrent range leases for one bounded decode operation.
// Close must run only after every Get call and consumer has finished.
type ReadScope struct {
	store    Store
	mu       sync.Mutex
	releases []func()
}

func NewReadScope(s Store) *ReadScope { return &ReadScope{store: s} }
func (s *ReadScope) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if n == -1 {
		return s.store.Get(ctx, key, off, n)
	}
	b, release, err := Acquire(ctx, s.store, key, off, n)
	s.mu.Lock()
	s.releases = append(s.releases, release)
	s.mu.Unlock()
	return b, "", err
}
func (s *ReadScope) Close() {
	for _, release := range s.releases {
		release()
	}
	s.releases = nil
}

// effectiveDiskLimit reserves 20 GiB of filesystem capacity. Existing
// cache bytes are reclaimable; bytes owned by other processes are not.
func effectiveDiskLimit(max, used, total, available int64) int64 {
	reserve := int64(20 << 30)
	if available >= reserve {
		return min(max, used+available-reserve)
	}
	return min(max, maxInt64Zero(used-(reserve-available)))
}
func maxInt64Zero(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}
func (c *DiskCache) refreshLimit() {
	if c.space == nil {
		return
	}
	total, available, err := c.space()
	if err != nil {
		c.limit = 0
		return
	}
	// Reserved writes may not yet occupy disk space. Counting them as already
	// reclaimable would let concurrent insertions encroach on the free reserve.
	var pending int64
	for _, cost := range c.pending {
		pending += cost
	}
	c.limit = effectiveDiskLimit(c.max, c.used-pending, total, available)
}

// LoadMapped also reports whether the result is backed by a pinned disk mapping.
func (c *DiskCache) LoadMapped(ctx context.Context, key string, fn func() ([]byte, error)) ([]byte, func(), bool, error) {
	name := fmt.Sprintf("%x", sha256.Sum256([]byte("decoded-v1\x00"+key)))
	return c.load(ctx, name, -1, fn)
}
