package repo

import (
	"container/list"
	"context"
	"errors"
	"gyit/internal/store"
	"sync"

	"golang.org/x/sync/singleflight"
)

// decodedCache is the platform boundary for persistent decoded byte leases.
// A nil implementation uses the same reader with its bounded in-memory LRU.
type decodedCache interface {
	Load(context.Context, string, func() ([]byte, error)) ([]byte, func(), error)
	Close() error
}

type cached struct {
	key   string
	alias string // At most one second name for the same in-memory bytes.
	data  []byte
}

// Leave two of the eight page-load slots available for foreground readers.
// This is shared by all speculative workers using this cache.
const cacheReadAheadLimit = 6

type cacheReadAheadKey struct{}

var errReadAheadBusy = errors.New("read-ahead budget occupied")

type cache struct {
	disk           decodedCache
	mu             sync.Mutex
	max, used      int
	items          map[string]*list.Element
	lru            *list.List
	flight         singleflight.Group
	slots          chan struct{}
	readAheadSlots chan struct{}
}

func newCache(max int) *cache {
	return &cache{max: max, items: make(map[string]*list.Element), lru: list.New(), slots: make(chan struct{}, 8), readAheadSlots: make(chan struct{}, cacheReadAheadLimit)}
}

// borrow permits wire readers to inspect a cached page without copying it.
// The caller must release before returning any view into the page's bytes.
func (c *cache) borrow(ctx context.Context, key string, fn func() ([]byte, error)) ([]byte, func(), error) {
	return c.borrowLimited(ctx, key, c.slots, fn)
}

func (c *cache) borrowLimited(ctx context.Context, key string, slots chan struct{}, fn func() ([]byte, error)) ([]byte, func(), error) {
	if c.disk != nil {
		return c.disk.Load(ctx, key, func() ([]byte, error) { return c.loadLimited(ctx, key, slots, fn) })
	}
	b, err := c.loadLimited(ctx, key, slots, fn)
	return b, func() {}, err
}

// borrowCached probes without consuming a load slot. This lets a small verified
// page outlive its larger source container without nesting cache load slots.
func (c *cache) borrowCached(ctx context.Context, key string) ([]byte, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, func() {}, err
	}
	if c.disk != nil {
		return c.disk.Load(ctx, key, func() ([]byte, error) { return nil, store.ErrNotFound })
	}
	if data, ok := c.get(key); ok {
		return data, func() {}, nil
	}
	return nil, func() {}, store.ErrNotFound
}

func (c *cache) get(key string) ([]byte, bool) {
	if c.disk != nil {
		b, release, err := c.disk.Load(context.Background(), key, func() ([]byte, error) { return nil, store.ErrNotFound })
		defer release()
		if err != nil {
			return nil, false
		}
		return append([]byte(nil), b...), true
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.items[key]
	if e == nil {
		return nil, false
	}
	c.lru.MoveToFront(e)
	return e.Value.(cached).data, true
}

func (c *cache) put(key string, data []byte) {
	if c.disk != nil {
		_, release, _ := c.disk.Load(context.Background(), key, func() ([]byte, error) { return data, nil })
		release()
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if len(data) > c.max || c.max == 0 {
		return
	}
	if e := c.items[key]; e != nil {
		c.lru.MoveToFront(e)
		return
	}
	for c.used+len(data) > c.max {
		e := c.lru.Back()
		v := e.Value.(cached)
		delete(c.items, v.key)
		if v.alias != "" {
			delete(c.items, v.alias)
		}
		c.used -= len(v.data)
		c.lru.Remove(e)
	}
	c.items[key] = c.lru.PushFront(cached{key: key, data: data})
	c.used += len(data)
}

// alias shares the same LRU entry and byte accounting. Acquisition uses one
// offset name and one verified OID name per object; eviction removes both.
// Persistent cache entries retain their ordinary independent leases.
func (c *cache) alias(key, alias string) {
	if c.disk != nil || key == alias {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.items[alias] != nil {
		return
	}
	e := c.items[key]
	if e == nil {
		return
	}
	value := e.Value.(cached)
	if value.alias != "" {
		return
	}
	value.alias = alias
	e.Value = value
	c.items[alias] = e
}

func (c *cache) load(ctx context.Context, key string, fn func() ([]byte, error)) ([]byte, error) {
	return c.loadLimited(ctx, key, c.slots, fn)
}

func (c *cache) loadLimited(ctx context.Context, key string, slots chan struct{}, fn func() ([]byte, error)) ([]byte, error) {
	for {
		if b, ok := c.get(key); ok {
			return b, nil
		}
		owned := false
		ch := c.flight.DoChan(key, func() (any, error) {
			owned = true
			if b, ok := c.get(key); ok {
				return b, nil
			}
			if ctx.Value(cacheReadAheadKey{}) == true {
				// Hold the speculative budget for the loader's lifetime, even
				// when its original waiter cancels before an I/O call returns.
				// Acquire before normal slots to leave capacity for foreground.
				select {
				case c.readAheadSlots <- struct{}{}:
				case <-ctx.Done():
					return nil, ctx.Err()
				default:
					// Never leave a shared flight queued behind speculation;
					// a foreground waiter may need this exact key immediately.
					return nil, errReadAheadBusy
				}
				defer func() { <-c.readAheadSlots }()
			}
			if slots != nil {
				select {
				case slots <- struct{}{}:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				defer func() { <-slots }()
			}
			b, err := fn()
			if err == nil {
				c.put(key, b)
			}
			return b, err
		})
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case result := <-ch:
			if result.Err != nil {
				// A short-lived reader (including read-ahead) may own the shared
				// load. Its cancellation must not cancel another live reader. The
				// flight has ended, so retry using this caller's context/loader.
				// Do not retry a backend error from our own loader indefinitely.
				if !owned && ctx.Err() == nil && (errors.Is(result.Err, context.Canceled) || errors.Is(result.Err, context.DeadlineExceeded) || errors.Is(result.Err, errReadAheadBusy)) {
					continue
				}
				return nil, result.Err
			}
			return result.Val.([]byte), nil
		}
	}
}
