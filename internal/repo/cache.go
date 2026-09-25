package repo

import (
	"container/list"
	"context"
	"sync"

	"golang.org/x/sync/singleflight"
)

type cached struct {
	key  string
	data []byte
}
type cache struct {
	mu        sync.Mutex
	max, used int
	items     map[string]*list.Element
	lru       *list.List
	flight    singleflight.Group
	slots     chan struct{}
}

func newCache(max int) *cache {
	return &cache{max: max, items: make(map[string]*list.Element), lru: list.New(), slots: make(chan struct{}, 8)}
}

func (c *cache) get(key string) ([]byte, bool) {
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
		c.used -= len(v.data)
		c.lru.Remove(e)
	}
	c.items[key] = c.lru.PushFront(cached{key, data})
	c.used += len(data)
}

func (c *cache) load(ctx context.Context, key string, fn func() ([]byte, error)) ([]byte, error) {
	if b, ok := c.get(key); ok {
		return b, nil
	}
	ch := c.flight.DoChan(key, func() (any, error) {
		if b, ok := c.get(key); ok {
			return b, nil
		}
		select {
		case c.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		defer func() { <-c.slots }()
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
			return nil, result.Err
		}
		return result.Val.([]byte), nil
	}
}
