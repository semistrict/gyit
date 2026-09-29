//go:build !js

package repo

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"gyit/internal/spill"
	"gyit/internal/store"
	"iter"

	"google.golang.org/protobuf/encoding/protowire"
)

// Keep leaf rewrites small even when records carry more than compact recipes.
const indexLeafTarget = 128 << 10

func indexItemBytes(v item) int {
	n := 0
	if v.Key != "" {
		n += 1 + protowire.SizeBytes(len(v.Key))
	}
	if len(v.Value) != 0 {
		n += 1 + protowire.SizeBytes(len(v.Value))
	}
	return 1 + protowire.SizeBytes(n)
}

// indexWriter batches durable writes without widening the reader's fetch unit.
// References are assigned before upload; update must flush before publishing its root.
type indexWriter struct {
	cache *cache
	// admit may copy decoded immutable bytes into the existing bounded cache.
	// It does not publish references and must not retain the reusable data buffer.
	admit     func(context.Context, string, []byte)
	packLimit int
	// A soft target for prefetched index containers; single large pages may
	// exceed it while remaining within packLimit/the format bound.
	packTarget int
	ctx        context.Context
	store      store.Store
	prefix     string
	number     int
	// step partitions directory pack numbers among workers sharing a prefix.
	// Zero retains the normal consecutive numbering.
	step int
	data []byte
}

func (w *indexWriter) key() string { return fmt.Sprintf("index/%s/%08x", w.prefix, w.number) }
func (w *indexWriter) flush() error {
	if len(w.data) == 0 {
		return nil
	}
	if err := w.store.Put(w.ctx, w.key(), w.data, ""); err != nil {
		return err
	}
	if w.admit != nil {
		w.admit(w.ctx, w.key(), w.data)
	}
	if w.cache != nil {
		data := w.data
		if w.cache.disk == nil {
			data = append([]byte(nil), data...)
		}
		w.cache.put("index-container/"+w.key(), data)
	}
	w.number += max(1, w.step)
	w.data = w.data[:0]
	return nil
}

func (w *indexWriter) save(p page) (edge, error) {
	b, err := marshal(p)
	if err != nil {
		return edge{}, err
	}
	id, err := w.saveBytes(b)
	if err != nil {
		return edge{}, err
	}
	max := ""
	if len(p.Items) > 0 {
		max = p.Items[len(p.Items)-1].Key
	} else if len(p.Children) > 0 {
		max = p.Children[len(p.Children)-1].Max
	}
	return edge{Max: max, ID: id}, nil
}

func (w *indexWriter) saveBytes(b []byte) (pageRef, error) {
	limit := w.packLimit
	if limit == 0 {
		limit = indexPackSize
	}
	if len(b) > limit {
		return pageRef{}, fmt.Errorf("index page exceeds pack size")
	}
	target := limit
	if w.packTarget > 0 {
		target = min(target, w.packTarget)
	}
	if len(w.data)+len(b) > target {
		if err := w.flush(); err != nil {
			return pageRef{}, err
		}
	}
	id := pageRef{Pack: w.key(), Offset: int64(len(w.data)), Length: int64(len(b)), Hash: fmt.Sprintf("%x", sha256.Sum256(b))}
	w.data = append(w.data, b...)
	return id, nil
}

type changes struct {
	cursor     *bolt.Cursor
	read       func() ([]byte, []byte, error)
	key, value []byte
}

func (c *changes) next() error {
	if c.read != nil {
		var err error
		c.key, c.value, err = c.read()
		return err
	}
	c.key, c.value = c.cursor.Next()
	return nil
}
func (c *changes) within(bound string) bool {
	return c.key != nil && (bound == "" || string(c.key) <= bound)
}

// merge performs a sorted copy-on-write merge. Unaffected subtrees are reused
// without reading them. Page buffers and the builder occupy O(fanout * height).
func (idx *index) merge(ctx context.Context, c *changes, w *indexWriter, id pageRef, bound string, emit func(edge) error) error {
	p := page{}
	if id != (pageRef{}) {
		var err error
		p, err = idx.page(ctx, id)
		if err != nil {
			return err
		}
	}
	if len(p.Children) == 0 {
		buf := make([]item, 0, fanout)
		bytes := 0
		flush := func() error {
			if len(buf) == 0 {
				return nil
			}
			e, err := w.save(page{Items: buf})
			if err != nil {
				return err
			}
			buf = make([]item, 0, fanout)
			bytes = 0
			return emit(e)
		}
		add := func(v item) error {
			size := indexItemBytes(v)
			if len(buf) != 0 && bytes+size > indexLeafTarget {
				if err := flush(); err != nil {
					return err
				}
			}
			buf = append(buf, v)
			bytes += size
			if len(buf) == fanout {
				return flush()
			}
			return nil
		}
		i := 0
		for i < len(p.Items) || c.within(bound) {
			if c.within(bound) && (i == len(p.Items) || string(c.key) <= p.Items[i].Key) {
				k := string(c.key)
				v := append([]byte(nil), c.value...)
				if i < len(p.Items) && p.Items[i].Key == k {
					i++
				}
				if err := c.next(); err != nil {
					return err
				}
				if err := add(item{k, v}); err != nil {
					return err
				}
			} else {
				if err := add(p.Items[i]); err != nil {
					return err
				}
				i++
			}
		}
		return flush()
	}
	buf := make([]edge, 0, fanout)
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		// A unary branch adds an object-store round trip without narrowing
		// the key range. References do not encode tree height, so promote its
		// immutable child directly; siblings may retain their old depths.
		if len(buf) == 1 {
			only := buf[0]
			buf = make([]edge, 0, fanout)
			return emit(only)
		}
		e, err := w.save(page{Children: buf})
		if err != nil {
			return err
		}
		buf = make([]edge, 0, fanout)
		return emit(e)
	}
	add := func(e edge) error {
		buf = append(buf, e)
		if len(buf) == fanout {
			return flush()
		}
		return nil
	}
	for i, child := range p.Children {
		upper := child.Max
		if i == len(p.Children)-1 {
			upper = bound
		}
		if !c.within(upper) {
			if err := add(child); err != nil {
				return err
			}
			continue
		}
		if err := idx.merge(ctx, c, w, child.ID, upper, add); err != nil {
			return err
		}
	}
	return flush()
}

// update merges a bounded stream of changed records into immutable subtrees.
func (idx *index) update(ctx context.Context, bucket *bolt.Bucket) (root pageRef, err error) {
	c := &changes{cursor: bucket.Cursor()}
	c.key, c.value = c.cursor.First()
	return idx.updateChanges(ctx, c)
}

// updateSorted consumes the same sorted changes as update without a temporary
// B-tree copy. Pulling the callback preserves bounded buffers and closes every
// staging reader even when an index upload or an existing-page fetch fails.
func (idx *index) updateSorted(ctx context.Context, records *spill.Sorter) (pageRef, error) {
	type record struct{ key, value []byte }
	stopped := errors.New("index stopped consuming staged records")
	sequence := func(yield func(record, error) bool) {
		err := records.Walk(ctx, func(key, value []byte) error {
			if !yield(record{key, value}, nil) {
				return stopped
			}
			return nil
		})
		if err != nil && !errors.Is(err, stopped) {
			yield(record{}, err)
		}
	}
	next, stop := iter.Pull2(sequence)
	defer stop()
	c := &changes{read: func() ([]byte, []byte, error) {
		r, err, ok := next()
		if !ok {
			return nil, nil, nil
		}
		return r.key, r.value, err
	}}
	if err := c.next(); err != nil {
		return pageRef{}, err
	}
	return idx.updateChanges(ctx, c)
}

func (idx *index) updateChanges(ctx context.Context, c *changes) (root pageRef, err error) {
	if c.key == nil {
		return idx.root, nil
	}
	w := &indexWriter{ctx: ctx, store: idx.store, prefix: rand.Text(), data: make([]byte, 0, indexContainerTarget)}
	if idx.containers {
		w.cache = idx.cache
		w.packTarget = indexContainerTarget
	}
	defer func() {
		if err == nil {
			err = w.flush()
		}
		if err != nil {
			root = pageRef{}
		}
	}()
	levels := [][]edge{}
	var add func(int, edge) error
	add = func(level int, e edge) error {
		for len(levels) <= level {
			levels = append(levels, nil)
		}
		levels[level] = append(levels[level], e)
		if len(levels[level]) < fanout {
			return nil
		}
		parent, err := w.save(page{Children: levels[level]})
		if err != nil {
			return err
		}
		levels[level] = nil
		return add(level+1, parent)
	}
	if err := idx.merge(ctx, c, w, idx.root, "", func(e edge) error { return add(0, e) }); err != nil {
		return pageRef{}, err
	}
	for level := 0; level < len(levels); level++ {
		if len(levels[level]) == 0 {
			continue
		}
		if level == len(levels)-1 && len(levels[level]) == 1 {
			return levels[level][0].ID, nil
		}
		parent, err := w.save(page{Children: levels[level]})
		if err != nil {
			return pageRef{}, err
		}
		levels[level] = nil
		if err := add(level+1, parent); err != nil {
			return pageRef{}, err
		}
	}
	return pageRef{}, fmt.Errorf("empty index")
}
