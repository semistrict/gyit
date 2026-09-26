package repo

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"sort"
	"strings"

	"gyit/internal/spill"
	"gyit/internal/store"
	bolt "go.etcd.io/bbolt"
)

const fanout = 128
const indexPackSize = 8 << 20

type pageRef struct {
	Pack           string
	Offset, Length int64
	Hash           string
}

type item struct {
	Key   string
	Value []byte
}
type edge struct {
	Max string
	ID  pageRef
}
type page struct {
	Items    []item
	Children []edge
}

type index struct {
	globalSizes   *globalSizeSlot
	globalSizeRef globalSizeRef
	store         store.Store
	cache         *cache
	root          pageRef
	// Direct lookup root belongs to the same immutable snapshot publication.
	blobRoot pageRef
}

func (idx *index) pageBytes(ctx context.Context, ref pageRef) ([]byte, error) {
	if !strings.HasPrefix(ref.Pack, "index/") || ref.Offset < 0 || ref.Length <= 0 || ref.Length > indexPackSize || ref.Offset > indexPackSize-ref.Length {
		return nil, fmt.Errorf("invalid index page range")
	}
	hash, err := hex.DecodeString(ref.Hash)
	if err != nil || len(hash) != sha256.Size {
		return nil, fmt.Errorf("invalid index page hash")
	}
	b, err := idx.cache.load(ctx, "index/"+ref.Hash, func() ([]byte, error) {
		b, _, err := idx.store.Get(ctx, ref.Pack, ref.Offset, ref.Length)
		if err == nil && (int64(len(b)) != ref.Length || fmt.Sprintf("%x", sha256.Sum256(b)) != ref.Hash) {
			err = fmt.Errorf("index checksum mismatch")
		}
		return b, err
	})
	return b, err
}

func (idx *index) page(ctx context.Context, ref pageRef) (page, error) {
	b, err := idx.pageBytes(ctx, ref)
	var p page
	if err == nil {
		err = unmarshal(b, &p)
	}
	return p, err
}

func (idx *index) get(ctx context.Context, key string, out any) error {
	id := idx.root
	for id != (pageRef{}) {
		p, err := idx.page(ctx, id)
		if err != nil {
			return err
		}
		if len(p.Children) == 0 {
			i := sort.Search(len(p.Items), func(i int) bool { return p.Items[i].Key >= key })
			if i == len(p.Items) || p.Items[i].Key != key {
				return store.ErrNotFound
			}
			return unmarshal(p.Items[i].Value, out)
		}
		i := sort.Search(len(p.Children), func(i int) bool { return p.Children[i].Max >= key })
		if i == len(p.Children) {
			return store.ErrNotFound
		}
		id = p.Children[i].ID
	}
	return store.ErrNotFound
}

// scan visits only pages intersecting a prefix, with a bounded output batch.
func (idx *index) scan(ctx context.Context, prefix, after string, limit int) ([]item, error) {
	var result []item
	var visit func(pageRef) error
	start := prefix
	if after > start {
		start = after
	}
	end := prefix + "\xff"
	visit = func(id pageRef) error {
		if id == (pageRef{}) {
			return nil
		}
		p, err := idx.page(ctx, id)
		if err != nil {
			return err
		}
		if len(p.Children) == 0 {
			for _, v := range p.Items {
				if v.Key >= start && v.Key > after && strings.HasPrefix(v.Key, prefix) {
					result = append(result, v)
					if len(result) == limit {
						break
					}
				}
			}
			return nil
		}
		for i, child := range p.Children {
			if child.Max < start {
				continue
			}
			if i > 0 && p.Children[i-1].Max >= end {
				break
			}
			if err := visit(child.ID); err != nil {
				return err
			}
			if len(result) == limit {
				break
			}
		}
		return nil
	}
	err := visit(idx.root)
	return result, err
}

// indexWriter batches durable writes without widening the reader's fetch unit.
// References are assigned before upload; update must flush before publishing its root.
type indexWriter struct {
	ctx    context.Context
	store  store.Store
	prefix string
	number int
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
	if len(b) > indexPackSize {
		return pageRef{}, fmt.Errorf("index page exceeds pack size")
	}
	if len(w.data)+len(b) > indexPackSize {
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
		flush := func() error {
			if len(buf) == 0 {
				return nil
			}
			e, err := w.save(page{Items: buf})
			if err != nil {
				return err
			}
			buf = make([]item, 0, fanout)
			return emit(e)
		}
		add := func(v item) error {
			buf = append(buf, v)
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

// buildRoot groups the stream of same-height subtrees into higher levels.
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
	w := &indexWriter{ctx: ctx, store: idx.store, prefix: rand.Text(), data: make([]byte, 0, indexPackSize)}
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
