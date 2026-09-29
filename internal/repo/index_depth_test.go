//go:build !js

package repo

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"
	"testing"
	"time"

	"gyit/internal/store"
)

// Build a representative object-recipe index without including staging or Git
// acquisition in the measurement. Updates must retain the old snapshot and
// copy only a bounded leaf/ancestor path, even in a large repository.
func TestIndexSmallUpdatePreservesSnapshotsWithinByteBudget(t *testing.T) {
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := &indexIOStore{Store: backend}
	idx := &index{store: s, cache: newCache(32 << 20), containers: true}
	const records = 100000
	value := bytes.Repeat([]byte{42}, 160)
	position := 0
	c := &changes{read: func() ([]byte, []byte, error) {
		if position == records {
			return nil, nil, nil
		}
		key := []byte(fmt.Sprintf("o/%040d", position))
		position++
		return key, value, nil
	}}
	if err := c.next(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	idx.root, err = idx.updateChanges(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("build=%s writes=%d bytes=%d", time.Since(start), s.writes, s.bytes)
	oldRoot := idx.root
	key := fmt.Sprintf("o/%040d", records/2)
	lookup := func(root pageRef) ([]byte, int) {
		t.Helper()
		depth := 0
		for {
			depth++
			p, err := idx.page(t.Context(), root)
			if err != nil {
				t.Fatal(err)
			}
			if len(p.Children) == 0 {
				for _, entry := range p.Items {
					if entry.Key == key {
						return entry.Value, depth
					}
				}
				t.Fatal("record missing")
			}
			found := false
			for _, child := range p.Children {
				if key <= child.Max {
					root, found = child.ID, true
					break
				}
			}
			if !found {
				t.Fatal("record outside index bounds")
			}
		}
	}
	got, depth := lookup(oldRoot)
	if !bytes.Equal(got, value) {
		t.Fatal("initial record differs")
	}
	s.reads, s.writes, s.bytes = 0, 0, 0
	updated := bytes.Repeat([]byte{43}, 160)
	c = &changes{key: []byte(key), value: updated, read: func() ([]byte, []byte, error) { return nil, nil, nil }}
	start = time.Now()
	idx.root, err = idx.updateChanges(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("depth=%d update=%s reads=%d writes=%d bytes=%d", depth, time.Since(start), s.reads, s.writes, s.bytes)
	if s.bytes > 256<<10 {
		t.Fatalf("one-record update rewrote %d bytes; budget is 256 KiB", s.bytes)
	}
	if got, _ := lookup(idx.root); !bytes.Equal(got, updated) {
		t.Fatal("updated root does not contain new value")
	}
	if got, _ := lookup(oldRoot); !bytes.Equal(got, value) {
		t.Fatal("update changed an existing snapshot")
	}
}

func TestIndexLargeValuesSplitWithinLeafBudget(t *testing.T) {
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	idx := &index{store: backend, cache: newCache(32 << 20)}
	// One value may exceed the soft page target. Other pages must split by
	// encoded bytes, rather than just record count.
	values := [][]byte{bytes.Repeat([]byte{1}, 96<<10), bytes.Repeat([]byte{2}, 64<<10), bytes.Repeat([]byte{3}, 192<<10), bytes.Repeat([]byte{4}, 32<<10)}
	n := 0
	c := &changes{read: func() ([]byte, []byte, error) {
		if n == len(values) {
			return nil, nil, nil
		}
		key, value := []byte(fmt.Sprint(n)), values[n]
		n++
		return key, value, nil
	}}
	if err := c.next(); err != nil {
		t.Fatal(err)
	}
	idx.root, err = idx.updateChanges(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	root, err := idx.page(t.Context(), idx.root)
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Children) != len(values) {
		t.Fatalf("large records must split into individual leaves: %d children", len(root.Children))
	}
	for i, child := range root.Children {
		p, err := idx.page(t.Context(), child.ID)
		if err != nil || len(p.Items) != 1 || !bytes.Equal(p.Items[0].Value, values[i]) {
			t.Fatalf("leaf %d: %+v, %v", i, p, err)
		}
		if child.ID.Length != int64(indexItemBytes(p.Items[0])) {
			t.Fatal("leaf size accounting does not match wire encoding")
		}
		if child.ID.Length > indexLeafTarget && len(values[i]) <= indexLeafTarget {
			t.Fatal("small record exceeds leaf budget")
		}
	}
}

// Repeated random insertions must not repeatedly split full nodes into a full
// left page and a nearly empty right page, growing a tall, sparse search tree.
func TestIndexIncrementalInsertionsKeepBoundedDepth(t *testing.T) {
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	idx := &index{store: backend, cache: newCache(32 << 20), containers: true}
	value := bytes.Repeat([]byte{42}, 160)
	next := 0
	c := &changes{read: func() ([]byte, []byte, error) {
		if next == 16384 {
			return nil, nil, nil
		}
		key := []byte(fmt.Sprintf("o/%040d", next))
		next++
		return key, value, nil
	}}
	if err := c.next(); err != nil {
		t.Fatal(err)
	}
	idx.root, err = idx.updateChanges(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	for batch := range 32 {
		keys := make([]string, 4096)
		for i := range keys {
			keys[i] = fmt.Sprintf("history/v2/commit/%x", sha256.Sum256([]byte(fmt.Sprintf("%d/%d", batch, i))))
		}
		sort.Strings(keys)
		next := 0
		c := &changes{read: func() ([]byte, []byte, error) {
			if next == len(keys) {
				return nil, nil, nil
			}
			key := []byte(keys[next])
			next++
			return key, value, nil
		}}
		if err := c.next(); err != nil {
			t.Fatal(err)
		}
		idx.root, err = idx.updateChanges(t.Context(), c)
		if err != nil {
			t.Fatal(err)
		}
	}
	var depth func(pageRef) (int, int)
	depth = func(ref pageRef) (int, int) {
		p, err := idx.page(t.Context(), ref)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Children) == 0 {
			return 1, len(p.Items)
		}
		maxDepth, records := 0, 0
		for _, child := range p.Children {
			d, n := depth(child.ID)
			maxDepth = max(maxDepth, d)
			records += n
		}
		return maxDepth + 1, records
	}
	got, records := depth(idx.root)
	if records != 16384+32*4096 {
		t.Fatalf("record count %d", records)
	}
	t.Logf("records=%d depth=%d", records, got)
	if got > 4 {
		t.Fatalf("%d records after 32 publications require %d sequential pages; want at most 4", records, got)
	}
}

// The live history index accumulated chains of one-child internal nodes.
// Rewriting their path must reuse the child reference, rather than recreating
// every redundant level. Unchanged snapshots and sibling subtrees remain valid.
func TestIndexUpdateCollapsesUnaryPaths(t *testing.T) {
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	idx := &index{store: backend, cache: newCache(32 << 20), containers: true}
	w := &indexWriter{ctx: t.Context(), store: backend, prefix: "unary-index"}
	leaf, err := w.save(page{Items: []item{{Key: "a", Value: []byte("old")}, {Key: "b", Value: []byte("keep")}}})
	if err != nil {
		t.Fatal(err)
	}
	chain := leaf
	for range 8 {
		chain, err = w.save(page{Children: []edge{chain}})
		if err != nil {
			t.Fatal(err)
		}
	}
	sibling, err := w.save(page{Items: []item{{Key: "z", Value: []byte("sibling")}}})
	if err != nil {
		t.Fatal(err)
	}
	root, err := w.save(page{Children: []edge{chain, sibling}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	idx.root = root.ID
	c := &changes{key: []byte("a"), value: []byte("new"), read: func() ([]byte, []byte, error) { return nil, nil, nil }}
	idx.root, err = idx.updateChanges(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	lookup := func(ref pageRef, key, want string) int {
		t.Helper()
		for depth := 1; depth <= 12; depth++ {
			p, err := idx.page(t.Context(), ref)
			if err != nil {
				t.Fatal(err)
			}
			if len(p.Children) == 0 {
				for _, item := range p.Items {
					if item.Key == key {
						if string(item.Value) != want {
							t.Fatalf("value %q != %q", item.Value, want)
						}
						return depth
					}
				}
				t.Fatal("missing key")
			}
			for _, child := range p.Children {
				if key <= child.Max {
					ref = child.ID
					break
				}
			}
		}
		t.Fatal("depth exceeds bound")
		return 0
	}
	lookup(root.ID, "a", "old")
	lookup(idx.root, "b", "keep")
	lookup(idx.root, "z", "sibling")
	if depth := lookup(idx.root, "a", "new"); depth > 2 {
		t.Fatalf("updated lookup still traverses %d pages, want 2", depth)
	}
}
