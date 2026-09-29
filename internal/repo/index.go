package repo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"gyit/internal/store"
	"strings"
)

const fanout = 128
const indexPackSize = 8 << 20
const indexContainerTarget = 2 << 20

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
	// Progressive object indexes store uncompressed pages in bounded containers.
	containers bool
	// Random object recipes benefit from narrow reads; scans still load whole
	// containers. Both paths share verified page and container cache entries.
	pageRanges  bool
	progressive *Progressive
	store       store.Store
	cache       *cache
	root        pageRef
}

func (idx *index) pageBytes(ctx context.Context, ref pageRef) ([]byte, error) {
	b, release, err := idx.borrowPage(ctx, ref)
	defer release()
	if idx.cache.disk != nil {
		b = append([]byte(nil), b...)
	}
	return b, err
}

func (idx *index) borrowPage(ctx context.Context, ref pageRef) ([]byte, func(), error) {
	if !strings.HasPrefix(ref.Pack, "index/") || ref.Offset < 0 || ref.Length <= 0 || ref.Length > indexPackSize || ref.Offset > indexPackSize-ref.Length {
		return nil, func() {}, fmt.Errorf("invalid index page range")
	}
	hash, err := hex.DecodeString(ref.Hash)
	if err != nil || len(hash) != sha256.Size {
		return nil, func() {}, fmt.Errorf("invalid index page hash")
	}
	if idx.containers {
		key := fmt.Sprintf("verified-index/%s/%x/%x/%s", ref.Pack, ref.Offset, ref.Length, ref.Hash)
		if page, release, err := idx.cache.borrowCached(ctx, key); !errors.Is(err, store.ErrNotFound) {
			return page, release, err
		} else {
			release()
		}
		var raw []byte
		var release func()
		if idx.pageRanges {
			raw, release, err = idx.cache.borrowCached(ctx, "index-container/"+ref.Pack)
			if errors.Is(err, store.ErrNotFound) {
				release()
				return idx.cache.borrow(ctx, key, func() ([]byte, error) {
					b, _, err := idx.store.Get(ctx, ref.Pack, ref.Offset, ref.Length)
					if err == nil && (int64(len(b)) != ref.Length || fmt.Sprintf("%x", sha256.Sum256(b)) != ref.Hash) {
						err = fmt.Errorf("index checksum mismatch")
					}
					return b, err
				})
			}
		} else {
			raw, release, err = idx.cache.borrow(ctx, "index-container/"+ref.Pack, func() ([]byte, error) {
				b, _, err := idx.store.Get(ctx, ref.Pack, 0, -1)
				if err == nil && len(b) > indexPackSize {
					return nil, fmt.Errorf("index container exceeds size bound")
				}
				return b, err
			})
		}
		defer release()
		if err != nil {
			return nil, func() {}, err
		}
		if ref.Offset+ref.Length > int64(len(raw)) {
			return nil, func() {}, fmt.Errorf("index container range")
		}
		// The cache loader may outlive a canceled waiter. Own these bytes before
		// handing them to it, so releasing the container cannot unmap its input.
		// This also prevents a small in-memory page from pinning a large container.
		page := append([]byte(nil), raw[ref.Offset:ref.Offset+ref.Length]...)
		return idx.cache.borrow(ctx, key, func() ([]byte, error) {
			if fmt.Sprintf("%x", sha256.Sum256(page)) != ref.Hash {
				return nil, fmt.Errorf("index checksum mismatch")
			}
			return page, nil
		})
	}
	return idx.cache.borrow(ctx, "index/"+ref.Hash, func() ([]byte, error) {
		b, _, err := idx.store.Get(ctx, ref.Pack, ref.Offset, ref.Length)
		if err == nil && (int64(len(b)) != ref.Length || fmt.Sprintf("%x", sha256.Sum256(b)) != ref.Hash) {
			err = fmt.Errorf("index checksum mismatch")
		}
		return b, err
	})
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
	return idx.getPrefetch(ctx, key, out, nil)
}

// A caller may warm sibling pages while it follows the authoritative lookup.
// The callback borrows a validated branch page and must not retain its bytes.
func (idx *index) getPrefetch(ctx context.Context, key string, out any, hint func(pageRef, []byte)) error {
	if idx.progressive != nil {
		return idx.progressive.historyRecord(ctx, key, out)
	}
	id := idx.root
	for id != (pageRef{}) {
		b, release, err := idx.borrowPage(ctx, id)
		if err != nil {
			release()
			return err
		}
		var next pageRef
		next, err = lookupIndexPage(b, key, out)
		if err == nil && next != (pageRef{}) && hint != nil {
			hint(id, b)
		}
		release()
		if err != nil || next == (pageRef{}) {
			return err
		}
		id = next
	}
	return store.ErrNotFound
}

// scan visits only pages intersecting a prefix, with a bounded output batch.
func (idx *index) scan(ctx context.Context, prefix, after string, limit int) ([]item, error) {
	if idx.progressive != nil {
		return idx.progressive.historyScan(ctx, prefix, after, limit)
	}
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
