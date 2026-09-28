package repo

import (
	"context"
	"sort"
	"strings"

	"gyit/internal/store"
)

// A reference listing performs thousands of small index probes. Retain only
// decoded routing nodes, with a fixed 2 MiB allowance; leaf payloads continue
// through the repository's existing bounded raw cache. This cache dies when the
// command finishes, so it cannot accumulate across concurrent publications.
type viewRefIndex struct {
	*index
	pages map[pageRef]page
	bytes int
}

func newViewRefIndex(idx *index) *viewRefIndex {
	return &viewRefIndex{index: idx, pages: make(map[pageRef]page)}
}

func (idx *viewRefIndex) page(ctx context.Context, ref pageRef) (page, error) {
	if p, ok := idx.pages[ref]; ok {
		return p, nil
	}
	p, err := idx.index.page(ctx, ref)
	if err != nil {
		return p, err
	}
	if len(p.Children) > 0 {
		// Include retained strings, slice elements, map overhead, and a margin.
		cost := 256
		for _, c := range p.Children {
			cost += 128 + len(c.Max) + len(c.ID.Pack) + len(c.ID.Hash)
		}
		if cost <= (2<<20)-idx.bytes {
			idx.pages[ref] = p
			idx.bytes += cost
		}
	}
	return p, nil
}

func (idx *viewRefIndex) get(ctx context.Context, key string, out any) error {
	if idx.progressive != nil {
		return idx.index.get(ctx, key, out)
	}
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

func (idx *viewRefIndex) scan(ctx context.Context, prefix, after string, limit int) ([]item, error) {
	if idx.progressive != nil {
		return idx.index.scan(ctx, prefix, after, limit)
	}
	var result []item
	start := max(prefix, after)
	end := prefix + "\xff"
	var visit func(pageRef) error
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
		for i, c := range p.Children {
			if c.Max < start {
				continue
			}
			if i > 0 && p.Children[i-1].Max >= end {
				break
			}
			if err := visit(c.ID); err != nil {
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

// Decode each requested commit leaf once instead of once per reference. Retain
// at most 4 MiB of subjects; overflow falls back to exact per-row lookup.
func (idx *viewRefIndex) subjects(ctx context.Context, ids []string) (map[string]string, error) {
	if idx.progressive != nil {
		out := make(map[string]string)
		used := 0
		for _, id := range ids {
			if _, ok := out[id]; ok {
				continue
			}
			var c commitInfo
			if err := idx.get(ctx, "c/"+id, &c); err != nil {
				return nil, err
			}
			subject := viewRefSubject(c)
			used += len(id) + len(subject) + 128
			if used > 4<<20 {
				break
			}
			out[id] = subject
		}
		return out, nil
	}
	sort.Strings(ids)
	subjects := make(map[string]string)
	used := 0
	var visit func(pageRef, []string) error
	visit = func(id pageRef, keys []string) error {
		if len(keys) == 0 || id == (pageRef{}) {
			return nil
		}
		p, err := idx.page(ctx, id)
		if err != nil {
			return err
		}
		if len(p.Children) == 0 {
			for _, key := range keys {
				i := sort.Search(len(p.Items), func(i int) bool { return p.Items[i].Key >= key })
				if i == len(p.Items) || p.Items[i].Key != key {
					return store.ErrNotFound
				}
				var c commitInfo
				if err := unmarshal(p.Items[i].Value, &c); err != nil {
					return err
				}
				subject := viewRefSubject(c)
				cost := len(key) + len(subject) + 128
				if cost <= (4<<20)-used {
					subjects[strings.TrimPrefix(key, "c/")] = subject
					used += cost
				}
			}
			return nil
		}
		for _, child := range p.Children {
			n := sort.Search(len(keys), func(i int) bool { return keys[i] > child.Max })
			if err := visit(child.ID, keys[:n]); err != nil {
				return err
			}
			keys = keys[n:]
			if len(keys) == 0 {
				break
			}
		}
		if len(keys) > 0 {
			return store.ErrNotFound
		}
		return nil
	}
	keys := make([]string, 0, len(ids))
	for i, id := range ids {
		if i == 0 || id != ids[i-1] {
			keys = append(keys, "c/"+id)
		}
	}
	err := visit(idx.root, keys)
	return subjects, err
}

func viewRefSubject(c commitInfo) string {
	return strings.Join(strings.Fields(strings.SplitN(string(c.Message), "\n\n", 2)[0]), " ")
}
