package repo

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
	"sort"
)

const progressivePageBytes = 8 << 10

func (p *Progressive) directoryRef(ctx context.Context, tree string) (pageRef, error) {
	var ref pageRef
	err := p.index().get(ctx, "tree/"+tree, &ref)

	return ref, err
}

// Historical trees need no durable derived directory publication. Decode the
// bounded Git tree and resolve sizes from the immutable object index on demand.
func (p *Progressive) unpreparedEntries(ctx context.Context, tree string) ([]Entry, error) {
	if err := p.Ensure(ctx, []string{tree}); err != nil {
		return nil, err
	}
	raw, kind, err := p.object(ctx, tree)
	if err != nil {
		return nil, err
	}
	if kind != 2 {
		return nil, fmt.Errorf("not a tree")
	}
	names, err := parseNativeTree(raw)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(names))
	for _, e := range names {
		if e.Mode != 0040000 && e.Mode != 0160000 {
			ids = append(ids, e.OID)
		}
	}
	if err := p.Ensure(ctx, ids); err != nil {
		return nil, err
	}
	out := make([]Entry, 0, len(names))
	for _, e := range names {
		entry := Entry{Name: e.Name, OID: e.OID, Mode: e.Mode, RawMode: e.RawMode}
		if e.Mode != 0040000 && e.Mode != 0160000 {
			entry.Size, err = p.ObjectSize(ctx, e.OID)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, entry)
	}
	return out, nil
}

func (p *Progressive) directoryPage(ctx context.Context, ref pageRef) (*pb.ProgressiveDirectory, error) {
	raw, err := p.directoryBytes(ctx, ref)
	if err != nil {
		return nil, err
	}
	page := &pb.ProgressiveDirectory{}
	if err := proto.Unmarshal(raw, page); err != nil {
		return nil, err
	}
	if len(page.Entries) > 64 || len(page.Children) > 32 || (len(page.Children) > 0 && len(page.Entries) > 0) {
		return nil, fmt.Errorf("metadata page shape")
	}
	previous := ""
	for _, e := range page.Entries {
		if string(e.Name) <= previous || e.Size < 0 || !validProgressiveOID(e.Oid) {
			return nil, fmt.Errorf("metadata entry")
		}
		previous = string(e.Name)
	}
	return page, nil
}
func (p *Progressive) readDir(ctx context.Context, tree, after string, limit int) ([]Entry, error) {
	return p.readDirAt(ctx, tree, pageRef{}, after, limit)
}

func (p *Progressive) readDirAt(ctx context.Context, tree string, ref pageRef, after string, limit int) ([]Entry, error) {
	if limit < 1 || limit > 128 {
		return nil, fmt.Errorf("directory batch must be 1..128")
	}
	var err error
	if ref == (pageRef{}) {
		ref, err = p.directoryRef(ctx, tree)
	}
	if errors.Is(err, store.ErrNotFound) {
		entries, err := p.unpreparedEntries(ctx, tree)
		if err != nil {
			return nil, err
		}
		start := sort.Search(len(entries), func(i int) bool { return entries[i].Name > after })
		return entries[start:min(len(entries), start+limit)], nil
	}
	if err != nil {
		return nil, err
	}
	var out []Entry
	var visit func(pageRef, int) error
	visit = func(ref pageRef, depth int) error {
		if depth > 16 {
			return fmt.Errorf("metadata routing depth")
		}
		page, err := p.directoryPage(ctx, ref)
		if err != nil {
			return err
		}
		for _, e := range page.Entries {
			if string(e.Name) > after {
				out = append(out, Entry{Name: string(e.Name), OID: e.Oid, Mode: e.Mode, Size: e.Size, RawMode: e.RawMode, directory: decodePageRef(e.Directory)})
				if len(out) == limit {
					return nil
				}
			}
		}
		for _, child := range page.Children {
			if string(child.MaxName) <= after {
				continue
			}
			if err := visit(decodePageRef(child.Page), depth+1); err != nil {
				return err
			}
			if len(out) == limit {
				break
			}
		}
		return nil
	}
	err = visit(ref, 0)
	return out, err
}
func (p *Progressive) lookup(ctx context.Context, tree, name string) (Entry, error) {
	return p.lookupAt(ctx, tree, pageRef{}, name)
}

func (p *Progressive) lookupAt(ctx context.Context, tree string, ref pageRef, name string) (Entry, error) {
	// Seek directly to the leaf containing name; no per-sibling file reads.
	var err error
	if ref == (pageRef{}) {
		ref, err = p.directoryRef(ctx, tree)
	}
	if errors.Is(err, store.ErrNotFound) {
		entries, err := p.unpreparedEntries(ctx, tree)
		if err != nil {
			return Entry{}, err
		}
		i := sort.Search(len(entries), func(i int) bool { return entries[i].Name >= name })
		if i == len(entries) || entries[i].Name != name {
			return Entry{}, store.ErrNotFound
		}
		return entries[i], nil
	}
	if err != nil {
		return Entry{}, err
	}
	for depth := 0; depth < 16; depth++ {
		page, err := p.directoryPage(ctx, ref)
		if err != nil {
			return Entry{}, err
		}
		if len(page.Children) > 0 {
			n := sort.Search(len(page.Children), func(i int) bool { return string(page.Children[i].MaxName) >= name })
			if n == len(page.Children) {
				return Entry{}, store.ErrNotFound
			}
			ref = decodePageRef(page.Children[n].Page)
			continue
		}
		n := sort.Search(len(page.Entries), func(i int) bool { return string(page.Entries[i].Name) >= name })
		if n == len(page.Entries) || string(page.Entries[n].Name) != name {
			return Entry{}, store.ErrNotFound
		}
		e := page.Entries[n]
		return Entry{Name: string(e.Name), OID: e.Oid, Mode: e.Mode, Size: e.Size, RawMode: e.RawMode, directory: decodePageRef(e.Directory)}, nil
	}
	return Entry{}, fmt.Errorf("metadata routing depth")
}
