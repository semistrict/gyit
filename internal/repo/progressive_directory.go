package repo

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sort"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
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

// PrepareSnapshot builds only previously unprepared or changed subtrees. All
// referenced blobs must have been acquired first. Publication is a single CAS.
func (p *Progressive) PrepareSnapshot(ctx context.Context, sha string) error {
	s, err := p.Open(ctx, sha)
	if err != nil {
		return err
	}
	missing, err := p.MissingSnapshot(ctx, s.Tree)
	if err != nil {
		return err
	}
	if err = p.Ensure(ctx, missing); err != nil {
		return err
	}
	p.writer.Lock()
	defer p.writer.Unlock()
	return p.buildDirectories(ctx, s.Tree)
}

// MissingSnapshot skips complete immutable subtrees before examining entries.
// Thus a small update need not enumerate the unchanged snapshot.
func (p *Progressive) MissingSnapshot(ctx context.Context, tree string) ([]string, error) {
	missing := make(map[string]bool)
	seen := make(map[string]bool)
	var visit func(string, int) error
	visit = func(tree string, depth int) error {
		if depth > 512 {
			return fmt.Errorf("directory depth limit")
		}
		if seen[tree] {
			return nil
		}
		seen[tree] = true
		var ref pageRef
		if err := p.index().get(ctx, "tree/"+tree, &ref); err == nil {
			return nil
		} else if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		raw, kind, err := p.object(ctx, tree)
		if err != nil {
			return err
		}
		if kind != 2 {
			return fmt.Errorf("not a tree")
		}
		entries, err := parseNativeTree(raw)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if e.Mode == 0040000 {
				if err := visit(e.OID, depth+1); err != nil {
					return err
				}
			} else if e.Mode != 0160000 {
				if _, err := p.ObjectSize(ctx, e.OID); errors.Is(err, store.ErrNotFound) {
					missing[e.OID] = true
				} else if err != nil {
					return err
				}
			}
		}
		return nil
	}
	err := visit(tree, 0)
	ids := make([]string, 0, len(missing))
	for id := range missing {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, err
}
func (p *Progressive) buildDirectories(ctx context.Context, tree string) error {
	enc, err := newCompressor()
	if err != nil {
		return err
	}
	defer enc.Close()
	w := &indexWriter{ctx: ctx, store: p.store, prefix: "progressive-" + rand.Text(), packLimit: progressiveContainerBytes}
	return p.stage(func(changes *bolt.Bucket) error {
		save := func(page *pb.ProgressiveDirectory) (pageRef, error) {
			raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(page)
			if e != nil {
				return pageRef{}, e
			}
			if len(raw) > 64<<10 {
				return pageRef{}, fmt.Errorf("metadata page too large")
			}
			return w.saveBytes(enc.EncodeAll(raw, nil))
		}
		var build func(string, int) (pageRef, error)
		build = func(oid string, depth int) (pageRef, error) {
			if depth > 512 {
				return pageRef{}, fmt.Errorf("directory depth limit")
			}
			if err := ctx.Err(); err != nil {
				return pageRef{}, err
			}
			key := "tree/" + oid
			var ref pageRef
			if v := changes.Get([]byte(key)); v != nil {
				err := unmarshal(v, &ref)
				return ref, err
			}
			if e := p.index().get(ctx, key, &ref); e == nil {
				return ref, nil
			} else if !errors.Is(e, store.ErrNotFound) {
				return ref, e
			}
			raw, kind, e := p.object(ctx, oid)
			if e != nil {
				return ref, e
			}
			if kind != 2 {
				return ref, fmt.Errorf("not a directory")
			}
			entries, e := parseNativeTree(raw)
			if e != nil {
				return ref, e
			}
			sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
			page := &pb.ProgressiveDirectory{}
			var leaves []*pb.DirectoryChild
			flush := func() error {
				r, e := save(page)
				if e != nil {
					return e
				}
				var maxName []byte
				if len(page.Entries) > 0 {
					maxName = page.Entries[len(page.Entries)-1].Name
				}
				leaves = append(leaves, &pb.DirectoryChild{MaxName: maxName, Page: encodePageRef(r)})
				page = &pb.ProgressiveDirectory{}
				return nil
			}
			for _, entry := range entries {
				e := &pb.ProgressiveEntry{Name: []byte(entry.Name), Oid: entry.OID, Mode: entry.Mode, RawMode: entry.RawMode}
				if entry.Mode == 0040000 {
					child, err := build(entry.OID, depth+1)
					if err != nil {
						return ref, err
					}
					e.Directory = encodePageRef(child)
				} else if entry.Mode != 0040000 && entry.Mode != 0160000 {
					e.Size, err = p.ObjectSize(ctx, entry.OID)
					if err != nil {
						return ref, err
					}
				}
				page.Entries = append(page.Entries, e)
				if len(page.Entries) >= 64 || proto.Size(page) >= progressivePageBytes {
					if err := flush(); err != nil {
						return ref, err
					}
				}
			}
			if len(page.Entries) > 0 || len(leaves) == 0 {
				if err := flush(); err != nil {
					return ref, err
				}
			}
			for len(leaves) > 1 {
				next := make([]*pb.DirectoryChild, 0, (len(leaves)+31)/32)
				for i := 0; i < len(leaves); i += 32 {
					end := min(i+32, len(leaves))
					r, e := save(&pb.ProgressiveDirectory{Children: leaves[i:end]})
					if e != nil {
						return ref, e
					}
					next = append(next, &pb.DirectoryChild{MaxName: leaves[end-1].MaxName, Page: encodePageRef(r)})
				}
				leaves = next
			}
			ref = decodePageRef(leaves[0].Page)
			b, e := marshal(ref)
			if e != nil {
				return ref, e
			}
			return ref, changes.Put([]byte(key), b)
		}
		if _, err := build(tree, 0); err != nil {
			return err
		}
		if key, _ := changes.Cursor().First(); key == nil {
			return nil
		}
		if err := w.flush(); err != nil {
			return err
		}
		return p.publish(ctx, changes)
	})
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
