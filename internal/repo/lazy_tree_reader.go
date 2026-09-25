package repo

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	wirecodec "gat/internal/packcodec"
	storagev1 "gat/internal/gen/gat/storage/v1"
	"gat/internal/store"
	"google.golang.org/protobuf/proto"
)

const nativeTreeBytes = 64 << 10
const nativeTreeWork = 4 << 20
const nativeTreeDescriptorBytes = 4 << 10

// Dirent deliberately cannot represent a file size. Callers that need stat
// semantics use Entry-returning methods, which resolve exact blob metadata.
type Dirent struct {
	Name, OID     string
	Mode, RawMode uint32
}

// In addition to the shared cache's eight decode flights, bound parsing on cache
// hits. This gate is never acquired by blob hydration or another tree read.
var nativeTreeReaders = make(chan struct{}, 8)

func isNativeTree(ref pageRef) bool {
	return strings.HasPrefix(ref.Pack, "index/native-trees-") || isArchiveTree(ref)
}
func direntOf(e Entry) Dirent {
	return Dirent{Name: e.Name, OID: e.OID, Mode: e.Mode, RawMode: e.RawMode}
}

func (s *Snapshot) LookupName(ctx context.Context, tree, name string) (Dirent, error) {
	var o object
	if err := s.idx.get(ctx, "o/"+tree, &o); err != nil {
		return Dirent{}, err
	}
	if o.Kind != "tree" {
		return Dirent{}, fmt.Errorf("%s is not a tree", tree)
	}
	if isNativeTree(o.Directory) {
		return s.lookupNativeTreeName(ctx, tree, o, name)
	}
	var e Entry
	var err error
	if o.Directory != (pageRef{}) {
		e, err = s.idx.lookupDirectory(ctx, o.Directory, name)
	} else {
		err = s.idx.get(ctx, treeKey(tree, name), &e)
		e.Name = name
	}
	return direntOf(e), err
}

func (s *Snapshot) ReadDirNames(ctx context.Context, tree, after string, limit int) ([]Dirent, error) {
	if limit < 1 || limit > fanout {
		return nil, fmt.Errorf("directory batch must be 1..%d", fanout)
	}
	var o object
	if err := s.idx.get(ctx, "o/"+tree, &o); err != nil {
		return nil, err
	}
	if o.Kind != "tree" {
		return nil, fmt.Errorf("%s is not a tree", tree)
	}
	if isNativeTree(o.Directory) {
		return s.readNativeTreeNames(ctx, tree, o, after, limit)
	}
	var entries []Entry
	var err error
	if o.Directory != (pageRef{}) {
		entries, err = s.idx.readDirectory(ctx, o.Directory, after, limit)
	} else {
		entries, err = s.ReadDir(ctx, tree, after, limit)
	}
	if err != nil {
		return nil, err
	}
	out := make([]Dirent, len(entries))
	for i, e := range entries {
		out[i] = direntOf(e)
	}
	return out, nil
}

func (s *Snapshot) ResolveName(ctx context.Context, path string) (Dirent, error) {
	e := Dirent{OID: s.Tree, Mode: 0040000}
	if path == "" {
		return e, nil
	}
	for _, name := range strings.Split(path, "/") {
		if e.Mode != 0040000 {
			return Dirent{}, store.ErrNotFound
		}
		var err error
		e, err = s.LookupName(ctx, e.OID, name)
		if err != nil {
			return Dirent{}, err
		}
	}
	return e, nil
}

func (s *Snapshot) hydrateDirent(ctx context.Context, d Dirent) (Entry, error) {
	if slot, _ := s.globalBinding(); slot != nil {
		return s.globalHydrateDirent(ctx, d)
	}
	e := Entry{Name: d.Name, OID: d.OID, Mode: d.Mode, RawMode: d.RawMode}
	if d.Mode == 0040000 || d.Mode == 0160000 {
		return e, nil
	}
	size, _, err := s.readBlobPart(ctx, d.OID, 0)
	if err != nil {
		return Entry{}, err
	}
	if size < 0 {
		return Entry{}, fmt.Errorf("negative blob size")
	}
	e.Size = size
	return e, nil
}

func (s *Snapshot) hydrateDirents(ctx context.Context, names []Dirent) ([]Entry, error) {
	if slot, _ := s.globalBinding(); slot != nil {
		return s.globalHydrateDirents(ctx, names)
	}
	out := make([]Entry, len(names))
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var first error
	var once sync.Once
	workers := min(8, len(names))
	for worker := 0; worker < workers; worker++ {
		wg.Go(func() {
			for i := worker; i < len(names); i += workers {
				e, err := s.hydrateDirent(workCtx, names[i])
				if err != nil {
					once.Do(func() { first = err; cancel() })
					return
				}
				out[i] = e
			}
		})
	}
	wg.Wait()
	if first != nil {
		return nil, first
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Snapshot) lookupNativeTreeName(ctx context.Context, oid string, o object, name string) (Dirent, error) {
	entries, err := s.nativeTreeEntries(ctx, oid, o)
	if err != nil {
		return Dirent{}, err
	}
	i := sort.Search(len(entries), func(i int) bool { return entries[i].Name >= name })
	if i == len(entries) || entries[i].Name != name {
		return Dirent{}, store.ErrNotFound
	}
	return entries[i], nil
}

func (s *Snapshot) readNativeTreeNames(ctx context.Context, oid string, o object, after string, limit int) ([]Dirent, error) {
	entries, err := s.nativeTreeEntries(ctx, oid, o)
	if err != nil {
		return nil, err
	}
	first := sort.Search(len(entries), func(i int) bool { return entries[i].Name > after })
	// A tiny page must not pin the full parsed array or raw tree after eviction.
	out := make([]Dirent, min(limit, len(entries)-first))
	copy(out, entries[first:first+len(out)])
	return out, nil
}

func (s *Snapshot) nativeTreeEntries(ctx context.Context, oid string, o object) ([]Dirent, error) {
	if isArchiveTree(o.Directory) {
		return s.archiveTreeEntries(ctx, oid, o)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case nativeTreeReaders <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-nativeTreeReaders }()
	if o.Size < 0 || o.Size > nativeTreeBytes || o.Directory.Length > nativeTreeDescriptorBytes {
		return nil, fmt.Errorf("native tree descriptor size limit")
	}
	wire, err := s.idx.pageBytes(ctx, o.Directory)
	if err != nil {
		return nil, err
	}
	var p storagev1.ChunkRecord
	if err := (proto.UnmarshalOptions{RecursionLimit: 4}).Unmarshal(wire, &p); err != nil {
		return nil, err
	}
	c := chunk{Pack: p.Pack, Offset: p.Offset, Length: p.Length, Hash: p.Hash}
	if p.Base != nil {
		if p.Base.Base != nil {
			return nil, fmt.Errorf("native tree descriptor depth")
		}
		c.Base = &chunkBase{Pack: p.Base.Pack, Offset: p.Base.Offset, Length: p.Base.Length, Hash: p.Base.Hash}
	}
	if c.Hash != "git-tree-sha1:"+oid {
		return nil, fmt.Errorf("native tree identity mismatch")
	}
	raw, err := s.readNativeTree(ctx, c)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) != o.Size {
		return nil, fmt.Errorf("native tree size mismatch")
	}
	return parseNativeTree(raw)
}

func validNativeTreeHash(hash string) bool {
	if !strings.HasPrefix(hash, "git-tree-sha1:") {
		return false
	}
	oid := strings.TrimPrefix(hash, "git-tree-sha1:")
	if len(oid) != 40 || strings.ToLower(oid) != oid {
		return false
	}
	_, err := hex.DecodeString(oid)
	return err == nil
}

func checkedNativeTree(raw []byte, hash string) ([]byte, error) {
	if !validNativeTreeHash(hash) {
		return nil, fmt.Errorf("invalid native tree hash")
	}
	h := sha1.New()
	fmt.Fprintf(h, "tree %d\x00", len(raw))
	h.Write(raw)
	if fmt.Sprintf("git-tree-sha1:%x", h.Sum(nil)) != hash {
		return nil, fmt.Errorf("native Git tree checksum mismatch")
	}
	return raw, nil
}

func (s *Snapshot) readNativeTree(ctx context.Context, c chunk) ([]byte, error) {
	loc := chunkLocation(c)
	if loc.Base != nil && loc.Base.Base != nil {
		return nil, fmt.Errorf("native tree descriptor depth")
	}
	var total int64
	for p := &loc; p != nil; p = p.Base {
		if !strings.HasPrefix(p.Pack, "packs/nativechain-") || !validNativeTreeHash(p.Hash) || p.Offset < 0 || p.Length <= 0 || p.Length > nativeTreeBytes || p.Offset > PackSize-p.Length {
			return nil, fmt.Errorf("invalid native tree payload range")
		}
		total += p.Length
	}
	if total > nativeTreeBytes {
		return nil, fmt.Errorf("native tree encoded closure limit")
	}
	return s.idx.cache.load(ctx, "tree/"+c.Hash, func() ([]byte, error) {
		// Root cache keys are deliberately separate: a native root can exceed
		// 64KiB, while a cached target always passed the target-size bound.
		var root []byte
		haveRoot := false
		parts := []chunkBase{loc}
		if loc.Base != nil {
			root, haveRoot = s.idx.cache.get("tree-root/" + loc.Base.Hash)
			if !haveRoot {
				parts = append(parts, *loc.Base)
			}
		}
		packed := make([][]byte, len(parts))
		errors := make([]error, len(parts))
		fetchCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		var wg sync.WaitGroup
		for i, part := range parts {
			wg.Go(func() {
				packed[i], _, errors[i] = s.idx.store.Get(fetchCtx, part.Pack, part.Offset, part.Length)
				if errors[i] != nil {
					cancel()
				}
			})
		}
		wg.Wait()
		for i, err := range errors {
			if err != nil {
				return nil, err
			}
			if int64(len(packed[i])) != parts[i].Length {
				return nil, fmt.Errorf("short native tree payload")
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if loc.Base == nil {
			raw, err := wirecodec.InflateBounded(packed[0], nativeTreeBytes)
			if err != nil {
				return nil, err
			}
			return checkedNativeTree(raw, loc.Hash)
		}
		if !haveRoot {
			var err error
			root, err = wirecodec.InflateBounded(packed[1], ChunkSize)
			if err != nil {
				return nil, err
			}
			root, err = checkedNativeTree(root, loc.Base.Hash)
			if err != nil {
				return nil, err
			}
			s.idx.cache.put("tree-root/"+loc.Base.Hash, root)
		}
		work := uint64(len(root))
		bundle, err := deferredNativeBundle(packed[0])
		if err != nil {
			return nil, err
		}
		raw := root
		for i, f := range bundle.Frames {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if f.RawSize > 2*ChunkSize || len(f.Zlib) > nativeTreeBytes || work+uint64(f.RawSize) > nativeTreeWork {
				return nil, fmt.Errorf("native tree decoded work limit")
			}
			work += uint64(f.RawSize)
			program := make([]byte, int(f.RawSize))
			if err := wirecodec.Inflate(program, f.Zlib); err != nil {
				return nil, err
			}
			pos := 0
			baseSize, err := number(program, &pos)
			if err != nil || baseSize != uint64(len(raw)) {
				return nil, fmt.Errorf("native tree delta base size")
			}
			resultSize, err := number(program, &pos)
			if err != nil || resultSize > ChunkSize || work+resultSize > nativeTreeWork {
				return nil, fmt.Errorf("native tree decoded work limit")
			}
			if i == len(bundle.Frames)-1 && resultSize > nativeTreeBytes {
				return nil, fmt.Errorf("native tree target size limit")
			}
			work += resultSize
			raw, err = apply(raw, program)
			if err != nil {
				return nil, err
			}
		}
		if len(raw) > nativeTreeBytes {
			return nil, fmt.Errorf("native tree target size limit")
		}
		return checkedNativeTree(raw, loc.Hash)
	})
}

func parseNativeTree(raw []byte) ([]Dirent, error) {
	if len(raw) > nativeTreeBytes {
		return nil, fmt.Errorf("native tree size limit")
	}
	r := bufio.NewReader(bytes.NewReader(raw))
	var out []Dirent
	for {
		mode, err := readTreeMode(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		name, err := r.ReadSlice(0)
		if err != nil {
			return nil, err
		}
		name = name[:len(name)-1]
		if len(name) == 0 || len(name) > 255 || bytes.Equal(name, []byte(".")) || bytes.Equal(name, []byte("..")) || bytes.ContainsRune(name, '/') {
			return nil, fmt.Errorf("invalid native tree name")
		}
		canonical := mode & 0170000
		switch canonical {
		case 0100000:
			canonical |= 0644
			if mode&0100 != 0 {
				canonical = 0100755
			}
		case 0040000, 0120000, 0160000:
		default:
			return nil, fmt.Errorf("invalid native tree mode")
		}
		copiedName := string(name)
		var oid [20]byte
		if _, err := io.ReadFull(r, oid[:]); err != nil {
			return nil, err
		}
		e := Dirent{Name: copiedName, OID: hex.EncodeToString(oid[:]), Mode: canonical}
		if mode != canonical {
			e.RawMode = mode
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	for i := 1; i < len(out); i++ {
		if out[i-1].Name == out[i].Name {
			return nil, fmt.Errorf("duplicate native tree name")
		}
	}
	return out, nil
}
