package repo

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	storagev1 "gat/internal/gen/gat/storage/v1"
	"gat/internal/store"
	"github.com/klauspost/compress/zstd"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

const directoryPageBytes = 64 << 10
const directorySortEntries = 4096

// Directory pages have no tree-ID prefix. Identical pages in different tree
// versions share one range; the disk-backed hash table bounds importer memory.
type directoryWriter struct {
	ceilingCounts ceilingValidationCounts
	reader        *bufio.Reader
	// Entry storage is reused only after a tree has been encoded, or its
	// current batch has been copied into the wide-directory spill file.
	// Both slices stay bounded by directorySortEntries; sorting never changes
	// pool positions. No published record retains these mutable buffers.
	entryPool               []*storagev1.NamedEntry
	entryOrder              []*storagev1.NamedEntry
	pageBuffer, frameBuffer []byte
	pages                   *indexWriter
	encoder                 *zstd.Encoder
	stage                   *stage
	sizes                   *blobSizes
	old                     *index
	tmp                     string
}

func (w *directoryWriter) fileSize(ctx context.Context, oid []byte) (int64, error) {
	if n, found, err := w.sizes.get(oid); err != nil || found {
		return n, err
	}
	var o object
	if err := w.old.get(ctx, "o/"+hex.EncodeToString(oid), &o); err != nil {
		return 0, err
	}
	if o.Kind != "blob" {
		return 0, fmt.Errorf("tree file references non-blob")
	}
	return o.Size, nil
}

func (w *directoryWriter) save(p *storagev1.DirectoryPage) (*storagev1.DirectoryChild, error) {
	if err := w.pages.ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).MarshalAppend(w.pageBuffer[:0], p)
	if err != nil {
		return nil, err
	}
	if len(raw) > directoryPageBytes {
		return nil, fmt.Errorf("directory page exceeds limit")
	}
	w.pageBuffer = raw
	hash := sha256.Sum256(raw)
	ref, found, err := w.stage.directoryPage(hash)
	if err != nil {
		return nil, err
	}
	if !found {
		w.frameBuffer = w.encoder.EncodeAll(raw, w.frameBuffer[:0])
		// A writer owns its pack, so it can upload before taking the shared
		// registry lock. The second lookup keeps duplicate pages canonical.
		if len(w.pages.data)+len(w.frameBuffer) > indexPackSize {
			if err := w.pages.flush(); err != nil {
				return nil, err
			}
		}
		ref, err = w.stage.saveDirectoryPage(hash, func() (pageRef, error) { return w.pages.saveBytes(w.frameBuffer) })
		if err != nil {
			return nil, err
		}
	}
	var max []byte
	if len(p.Entries) > 0 {
		max = p.Entries[len(p.Entries)-1].Name
	}
	if len(p.Children) > 0 {
		max = p.Children[len(p.Children)-1].MaxName
	}
	return &storagev1.DirectoryChild{MaxName: max, Page: encodePageRef(ref)}, nil
}

// Git sorts directories as name+"/", whereas ReadDir sorts by raw name. Small
// trees sort in memory; unusually wide trees spill to bounded disk transactions.
type directoryBuilder struct {
	writer  *directoryWriter
	entries []*storagev1.NamedEntry
	spill   *bolt.DB
	path    string
}

func (b *directoryBuilder) close() {
	if b.spill != nil {
		b.spill.Close()
		os.Remove(b.path)
		b.spill = nil
	}
}

func (b *directoryBuilder) add(e *storagev1.NamedEntry) error {
	b.entries = append(b.entries, e)
	if len(b.entries) < directorySortEntries {
		return nil
	}
	return b.spillEntries()
}

func (b *directoryBuilder) spillEntries() error {
	if b.spill == nil {
		b.path = filepath.Join(b.writer.tmp, "directory-sort.db")
		var err error
		b.spill, err = bolt.Open(b.path, 0600, &bolt.Options{NoSync: true})
		if err != nil {
			return err
		}
	}
	err := b.spill.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("entries"))
		if err != nil {
			return err
		}
		for _, entry := range b.entries {
			if err := b.writer.pages.ctx.Err(); err != nil {
				return err
			}
			if bucket.Get(entry.Name) != nil {
				return fmt.Errorf("duplicate directory name")
			}
			raw, err := proto.Marshal(entry)
			if err != nil {
				return err
			}
			if err := bucket.Put(entry.Name, raw); err != nil {
				return err
			}
		}
		return nil
	})
	b.entries = b.entries[:0]
	return err
}

func (b *directoryBuilder) finish() (pageRef, error) {
	defer b.close()
	var leaf []*storagev1.NamedEntry
	var levels [][]*storagev1.DirectoryChild
	var add func(int, *storagev1.DirectoryChild) error
	add = func(level int, child *storagev1.DirectoryChild) error {
		for len(levels) <= level {
			levels = append(levels, nil)
		}
		levels[level] = append(levels[level], child)
		if len(levels[level]) < fanout {
			return nil
		}
		parent, err := b.writer.save(&storagev1.DirectoryPage{Children: levels[level]})
		if err != nil {
			return err
		}
		levels[level] = nil
		return add(level+1, parent)
	}
	flush := func() error {
		child, err := b.writer.save(&storagev1.DirectoryPage{Entries: leaf})
		if err != nil {
			return err
		}
		leaf = leaf[:0]
		return add(0, child)
	}
	var previous []byte
	emit := func(e *storagev1.NamedEntry) error {
		if bytes.Compare(e.Name, previous) <= 0 {
			return fmt.Errorf("duplicate or unordered directory name")
		}
		previous = e.Name
		leaf = append(leaf, e)
		if len(leaf) == fanout {
			return flush()
		}
		return nil
	}
	if b.spill != nil {
		if err := b.spillEntries(); err != nil {
			return pageRef{}, err
		}
		if err := b.spill.View(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte("entries")).ForEach(func(k, v []byte) error {
				var entry storagev1.NamedEntry
				if err := proto.Unmarshal(v, &entry); err != nil {
					return err
				}
				return emit(&entry)
			})
		}); err != nil {
			return pageRef{}, err
		}
	} else {
		sort.Slice(b.entries, func(i, j int) bool { return bytes.Compare(b.entries[i].Name, b.entries[j].Name) < 0 })
		for _, e := range b.entries {
			if err := emit(e); err != nil {
				return pageRef{}, err
			}
		}
	}
	if len(leaf) > 0 || len(levels) == 0 {
		if err := flush(); err != nil {
			return pageRef{}, err
		}
	}
	for level := 0; level < len(levels); level++ {
		if len(levels[level]) == 0 {
			continue
		}
		if level == len(levels)-1 && len(levels[level]) == 1 {
			return decodePageRef(levels[level][0].Page), nil
		}
		parent, err := b.writer.save(&storagev1.DirectoryPage{Children: levels[level]})
		if err != nil {
			return pageRef{}, err
		}
		levels[level] = nil
		if err := add(level+1, parent); err != nil {
			return pageRef{}, err
		}
	}
	return pageRef{}, fmt.Errorf("missing directory root")
}

func (idx *index) directoryPage(ctx context.Context, ref pageRef) (*storagev1.DirectoryPage, error) {
	if !strings.HasPrefix(ref.Pack, "index/") || ref.Offset < 0 || ref.Length <= 0 || ref.Length > directoryPageBytes+1024 || ref.Offset > indexPackSize-ref.Length {
		return nil, fmt.Errorf("invalid directory page range")
	}
	hash, err := hex.DecodeString(ref.Hash)
	if err != nil || len(hash) != sha256.Size {
		return nil, fmt.Errorf("invalid directory page hash")
	}
	// Cache decoded bytes, so the configured budget accounts for expansion.
	raw, err := idx.cache.load(ctx, "directory/"+ref.Hash, func() ([]byte, error) {
		data, _, err := idx.store.Get(ctx, ref.Pack, ref.Offset, ref.Length)
		if err != nil {
			return nil, err
		}
		actual := sha256.Sum256(data)
		if int64(len(data)) != ref.Length || !bytes.Equal(actual[:], hash) {
			return nil, fmt.Errorf("directory checksum mismatch")
		}
		return decodeFrame(data, directoryPageBytes)
	})
	if err != nil {
		return nil, err
	}
	var p storagev1.DirectoryPage
	if err := proto.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	if len(p.Entries) > fanout || len(p.Children) > fanout || (len(p.Entries) > 0 && len(p.Children) > 0) {
		return nil, fmt.Errorf("invalid directory page shape")
	}
	previous := ""
	for _, e := range p.Entries {
		name := string(e.Name)
		if name <= previous || len(name) > 255 || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") || (len(e.Oid) != 20 && len(e.Oid) != 32) || e.Size < 0 {
			return nil, fmt.Errorf("invalid directory entry")
		}
		switch e.Mode {
		case 0040000, 0100644, 0100755, 0120000, 0160000:
		default:
			return nil, fmt.Errorf("invalid directory mode")
		}
		previous = name
	}
	previous = ""
	for _, c := range p.Children {
		if string(c.MaxName) <= previous || len(c.MaxName) > 255 || c.Page == nil {
			return nil, fmt.Errorf("invalid directory routing page")
		}
		previous = string(c.MaxName)
	}
	return &p, nil
}

func directoryEntry(e *storagev1.NamedEntry) Entry {
	return Entry{Name: string(e.Name), OID: hex.EncodeToString(e.Oid), Mode: e.Mode, Size: e.Size, RawMode: e.RawMode}
}

func (idx *index) lookupDirectory(ctx context.Context, ref pageRef, name string) (Entry, error) {
	for depth := 0; depth < 16; depth++ {
		p, err := idx.directoryPage(ctx, ref)
		if err != nil {
			return Entry{}, err
		}
		if len(p.Children) == 0 {
			i := sort.Search(len(p.Entries), func(i int) bool { return string(p.Entries[i].Name) >= name })
			if i == len(p.Entries) || string(p.Entries[i].Name) != name {
				return Entry{}, store.ErrNotFound
			}
			return directoryEntry(p.Entries[i]), nil
		}
		i := sort.Search(len(p.Children), func(i int) bool { return string(p.Children[i].MaxName) >= name })
		if i == len(p.Children) {
			return Entry{}, store.ErrNotFound
		}
		ref = decodePageRef(p.Children[i].Page)
	}
	return Entry{}, fmt.Errorf("directory exceeds depth limit")
}

func (idx *index) readDirectory(ctx context.Context, ref pageRef, after string, limit int) ([]Entry, error) {
	var result []Entry
	var visit func(pageRef, int) error
	visit = func(ref pageRef, depth int) error {
		if depth >= 16 {
			return fmt.Errorf("directory exceeds depth limit")
		}
		p, err := idx.directoryPage(ctx, ref)
		if err != nil {
			return err
		}
		for _, e := range p.Entries {
			if string(e.Name) > after {
				result = append(result, directoryEntry(e))
				if len(result) == limit {
					return nil
				}
			}
		}
		for _, c := range p.Children {
			if string(c.MaxName) <= after {
				continue
			}
			if err := visit(decodePageRef(c.Page), depth+1); err != nil {
				return err
			}
			if len(result) == limit {
				break
			}
		}
		return nil
	}
	err := visit(ref, 0)
	return result, err
}

func (w *directoryWriter) readTree(body io.Reader, oidBytes int) (pageRef, error) {
	w.ceilingCounts = ceilingValidationCounts{}
	builder := &directoryBuilder{writer: w, entries: w.entryOrder[:0]}
	defer func() { w.entryOrder = builder.entries[:0] }()
	defer builder.close()
	if w.reader == nil {
		w.reader = bufio.NewReader(body)
	} else {
		w.reader.Reset(body)
	}
	tr := w.reader
	for {
		mode, err := readTreeMode(tr)
		if err == io.EOF {
			break
		}
		if err != nil {
			return pageRef{}, err
		}
		name, err := tr.ReadSlice(0)
		if err != nil {
			return pageRef{}, err
		}
		name = name[:len(name)-1]
		if len(name) == 0 || bytes.Equal(name, []byte(".")) || bytes.Equal(name, []byte("..")) || bytes.ContainsRune(name, '/') || len(name) > 255 {
			return pageRef{}, fmt.Errorf("unsupported tree entry name %q", name)
		}
		// Git normalizes historical permission bits when traversing trees.
		// Only the owner's execute bit distinguishes regular file modes.
		canonical := mode & 0170000
		switch canonical {
		case 0100000:
			canonical |= 0644
			if mode&0100 != 0 {
				canonical = 0100755
			}
		case 0040000, 0120000, 0160000:
		default:
			return pageRef{}, fmt.Errorf("unsupported tree mode %o", mode)
		}
		position := len(builder.entries)
		if position == len(w.entryPool) {
			w.entryPool = append(w.entryPool, &storagev1.NamedEntry{})
		}
		e := w.entryPool[position]
		nameBuffer, oidBuffer := e.Name[:0], e.Oid[:0]
		e.Reset()
		e.Name = append(nameBuffer, name...)
		e.Oid = append(oidBuffer, make([]byte, oidBytes)...)
		e.Mode = uint32(canonical)
		child := e.Oid
		if _, err := io.ReadFull(tr, child); err != nil {
			return pageRef{}, err
		}
		if mode != canonical {
			e.RawMode = uint32(mode)
		}
		if canonical != 0040000 && canonical != 0160000 {
			w.ceilingCounts.blobLookups++
			e.Size, err = w.fileSize(w.pages.ctx, child)
			if err != nil {
				return pageRef{}, fmt.Errorf("resolve tree entry %x: %w", child, err)
			}
		}
		w.ceilingCounts.entries++
		if err := builder.add(e); err != nil {
			return pageRef{}, err
		}
	}

	return builder.finish()
}

// Parse directly from the reader's window. Long leading-zero modes remain
// valid without retaining their text; overflow and partial entries are errors.
func readTreeMode(r *bufio.Reader) (uint32, error) {
	var mode uint32
	seen := false
	for {
		part, err := r.ReadSlice(' ')
		if err == nil {
			part = part[:len(part)-1]
		}
		for _, c := range part {
			if c < '0' || c > '7' || mode > (^uint32(0)-uint32(c-'0'))/8 {
				return 0, fmt.Errorf("invalid tree mode")
			}
			mode = mode*8 + uint32(c-'0')
			seen = true
		}
		switch err {
		case nil:
			if !seen {
				return 0, fmt.Errorf("empty tree mode")
			}
			return mode, nil
		case bufio.ErrBufferFull:
			continue
		case io.EOF:
			if seen {
				return 0, io.ErrUnexpectedEOF
			}
			return 0, io.EOF
		default:
			return 0, err
		}
	}
}
