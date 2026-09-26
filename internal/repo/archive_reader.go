package repo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"gat/internal/store"
	"strings"

	"gat/internal/archive"
	archivewire "gat/internal/archive/wire"
)

func isArchiveTree(ref pageRef) bool { return strings.HasPrefix(ref.Pack, "index/tree-archive-") }

// The enclosing catalog/page has already authenticated these bytes. DecodeRecipe
// still performs all bounded shape checks; this computed checksum is not an
// independent assertion of metadata authenticity.
func trustedArchiveRecipe(encoded []byte) (archivewire.Recipe, error) {
	if len(encoded) > archivewire.WireLimit {
		return archivewire.Recipe{}, archivewire.ErrLimit
	}
	return archivewire.DecodeRecipe(encoded, fmt.Sprintf("%x", sha256.Sum256(encoded)))
}
func archiveOID(oid string) ([20]byte, error) {
	var id [20]byte
	if len(oid) != 40 || strings.ToLower(oid) != oid {
		return id, fmt.Errorf("invalid archive object identity")
	}
	b, err := hex.DecodeString(oid)
	if err != nil {
		return id, err
	}
	copy(id[:], b)
	return id, nil
}
func checkedArchiveChunk(c chunk) (archivewire.Recipe, error) {
	if c.ArchiveRecipe == "" || c.Pack != "" || c.Offset != 0 || c.Length != 0 || c.Base != nil || !strings.HasPrefix(c.Hash, "git-sha1:") {
		return archivewire.Recipe{}, fmt.Errorf("invalid archive chunk union")
	}
	id, err := archiveOID(strings.TrimPrefix(c.Hash, "git-sha1:"))
	if err != nil {
		return archivewire.Recipe{}, err
	}
	r, err := trustedArchiveRecipe([]byte(c.ArchiveRecipe))
	if err != nil {
		return r, err
	}
	if r.TargetOID != id {
		return r, fmt.Errorf("archive chunk identity mismatch")
	}
	return r, nil
}
func checkedArchiveBlob(c chunk, oid string, size, part int64) (archivewire.Recipe, error) {
	r, err := checkedArchiveChunk(c)
	if err != nil {
		return r, err
	}
	id, err := archiveOID(oid)
	if err != nil {
		return r, err
	}
	if part != 0 || size <= 0 || size > ChunkSize || r.TargetOID != id || int64(r.Frames[len(r.Frames)-1].Size) != size {
		return r, fmt.Errorf("archive blob logical identity/size mismatch")
	}
	return r, nil
}
func archiveDataKey(kind string, oid [20]byte) string {
	return fmt.Sprintf("data/archive-%s:%x", kind, oid)
}
func (s *Snapshot) readArchiveObject(ctx context.Context, kind string, r archivewire.Recipe) ([]byte, error) {
	key := archiveDataKey(kind, r.TargetOID)
	result, err := s.idx.cache.load(ctx, key, func() ([]byte, error) {
		reads := store.NewReadScope(s.idx.store)
		defer reads.Close()
		var base *archivewire.VerifiedBase
		for i := len(r.Frames) - 1; i >= 0; i-- {
			if b, ok := s.idx.cache.get(archiveDataKey(kind, r.Frames[i].OID)); ok {
				if len(b) != int(r.Frames[i].Size) {
					return nil, fmt.Errorf("cached archive object size mismatch")
				}
				base = &archivewire.VerifiedBase{Ordinal: i, Raw: b}
				break
			}
		}
		fetch := func(ctx context.Context, segment uint64, off, n uint32) ([]byte, error) {
			b, _, err := reads.Get(ctx, archive.Key(r.ArchiveID, segment), int64(off), int64(n))
			return b, err
		}
		b, _, err := archivewire.ReadObject(ctx, kind, r, r.TargetOID, fetch, base, func(i int, b []byte) { s.idx.cache.put(archiveDataKey(kind, r.Frames[i].OID), b) })
		return b, err
	})
	if err == nil && len(result) != int(r.Frames[len(r.Frames)-1].Size) {
		return nil, fmt.Errorf("archive cached target size mismatch")
	}
	return result, err
}
func (s *Snapshot) readArchiveBlob(ctx context.Context, c chunk) ([]byte, error) {
	r, err := checkedArchiveChunk(c)
	if err != nil {
		return nil, err
	}
	return s.readArchiveObject(ctx, "blob", r)
}
func (s *Snapshot) archiveTreeEntries(ctx context.Context, oid string, o object) ([]Dirent, error) {
	if o.Kind != "tree" || o.Size < 0 || o.Size > nativeTreeBytes || o.Directory.Length <= 0 || o.Directory.Length > archivewire.WireLimit {
		return nil, fmt.Errorf("archive tree descriptor bounds")
	}
	id, err := archiveOID(oid)
	if err != nil {
		return nil, err
	}
	select {
	case nativeTreeReaders <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-nativeTreeReaders }()
	encoded, err := s.idx.pageBytes(ctx, o.Directory)
	if err != nil {
		return nil, err
	}
	r, err := trustedArchiveRecipe(encoded)
	if err != nil {
		return nil, err
	}
	if r.TargetOID != id || int64(r.Frames[len(r.Frames)-1].Size) != o.Size {
		return nil, fmt.Errorf("archive tree logical identity/size mismatch")
	}
	raw, err := s.readArchiveObject(ctx, "tree", r)
	if err != nil {
		return nil, err
	}
	return parseNativeTree(raw)
}
