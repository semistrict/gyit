package repo

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"strings"

	sizewire "gyit/internal/globalsizes/wire"
)

const globalReaderFormat = 9013

// The identity belongs to the immutable main index pinned by this Snapshot.
// Derived history/diff snapshots share that same index, hence the same binding.
func (s *Snapshot) globalBinding() (*globalSizeSlot, globalSizeRef) {
	if s.globalSizes != nil {
		return s.globalSizes, s.globalSizeRef
	}
	if s.idx != nil {
		return s.idx.globalSizes, s.idx.globalSizeRef
	}
	return nil, globalSizeRef{}
}

func (r *Repository) bindGlobalIndex(ctx context.Context, m manifest, idx *index) error {
	if r.readerOwner != nil {
		return r.readerOwner.bindGlobalIndex(ctx, m, idx)
	}
	if !manifestHasGlobalSizes(m.Version) {
		return nil
	}
	if m.Format != "sha1" {
		return fmt.Errorf("global size format requires SHA1")
	}
	r.globalMu.Lock()
	if r.configuredCacheBytes < DefaultCacheBytes {
		r.globalMu.Unlock()
		return fmt.Errorf("global size format requires at least32MiB configured cache")
	}
	if r.globalSizes == nil {
		// Shrink the existing shared object in place: old snapshots must not retain
		// a separate32MiB cache beside the new16MiB reservation.
		r.cache.mu.Lock()
		if r.cache.disk == nil {
			r.cache.max = globalOrdinaryBudget
		}
		for r.cache.used > r.cache.max {
			e := r.cache.lru.Back()
			v := e.Value.(cached)
			delete(r.cache.items, v.key)
			r.cache.used -= len(v.data)
			r.cache.lru.Remove(e)
		}
		r.cache.mu.Unlock()
		r.globalSizes = newGlobalSizeSlot(r.store)
		r.globalSizes.disk = r.cache.disk
	}
	slot := r.globalSizes
	r.globalMu.Unlock()
	var c chunk
	if err := idx.get(ctx, "x/global-sizes", &c); err != nil {
		return fmt.Errorf("global size pointer: %w", err)
	}
	if c.ArchiveRecipe != "" || c.Base != nil || !strings.HasPrefix(c.Pack, "index/global-sizes-") || c.Offset < 0 || c.Length < 0 || c.Length > int64(globalTableBudget-sizewire.ReaderMetadataBytes) || c.Offset > math.MaxInt64-c.Length || len(c.Hash) != 64 || strings.ToLower(c.Hash) != c.Hash {
		return fmt.Errorf("invalid global size pointer")
	}
	if _, err := hex.DecodeString(c.Hash); err != nil {
		return fmt.Errorf("invalid global size checksum")
	}
	ref := globalSizeRef{Key: c.Pack, Hash: c.Hash, Offset: c.Offset, Length: c.Length}
	if err := slot.with(ctx, ref, func(*sizewire.Table) error { return nil }); err != nil {
		return err
	}
	idx.globalSizes, idx.globalSizeRef = slot, ref
	return nil
}
