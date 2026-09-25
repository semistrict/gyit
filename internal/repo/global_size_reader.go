package repo

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"sync/atomic"

	sizewire "gat/internal/globalsizes/wire"
	"gat/internal/store"
)

const globalTableBudget = 16 << 20
const globalOrdinaryBudget = DefaultCacheBytes - globalTableBudget

type globalSizeRef struct {
	Key, Hash      string
	Offset, Length int64
}

// One exclusively leased generation. A replacement cannot retain the preceding
// generation or expose a table pointer beyond the callback's lifetime. The
// entire slot is reserved separately from the ordinary16MiB byte cache.
type globalSizeSlot struct {
	store                          store.Store
	gate                           chan struct{}
	ref                            globalSizeRef
	table                          *sizewire.Table
	charged, peak, loads, verified atomic.Uint64
}

func newGlobalSizeSlot(backend store.Store) *globalSizeSlot {
	return &globalSizeSlot{store: backend, gate: make(chan struct{}, 1)}
}
func (s *globalSizeSlot) with(ctx context.Context, ref globalSizeRef, fn func(*sizewire.Table) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-s.gate }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.table == nil || s.ref != ref {
		// Release old borrowed views before allocating the next generation. Ordinary
		// LRU eviction cannot release this charged slot or leave an uncharged pin.
		s.table = nil
		s.ref = globalSizeRef{}
		s.charged.Store(0)
		if ref.Key == "" || ref.Offset < 0 || ref.Length < 0 || ref.Length > int64(globalTableBudget-sizewire.ReaderMetadataBytes) || ref.Offset > math.MaxInt64-ref.Length {
			return fmt.Errorf("global table descriptor exceeds slot")
		}
		data, _, err := s.store.Get(ctx, ref.Key, ref.Offset, ref.Length)
		s.loads.Add(1)
		if err != nil {
			return err
		}
		if int64(len(data)) != ref.Length || cap(data) > globalTableBudget-sizewire.ReaderMetadataBytes {
			return fmt.Errorf("global table payload capacity exceeds slot")
		}
		table, err := sizewire.DecodeKnown(data, ref.Hash)
		if err != nil {
			return err
		}
		retained := uint64(cap(data)) + table.OwnedBytes()
		if retained > globalTableBudget {
			return fmt.Errorf("global table retained bytes exceed slot")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		s.table, s.ref = table, ref
		s.charged.Store(retained)
		for old := s.peak.Load(); retained > old && !s.peak.CompareAndSwap(old, retained); old = s.peak.Load() {
		}
		s.verified.Add(1)
	}
	return fn(s.table)
}
func (s *globalSizeSlot) clear(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-s.gate }()
	if err := ctx.Err(); err != nil {
		return err
	}
	s.table = nil
	s.ref = globalSizeRef{}
	s.charged.Store(0)
	return nil
}
func globalKnownEntry(table *sizewire.Table, d Dirent) (Entry, error) {
	e := Entry{Name: d.Name, OID: d.OID, Mode: d.Mode, RawMode: d.RawMode}
	if d.Mode == 0040000 || d.Mode == 0160000 {
		return e, nil
	}
	var id [20]byte
	if len(d.OID) != 40 {
		return Entry{}, fmt.Errorf("invalid blob identity")
	}
	n, err := hex.Decode(id[:], []byte(d.OID))
	if err != nil || n != len(id) {
		return Entry{}, fmt.Errorf("invalid blob identity")
	}
	e.Size, err = table.LookupKnown(id)
	return e, err
}
func (s *Snapshot) globalHydrateDirent(ctx context.Context, d Dirent) (Entry, error) {
	slot, ref := s.globalBinding()
	if slot == nil {
		return Entry{}, fmt.Errorf("missing global size binding")
	}
	e := Entry{Name: d.Name, OID: d.OID, Mode: d.Mode, RawMode: d.RawMode}
	if d.Mode == 0040000 || d.Mode == 0160000 {
		return e, nil
	}
	err := slot.with(ctx, ref, func(table *sizewire.Table) error { var err error; e, err = globalKnownEntry(table, d); return err })
	if err != nil {
		return Entry{}, err
	}
	return e, nil
}
func (s *Snapshot) globalHydrateDirents(ctx context.Context, names []Dirent) ([]Entry, error) {
	slot, ref := s.globalBinding()
	if slot == nil {
		return nil, fmt.Errorf("missing global size binding")
	}
	out := make([]Entry, len(names))
	needed := false
	for i, d := range names {
		if d.Mode != 0040000 && d.Mode != 0160000 {
			needed = true
		}
		out[i] = Entry{Name: d.Name, OID: d.OID, Mode: d.Mode, RawMode: d.RawMode}
	}
	if !needed {
		return out, ctx.Err()
	}
	err := slot.with(ctx, ref, func(table *sizewire.Table) error {
		for i, d := range names {
			if err := ctx.Err(); err != nil {
				return err
			}
			e, err := globalKnownEntry(table, d)
			if err != nil {
				return err
			}
			out[i] = e
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
