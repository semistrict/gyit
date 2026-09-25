package repo

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	sizewire "gat/internal/globalsizes/wire"
	"gat/internal/store"
)

func globalTestData(t *testing.T, id [20]byte, size uint32) []byte {
	t.Helper()
	bits := make([]byte, 8)
	binary.LittleEndian.PutUint64(bits, 1<<(sizewire.HashOID(id, 7)%64))
	values := make([]byte, 4)
	binary.LittleEndian.PutUint32(values, size)
	b, err := sizewire.Encode(sizewire.Payload{Count: 1, Levels: []sizewire.Level{{Seed: 7, BitCount: 64, BitmapLE64: bits, RanksLE32: make([]byte, 4)}}, SizesLE32: values})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func globalTestRef(t *testing.T, s store.Store, key string, b []byte) globalSizeRef {
	t.Helper()
	if err := s.Put(t.Context(), key, b, ""); err != nil {
		t.Fatal(err)
	}
	return globalSizeRef{Key: key, Hash: fmt.Sprintf("%x", sha256.Sum256(b)), Length: int64(len(b))}
}

func TestGlobalSizeSnapshotAndPressure(t *testing.T) {
	ctx := t.Context()
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var id [20]byte
	id[19] = 9
	ref := globalTestRef(t, backend, "index/global-sizes-tiny", globalTestData(t, id, 314))
	slot := newGlobalSizeSlot(backend)
	raw := append([]byte("100644 file\x00"), id[:]...)
	oid, directory := lazyTreeFixtureDescriptor(t, ctx, backend, raw)
	root := lazyFixtureMainRoot(t, ctx, backend, "global-size-tiny-main", oid, len(raw), directory)
	cache := newCache(globalOrdinaryBudget)
	s := &Snapshot{idx: &index{store: backend, cache: cache, root: root}, Tree: oid, globalSizes: slot, globalSizeRef: ref}
	want := Entry{Name: "file", OID: hex.EncodeToString(id[:]), Mode: 0100644, Size: 314}
	for round := 0; round < 3; round++ {
		for i := 0; i < 24; i++ {
			cache.put(fmt.Sprintf("pressure-%d-%d", round, i), make([]byte, 1<<20))
		}
		page, err := s.ReadDir(ctx, oid, "", 128)
		if err != nil || !reflect.DeepEqual(page, []Entry{want}) {
			t.Fatalf("page=%v err=%v", page, err)
		}
		got, err := s.Lookup(ctx, oid, "file")
		if err != nil || got != want {
			t.Fatalf("lookup=%v err=%v", got, err)
		}
		cache.mu.Lock()
		used := cache.used
		cache.mu.Unlock()
		if uint64(used)+slot.charged.Load() > DefaultCacheBytes {
			t.Fatal("combined retained budget exceeded")
		}
	}
	if slot.loads.Load() != 1 || slot.verified.Load() != 1 {
		t.Fatal("pinned table refetched or reverified")
	}
	if err := slot.clear(ctx); err != nil || slot.charged.Load() != 0 {
		t.Fatal("slot not released", err)
	}
}

func TestGlobalSizeLeaseAndGeneration(t *testing.T) {
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var id [20]byte
	a := globalTestRef(t, backend, "index/global-sizes-a", globalTestData(t, id, 11))
	b := globalTestRef(t, backend, "index/global-sizes-b", globalTestData(t, id, 22))
	slot := newGlobalSizeSlot(backend)
	entered, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- slot.with(t.Context(), a, func(table *sizewire.Table) error {
			close(entered)
			<-release
			n, err := table.LookupKnown(id)
			if err == nil && n != 11 {
				return fmt.Errorf("old generation changed")
			}
			return err
		})
	}()
	<-entered
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := slot.with(ctx, b, func(*sizewire.Table) error { return fmt.Errorf("canceled callback called") }); !errors.Is(err, context.Canceled) {
		t.Fatal("waiting cancellation", err)
	}
	if slot.loads.Load() != 1 {
		t.Fatal("replacement loaded while old lease active")
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := slot.with(t.Context(), b, func(table *sizewire.Table) error {
		n, err := table.LookupKnown(id)
		if n != 22 {
			return fmt.Errorf("new generation=%d", n)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if slot.loads.Load() != 2 || slot.verified.Load() != 2 || slot.peak.Load() > globalTableBudget {
		t.Fatal("generation accounting")
	}
}

type globalOversizedStore struct {
	store.Store
	data []byte
}

func (s globalOversizedStore) Get(context.Context, string, int64, int64) ([]byte, string, error) {
	return s.data, "", nil
}
func TestGlobalSizeCapacityChecksumAndDirSkip(t *testing.T) {
	var id [20]byte
	b := globalTestData(t, id, 7)
	ref := globalSizeRef{Key: "index/global-sizes-test", Hash: fmt.Sprintf("%x", sha256.Sum256(b)), Length: int64(len(b))}
	expanded := make([]byte, len(b), globalTableBudget-sizewire.ReaderMetadataBytes+1)
	copy(expanded, b)
	slot := newGlobalSizeSlot(globalOversizedStore{data: expanded})
	if err := slot.with(t.Context(), ref, func(*sizewire.Table) error { return nil }); err == nil || slot.charged.Load() != 0 {
		t.Fatal("excess payload capacity accepted")
	}
	slot = newGlobalSizeSlot(globalOversizedStore{data: b})
	ref.Hash = fmt.Sprintf("%064x", 1)
	if err := slot.with(t.Context(), ref, func(*sizewire.Table) error { return nil }); err == nil || slot.verified.Load() != 0 || slot.charged.Load() != 0 {
		t.Fatal("corruption retained")
	}
	snapshot := &Snapshot{globalSizes: slot, globalSizeRef: ref}
	dirs := []Dirent{{Name: "dir", Mode: 0040000}, {Name: "submodule", Mode: 0160000}}
	if got, err := snapshot.hydrateDirents(t.Context(), dirs); err != nil || len(got) != 2 || got[0].Size != 0 || got[1].Size != 0 {
		t.Fatal("directory skip", got, err)
	}
	if slot.loads.Load() != 1 {
		t.Fatal("directory-only page fetched sizes")
	}
}

func TestGlobalSizeConcurrentLease(t *testing.T) {
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var id [20]byte
	ref := globalTestRef(t, backend, "index/global-sizes-concurrent", globalTestData(t, id, 77))
	slot := newGlobalSizeSlot(backend)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			if err := slot.with(t.Context(), ref, func(table *sizewire.Table) error {
				n, err := table.LookupKnown(id)
				if n != 77 {
					return fmt.Errorf("size=%d", n)
				}
				return err
			}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if slot.loads.Load() != 1 || slot.verified.Load() != 1 {
		t.Fatal("duplicate table load")
	}
}
