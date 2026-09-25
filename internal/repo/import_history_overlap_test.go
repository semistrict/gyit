package repo

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	storagev1 "gat/internal/gen/gat/storage/v1"
	"gat/internal/store"
	"google.golang.org/protobuf/proto"
)

type overlapStore struct {
	store.Store
	firstOID                      []byte
	historyStarted, objectWritten chan struct{}
	historyStopped                chan struct{}
	objectFailure                 error
	releaseHistory                <-chan struct{}
	historyOnce, objectOnce       sync.Once
}

type historyFailureStore struct {
	store.Store
	firstOID                     []byte
	failure                      error
	objectStarted, objectStopped chan struct{}
}

func (s *historyFailureStore) Put(ctx context.Context, key string, data []byte, condition string) error {
	if strings.HasPrefix(key, "packs/") {
		close(s.objectStarted)
		<-ctx.Done()
		close(s.objectStopped)
		return ctx.Err()
	}
	var block storagev1.HistoryBlock
	if strings.HasPrefix(key, "index/") && proto.Unmarshal(data, &block) == nil && len(block.Commits) > 0 && bytes.Equal(block.Commits[0].Oid, s.firstOID) {
		select {
		case <-s.objectStarted:
			return s.failure
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Store.Put(ctx, key, data, condition)
}

func TestImportHistoryFailureCancelsObjectWrites(t *testing.T) {
	for _, preload := range []bool{false, true} {
		name := "normal"
		if preload {
			name = "preloaded"
		}
		t.Run(name, func(t *testing.T) {
			importer := Import
			if preload {
				importer = func(ctx context.Context, backend store.Store, opt ImportOptions) (Stats, error) {
					return importWithMetadataThreshold(ctx, backend, opt, 0)
				}
			}

			source := t.TempDir()
			command(t, source, "init", "-q")
			write(t, source, "file", []byte("contents\n"))
			sha := commit(t, source)
			oid, err := hex.DecodeString(sha)
			if err != nil {
				t.Fatal(err)
			}
			local, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("history upload failed")
			storage := &historyFailureStore{Store: local, firstOID: oid, failure: injected, objectStarted: make(chan struct{}), objectStopped: make(chan struct{})}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if _, err := importer(ctx, storage, ImportOptions{Repo: source}); !errors.Is(err, injected) {
				t.Fatalf("lost history failure: %v", err)
			}
			select {
			case <-storage.objectStopped:
			default:
				t.Fatal("import returned before object writer stopped")
			}
			if _, _, err := local.Get(t.Context(), "HEAD", 0, -1); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("failed history published HEAD: %v", err)
			}
		})
	}
}

func (s *overlapStore) Put(ctx context.Context, key string, data []byte, condition string) error {
	if strings.HasPrefix(key, "index/") {
		var block storagev1.HistoryBlock
		if proto.Unmarshal(data, &block) == nil && len(block.Commits) > 0 && bytes.Equal(block.Commits[0].Oid, s.firstOID) {
			s.historyOnce.Do(func() { close(s.historyStarted) })
			select {
			case <-s.releaseHistory:
			case <-ctx.Done():
				if s.historyStopped != nil {
					close(s.historyStopped)
				}
				return ctx.Err()
			}
		}
	}
	if strings.HasPrefix(key, "packs/") {
		// A serial importer cannot get past this barrier: it starts history
		// only after this object write completes. Independent work must overlap.
		select {
		case <-s.historyStarted:
		case <-ctx.Done():
			return ctx.Err()
		}
		if s.objectFailure != nil {
			return s.objectFailure
		}
		err := s.Store.Put(ctx, key, data, condition)
		if err == nil {
			s.objectOnce.Do(func() { close(s.objectWritten) })
		}
		return err
	}
	return s.Store.Put(ctx, key, data, condition)
}

func TestImportObjectFailureCancelsHistoryBeforeReturning(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q")
	write(t, source, "file", []byte("contents\n"))
	sha := commit(t, source)
	oid, err := hex.DecodeString(sha)
	if err != nil {
		t.Fatal(err)
	}
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("object upload failed")
	storage := &overlapStore{Store: local, firstOID: oid, historyStarted: make(chan struct{}), objectWritten: make(chan struct{}), historyStopped: make(chan struct{}), releaseHistory: make(chan struct{}), objectFailure: injected}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := Import(ctx, storage, ImportOptions{Repo: source}); !errors.Is(err, injected) {
		t.Fatalf("lost object failure: %v", err)
	}
	select {
	case <-storage.historyStopped:
	default:
		t.Fatal("import returned before history writer stopped")
	}
	if _, _, err := local.Get(t.Context(), "HEAD", 0, -1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("failed object upload published HEAD: %v", err)
	}
}

func TestImportOverlapsHistoryWithoutPublishingEarly(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q")
	write(t, source, "file", []byte("contents\n"))
	sha := commit(t, source)
	oid, err := hex.DecodeString(sha)
	if err != nil {
		t.Fatal(err)
	}
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var releaseOnce sync.Once
	allowHistory := func() { releaseOnce.Do(func() { close(release) }) }
	storage := &overlapStore{Store: local, firstOID: oid, historyStarted: make(chan struct{}), objectWritten: make(chan struct{}), releaseHistory: release}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	done, result := make(chan struct{}), make(chan error, 1)
	go func() { _, err := Import(ctx, storage, ImportOptions{Repo: source}); result <- err; close(done) }()
	defer func() { cancel(); <-done }()
	defer allowHistory()
	select {
	case <-storage.objectWritten:
	case err := <-result:
		t.Fatalf("import stopped before work overlapped: %v", err)
	case <-ctx.Done():
		t.Fatal("object conversion and history indexing did not overlap")
	}
	if _, _, err := local.Get(ctx, "HEAD", 0, -1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("published before history completed: %v", err)
	}
	select {
	case err := <-result:
		t.Fatalf("import returned before history completed: %v", err)
	default:
	}
	allowHistory()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	r, _ := New(local, 64<<10)
	snap, err := r.Open(ctx, sha)
	if err != nil {
		t.Fatal(err)
	}
	graphParity(t, r, snap, source, "rev-list", "--parents", "HEAD")
}
