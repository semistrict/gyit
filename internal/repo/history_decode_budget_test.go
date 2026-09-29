//go:build !js

package repo

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gyit/internal/store"
)

type objectDecodeGate struct {
	store.Store
	active, readers, violations atomic.Int32
	entered                     chan struct{}
	release                     chan struct{}
}

func (s *objectDecodeGate) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if strings.HasPrefix(key, "packs/progressive/") {
		if s.active.Add(1) > 4 {
			s.violations.Add(1)
		}
		defer s.active.Add(-1)
		if _, local := ctx.Value(historySourceKey{}).(*historySource); !local {
			if s.readers.Add(1) > 2 {
				s.violations.Add(1)
			}
			defer s.readers.Add(-1)
		}
		s.entered <- struct{}{}
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	return s.Store.Get(ctx, key, off, n)
}

func TestObjectDecodingBoundsAcquisitionAndForeground(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for i := range 8 {
		write(t, dir, fmt.Sprintf("file-%d", i), []byte(fmt.Sprintf("contents-%d", i)))
	}
	commit(t, dir)
	command(t, dir, "repack", "-ad", "--window=0")
	ids := make([]string, 8)
	for i := range ids {
		ids[i] = command(t, dir, "rev-parse", fmt.Sprintf("HEAD:file-%d", i))
	}
	for _, mode := range []string{"foreground", "acquisition", "mixed", "foreground-disk", "acquisition-disk", "mixed-disk", "foreground-one-page-slot"} {
		t.Run(mode, func(t *testing.T) {
			backend, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			gate := &objectDecodeGate{Store: backend, entered: make(chan struct{}, 32), release: make(chan struct{})}
			var disk *store.DiskCache
			if strings.HasSuffix(mode, "-disk") {
				disk, err = store.NewDiskCache(nil, t.TempDir(), "decode-budget", 32<<20)
				if err != nil {
					t.Fatal(err)
				}
				defer disk.Close()
			}
			class := strings.TrimSuffix(mode, "-disk")
			class = strings.TrimSuffix(class, "-one-page-slot")
			p, err := NewProgressive(t.Context(), gate, disk, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
				t.Fatal(err)
			}
			// Exercise the four-slot configuration even on a smaller test machine.
			p.slots = make(chan struct{}, 4)
			if strings.HasSuffix(mode, "-one-page-slot") {
				// Force the nested-budget deadlock deterministically: an object
				// loader must leave the sole page-fetch slot free for its index.
				p.cache.slots = make(chan struct{}, 1)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			source := context.WithValue(ctx, historySourceKey{}, &historySource{cache: newCache(0)})
			var wg sync.WaitGroup
			done := make(chan error, 8)
			defer func() { cancel(); close(gate.release); wg.Wait() }()
			start := func(i int, local bool) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					request := ctx
					if local {
						request = source
					}
					data, kind, err := p.object(request, ids[i])
					if err == nil && (kind != 3 || string(data) != fmt.Sprintf("contents-%d", i)) {
						err = fmt.Errorf("incorrect object %d", i)
					}
					done <- err
				}()
			}
			wait := func(n int) {
				t.Helper()
				for range n {
					select {
					case <-gate.entered:
					case <-ctx.Done():
						t.Fatal("decoder failed to use available capacity")
					}
				}
			}
			if class == "mixed" {
				start(0, false)
				start(1, false)
				wait(2)
				for i := 2; i < 8; i++ {
					start(i, true)
				}
				wait(2)
			} else {
				for i := range ids {
					start(i, class == "acquisition")
				}
				n := 2
				if class == "acquisition" {
					n = 4
				}
				wait(n)
			}
			select {
			case <-gate.entered:
				t.Fatal("decoder exceeded concurrent object budget")
			case <-time.After(25 * time.Millisecond):
			}
			if gate.violations.Load() != 0 {
				t.Fatal("concurrent reader or acquisition bound exceeded")
			}
			// Release one blocked read at a time, preserving the chance to catch a
			// queued reader bypassing the limit as acquisition/read work mixes.
			for range 8 {
				gate.release <- struct{}{}
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if gate.violations.Load() != 0 {
				t.Fatal("queued decoding exceeded its budget")
			}
		})
	}
}
