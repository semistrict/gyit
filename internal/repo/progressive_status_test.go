//go:build !js

package repo

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"gyit/internal/store"
)

type statePublicationGate struct {
	store.Store
	enabled          atomic.Bool
	heads            atomic.Int64
	started, release chan struct{}
	err              error
}

func (s *statePublicationGate) Put(ctx context.Context, key string, data []byte, condition string) error {
	if key == "HEAD" {
		s.heads.Add(1)
		if s.enabled.Load() {
			close(s.started)
			select {
			case <-s.release:
			case <-ctx.Done():
				return ctx.Err()
			}
			if s.err != nil {
				return s.err
			}
		}
	}
	return s.Store.Put(ctx, key, data, condition)
}

func TestProgressiveStateUsesDataPublication(t *testing.T) {
	for _, phase := range []string{"snapshot", "history"} {
		for _, fail := range []bool{false, true} {
			name := phase + "/success"
			if fail {
				name = phase + "/failed-CAS"
			}
			t.Run(name, func(t *testing.T) {
				source := t.TempDir()
				command(t, source, "init", "-qb", "main")
				write(t, source, "file", []byte("contents\n"))
				commit(t, source)
				sha := command(t, source, "rev-parse", "HEAD")
				command(t, source, "repack", "-ad")
				gitdir := filepath.Join(source, ".git")
				backend, err := store.NewLocal(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				gate := &statePublicationGate{Store: backend, started: make(chan struct{}), release: make(chan struct{})}
				if fail {
					gate.err = store.ErrConflict
				}
				p, err := NewProgressive(t.Context(), gate, nil, t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				if err := p.ImportPacks(t.Context(), gitdir); err != nil {
					t.Fatal(err)
				}
				old, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				check := func(reader *Progressive, complete bool) {
					t.Helper()
					state, err := reader.State(t.Context(), sha)
					if err != nil || state.SnapshotComplete != (complete && phase == "snapshot") || state.HistoryComplete != (complete && phase == "history") {
						t.Fatalf("completion disagrees with published data: %v %v", state, err)
					}
				}
				check(old, false)
				before := gate.heads.Load()
				gate.enabled.Store(true)
				ctx, cancel := context.WithCancel(t.Context())
				done, stopped := make(chan error, 1), make(chan struct{})
				defer func() { cancel(); <-stopped }()
				go func() {
					defer close(stopped)
					if phase == "snapshot" {
						done <- p.PrepareSnapshot(ctx, sha)
					} else {
						done <- p.IngestHistory(ctx, sha, gitdir)
					}
				}()
				select {
				case <-gate.started:
				case <-time.After(3 * time.Second):
					t.Fatal("data publication did not start")
				}
				check(p, false)
				check(old, false)
				close(gate.release)
				if err := <-done; !errors.Is(err, gate.err) {
					t.Fatalf("data publication: %v", err)
				}
				check(p, !fail)
				check(old, false)
				reader, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				check(reader, !fail)
				if heads := gate.heads.Load() - before; heads != 1 {
					t.Fatalf("completion required %d HEAD writes; want the data CAS alone", heads)
				}
			})
		}
	}
}

func TestProgressiveStateReusesPreparedTree(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-qb", "main")
	write(t, source, "file", []byte("contents\n"))
	commit(t, source)
	old := command(t, source, "rev-parse", "HEAD")
	command(t, source, "repack", "-ad")
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gitdir := filepath.Join(source, ".git")
	if err := p.ImportPacks(t.Context(), gitdir); err != nil {
		t.Fatal(err)
	}
	if err := p.PrepareSnapshot(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	if err := p.IngestHistory(t.Context(), old, gitdir); err != nil {
		t.Fatal(err)
	}
	command(t, source, "-c", "user.name=Test", "-c", "user.email=test@example.test", "commit", "--allow-empty", "-qm", "same snapshot")
	sha := command(t, source, "rev-parse", "HEAD")
	command(t, source, "repack", "-ad")
	if err := p.ImportPacks(t.Context(), gitdir); err != nil {
		t.Fatal(err)
	}
	state, err := p.State(t.Context(), sha)
	if err != nil || !state.SnapshotComplete || state.HistoryComplete {
		t.Fatalf("new revision must reuse the prepared tree without inventing history coverage: %v %v", state, err)
	}
	state, err = p.State(t.Context(), old)
	if err != nil || !state.SnapshotComplete || !state.HistoryComplete {
		t.Fatalf("new revision changed old completion state: %v %v", state, err)
	}
}
