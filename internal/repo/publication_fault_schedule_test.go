package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"gyit/internal/store"
)

// Schedule faults at semantic store boundaries, independent of random object
// names or worker arrival order. The real importer, local CAS and readers run.
// This models failed requests and lost replies, not process death or torn disk
// writes (Store requires atomic whole-object writes).
type scheduledPublicationFault struct {
	store.Store
	prefix    string
	land      bool
	fired     atomic.Bool
	headCalls atomic.Int64
	cause     error
}

func (s *scheduledPublicationFault) Put(ctx context.Context, key string, body []byte, condition string) error {
	if key == "HEAD" {
		s.headCalls.Add(1)
	}
	if strings.HasPrefix(key, s.prefix) && s.fired.CompareAndSwap(false, true) {
		if s.land {
			if err := s.Store.Put(ctx, key, body, condition); err != nil {
				return err
			}
		}
		return s.cause
	}
	return s.Store.Put(ctx, key, body, condition)
}

func TestPublicationFaultSchedule(t *testing.T) {
	for _, archive := range []bool{false, true} {
		for _, prefix := range []string{"packs/", "index/", "generations/", "HEAD"} {
			for _, land := range []bool{false, true} {
				t.Run(fmt.Sprintf("archive=%t/%s/land=%t", archive, strings.TrimSuffix(prefix, "/"), land), func(t *testing.T) {
					fixture := makeReplacementFixture(t)
					backend, err := store.NewLocal(t.TempDir())
					if err != nil {
						t.Fatal(err)
					}
					options := ImportOptions{Repo: fixture.source, TempDir: t.TempDir(), CompressionWorkers: 2, DisableDeltas: !archive}
					stats, err := Import(t.Context(), backend, options)
					if err != nil {
						t.Fatal(err)
					}
					if (stats.ImportMode == "archive") != archive {
						t.Fatalf("wrong importer: mode=%s fallback=%s", stats.ImportMode, stats.FallbackReason)
					}
					before, token, err := backend.Get(t.Context(), "HEAD", 0, -1)
					if err != nil {
						t.Fatal(err)
					}
					reader, err := New(backend, DefaultCacheBytes)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { reader.Close() })
					pinned, err := reader.Open(t.Context(), fixture.old)
					if err != nil {
						t.Fatal(err)
					}
					appendReplacementFixture(t, &fixture)
					cause := errors.New("scheduled store reply failure")
					fault := &scheduledPublicationFault{Store: backend, prefix: prefix, land: land, cause: cause}
					_, err = Import(t.Context(), fault, options)
					if !fault.fired.Load() || !errors.Is(err, cause) {
						t.Fatalf("fault not reached or error lost: fired=%t err=%v", fault.fired.Load(), err)
					}
					if prefix != "HEAD" && fault.headCalls.Load() != 0 {
						t.Fatal("published after a staging write failed")
					}
					replacementScratchEmpty(t, options.TempDir)
					// A failed reply says nothing about whether the CAS landed.
					// Our adapter knows the outcome; require that exact generation,
					// never a loose choice of old/new bytes that hides a mixed view.
					published := prefix == "HEAD" && land
					after, nextToken, err := backend.Get(t.Context(), "HEAD", 0, -1)
					if err != nil {
						t.Fatal(err)
					}
					if published {
						if bytes.Equal(before, after) || token == nextToken {
							t.Fatal("successful CAS did not select a new generation")
						}
					} else if !bytes.Equal(before, after) || token != nextToken {
						t.Fatal("failed publication changed HEAD")
					}
					assertSelected := func(wantSHA, wantBody string) {
						t.Helper()
						cold, err := New(backend, DefaultCacheBytes)
						if err != nil {
							t.Fatal(err)
						}
						defer cold.Close()
						snapshot, err := cold.OpenRevision(t.Context(), "main", "")
						if err != nil {
							t.Fatal(err)
						}
						if err := replacementRead(t.Context(), snapshot, wantSHA, wantBody); err != nil {
							t.Fatal(err)
						}
						if wantSHA == fixture.old {
							if _, err := cold.Open(t.Context(), fixture.next); !errors.Is(err, store.ErrNotFound) {
								t.Fatal("unpublished commit became visible", err)
							}
						}
					}
					if published {
						assertSelected(fixture.next, fixture.nextBody)
					} else {
						assertSelected(fixture.old, fixture.oldBody)
					}
					// Retry uses the same store, including every staged object left
					// by the fault, and must recover without mixing generations.
					if _, err := Import(t.Context(), backend, options); err != nil {
						t.Fatal("retry failed", err)
					}
					assertSelected(fixture.next, fixture.nextBody)
					if err := replacementRead(t.Context(), pinned, fixture.old, fixture.oldBody); err != nil {
						t.Fatal("pinned snapshot changed", err)
					}
					replacementScratchEmpty(t, options.TempDir)
				})
			}
		}
	}
}
