package repo

import (
	"context"
	"errors"
	"strings"
	"testing"

	"gyit/internal/store"
)

// The orphan merge has one unindexed parent and one reachable/indexed parent.
// Its complete local metadata must remain usable without putting it in the
// reachable-only history accelerator.
func TestAllLocalReaderOrphanMixedHistory(t *testing.T) {
	f, merge := makeAllLocalCommitFixture(t)
	allLocalTestEnv(t, f.pack)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = importWithMetadataThreshold(t.Context(), backend, ImportOptions{Repo: f.source, CompressionWorkers: 3}, 0); err != nil {
		t.Fatal(err)
	}
	r, err := New(backend, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	s, err := r.Open(t.Context(), merge)
	if err != nil {
		t.Fatal(err)
	}
	for _, sha := range []string{merge, f.orphanCommit} {
		var position historyPosition
		if err = s.history.get(t.Context(), "g/"+sha, &position); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("orphan accelerator identity %s: %v", sha, err)
		}
	}
	var reachable historyPosition
	if err = s.history.get(t.Context(), "g/"+f.tip, &reachable); err != nil || reachable == 0 {
		t.Fatalf("reachable accelerator identity: %d: %v", reachable, err)
	}

	t.Run("mixed IDs are canonical", func(t *testing.T) {
		g := newViewGraph(s)
		id, err := g.id(t.Context(), merge)
		if err != nil || id.pos != 0 || id.sha != merge {
			t.Fatal("orphan must use SHA identity", id, err)
		}
		_, parents, err := g.node(t.Context(), id)
		if err != nil || len(parents) != 2 {
			t.Fatal("orphan merge parents", parents, err)
		}
		if parents[0].pos != 0 || parents[0].sha != f.orphanCommit || parents[1].pos != uint64(reachable) {
			t.Fatalf("ordered mixed parents must retain canonical identities: %+v", parents)
		}
	})
	for _, q := range []ViewOptions{
		{Command: "rev-list", Args: []string{"--count", merge}},
		{Command: "rev-list", Args: []string{"--count", merge, "^" + f.tip}},
		{Command: "rev-list", Args: []string{"--count", merge, f.tip}},
		{Command: "rev-list", Args: []string{"--parents", "--first-parent", merge}},
		{Command: "merge-base", Args: []string{merge, f.tip}},
		{Command: "merge-base", Args: []string{"--is-ancestor", f.tip, merge}},
		{Command: "merge-base", Args: []string{"--is-ancestor", f.orphanCommit, merge}},
		{Command: "merge-base", Args: []string{"--is-ancestor", merge, f.tip}},
	} {
		t.Run(q.Command+"/"+strings.Join(q.Args, " "), func(t *testing.T) {
			graphParity(t, r, s, f.source, q.Command, q.Args...)
		})
	}
	for _, sha := range []string{merge, f.orphanCommit, f.tip} {
		for _, path := range []string{"orphan.txt", "fast.txt"} {
			for _, firstParent := range []bool{false, true} {
				snapshot, err := r.Open(t.Context(), sha)
				if err != nil {
					t.Fatal(err)
				}
				args := []string{"log", "--format=%H"}
				if firstParent {
					args = append(args, "--first-parent")
				}
				args = append(args, sha, "--", path)
				want := strings.Fields(command(t, f.source, args...))
				var got []string
				if err = snapshot.LogPaths(t.Context(), 100, firstParent, []string{path}, func(e LogEntry) error {
					got = append(got, e.SHA)
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if strings.Join(got, " ") != strings.Join(want, " ") {
					t.Fatalf("path log %s %s first-parent=%t: got %v want %v", sha, path, firstParent, got, want)
				}
			}
		}
	}

	t.Run("history IO errors remain fatal", func(t *testing.T) {
		failure := errors.New("history read failed")
		broken := *s
		history := *s.history
		history.store = allLocalHistoryErrorStore{Store: backend, err: failure}
		history.cache = newCache(DefaultCacheBytes)
		broken.history = &history
		g := newViewGraph(&broken)
		if _, err := g.id(t.Context(), merge); !errors.Is(err, failure) {
			t.Fatal("history I/O error hidden by graph fallback", err)
		}
		// orphan.txt exists and is a file, so this reaches the accelerator.
		if _, err := broken.advanceUnchangedLog(t.Context(), merge, []string{"orphan.txt"}, &historyCursor{idx: &history}); !errors.Is(err, failure) {
			t.Fatal("history I/O error hidden by path fallback", err)
		}
		if _, _, err := g.node(t.Context(), viewGraphID{sha: merge}); !errors.Is(err, failure) {
			t.Fatal("history I/O error hidden by parent normalization", err)
		}
	})
}

type allLocalHistoryErrorStore struct {
	store.Store
	err error
}

func (s allLocalHistoryErrorStore) Get(context.Context, string, int64, int64) ([]byte, string, error) {
	return nil, "", s.err
}
