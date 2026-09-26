package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"gyit/internal/store"
)

type publicationStore struct {
	store.Store
	tokenOverride *string
	writes        atomic.Int64
	ready         chan<- string
	release       <-chan struct{}
}

func (s *publicationStore) Get(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	b, token, err := s.Store.Get(ctx, key, off, length)
	if key == "HEAD" && err == nil && s.tokenOverride != nil {
		token = *s.tokenOverride
	}
	return b, token, err
}

func (s *publicationStore) Put(ctx context.Context, key string, b []byte, condition string) error {
	s.writes.Add(1)
	if key == "HEAD" && s.ready != nil {
		select {
		case s.ready <- condition:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Store.Put(ctx, key, b, condition)
}

func TestImportRequiresPublicationToken(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	command(t, dir, "init", "-q")
	write(t, dir, "file", []byte("before"))
	commit(t, dir)
	s, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Import(ctx, s, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	before, _, err := s.Get(ctx, "HEAD", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	write(t, dir, "file", []byte("after"))
	commit(t, dir)
	for _, token := range []string{"", "*"} {
		t.Run(fmt.Sprintf("token=%q", token), func(t *testing.T) {
			broken := &publicationStore{Store: s, tokenOverride: &token}
			if _, err := Import(ctx, broken, ImportOptions{Repo: dir}); err == nil {
				t.Error("import must reject an existing HEAD without a usable CAS token")
			}
			if writes := broken.writes.Load(); writes != 0 {
				t.Errorf("invalid token allowed %d staging/publication writes", writes)
			}
			after, _, err := s.Get(ctx, "HEAD", 0, -1)
			if err != nil || !bytes.Equal(before, after) {
				t.Errorf("HEAD changed: %v", err)
			}
		})
	}
}

func TestConcurrentImportPublication(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%t", existing), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			dir := t.TempDir()
			command(t, dir, "init", "-q")
			write(t, dir, "file", []byte("base"))
			base := commit(t, dir)
			root := t.TempDir()
			local, err := store.NewLocal(root)
			if err != nil {
				t.Fatal(err)
			}
			r, err := New(local, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			var pinned *Snapshot
			if existing {
				if _, err := Import(ctx, local, ImportOptions{Repo: dir}); err != nil {
					t.Fatal(err)
				}
				pinned, err = r.Open(ctx, base)
				if err != nil {
					t.Fatal(err)
				}
			}
			tips := make([]string, 2)
			for i := range tips {
				command(t, dir, "checkout", "--detach", base)
				write(t, dir, "file", []byte(fmt.Sprintf("writer-%d", i)))
				tips[i] = commit(t, dir)
			}
			ready := make(chan string, 2)
			release := make(chan struct{})
			type result struct {
				writer int
				err    error
			}
			results := make(chan result, 2)
			for i := range tips {
				// Separate store instances, sharing only the on-disk object store.
				s, err := store.NewLocal(root)
				if err != nil {
					t.Fatal(err)
				}
				go func() {
					_, err := Import(ctx, &publicationStore{Store: s, ready: ready, release: release}, ImportOptions{Repo: dir, Revision: tips[i]})
					results <- result{i, err}
				}()
			}
			tokens := make([]string, 2)
			for i := range tokens {
				select {
				case tokens[i] = <-ready:
				case early := <-results:
					t.Fatalf("import failed before publication: %v", early.err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if tokens[0] != tokens[1] || tokens[0] == "" || (tokens[0] == "*") == existing {
				t.Fatalf("bad publication conditions: %q", tokens)
			}
			// Neither staged generation is visible before the atomic publication.
			for _, tip := range tips {
				if _, err := r.Open(ctx, tip); err == nil {
					t.Fatal("unpublished commit visible")
				}
			}
			close(release)
			winner, loser := -1, -1
			for range tips {
				select {
				case result := <-results:
					if result.err == nil {
						if winner != -1 {
							t.Fatal("two writers published with the same token")
						}
						winner = result.writer
					} else if errors.Is(result.err, store.ErrConflict) {
						loser = result.writer
					} else {
						t.Fatal(result.err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if winner < 0 || loser < 0 {
				t.Fatalf("winner=%d loser=%d", winner, loser)
			}
			check := func(s *Snapshot, want string) {
				t.Helper()
				entry, err := s.Resolve(ctx, "file")
				if err != nil {
					t.Fatal(err)
				}
				b := make([]byte, len(want))
				n, err := s.ReadAt(ctx, entry.OID, b, 0)
				if err != nil || n != len(b) || string(b) != want {
					t.Fatalf("read %q: %v", b, err)
				}
			}
			snapshot, err := r.Open(ctx, tips[winner])
			if err != nil {
				t.Fatal(err)
			}
			check(snapshot, fmt.Sprintf("writer-%d", winner))
			if _, err := r.Open(ctx, tips[loser]); err == nil {
				t.Fatal("losing generation became visible")
			}
			// A deliberate retry rereads HEAD and preserves the winning generation's objects.
			if _, err := Import(ctx, local, ImportOptions{Repo: dir, Revision: tips[loser]}); err != nil {
				t.Fatal(err)
			}
			for i, tip := range tips {
				s, err := r.Open(ctx, tip)
				if err != nil {
					t.Fatal(err)
				}
				check(s, fmt.Sprintf("writer-%d", i))
			}
			check(snapshot, fmt.Sprintf("writer-%d", winner))
			if pinned != nil {
				check(pinned, "base")
			}
		})
	}
}
