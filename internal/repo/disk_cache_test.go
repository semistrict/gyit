package repo

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"gyit/internal/store"
)

// A disconnected payload service still permits fresh publication metadata.
// This checks persisted-cache behavior at the repository API boundary.
type metadataOnlyStore struct{ store.Store }

func (s metadataOnlyStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if n >= 0 {
		return nil, "", fmt.Errorf("payload storage disconnected")
	}
	return s.Store.Get(ctx, key, off, n)
}

func TestRepositoryDiskCacheAcrossRemount(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprint("native=", native), func(t *testing.T) {
			ctx := t.Context()
			source := t.TempDir()
			command(t, source, "init", "-q")
			content := bytes.Repeat([]byte("first version of a real repository file\n"), 4096)
			write(t, source, "sub/file", content)
			first := commit(t, source)
			if native {
				command(t, source, "gc", "--prune=now")
			}
			origin, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			opts := ImportOptions{Repo: source, CompressionWorkers: 2}
			if !native {
				opts.DisableDeltas = true
			}
			if _, err := Import(ctx, origin, opts); err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			cached, err := NewDisk(origin, dir, "repo", 16<<20)
			if err != nil {
				t.Fatal(err)
			}
			read := func(r *Repository, rev string, want []byte) {
				t.Helper()
				snap, err := r.OpenRevision(ctx, rev, "")
				if err != nil {
					t.Fatal(err)
				}
				entry, err := snap.Resolve(ctx, "sub/file")
				if err != nil {
					t.Fatal(err)
				}
				got := make([]byte, len(want))
				n, err := snap.ReadAt(ctx, entry.OID, got, 0)
				if n != len(want) || !bytes.Equal(got, want) {
					t.Fatalf("read mismatch: n=%d err=%v", n, err)
				}
			}
			read(cached, first, content)
			cached.Close()
			disconnected, err := NewDisk(metadataOnlyStore{origin}, dir, "repo", 16<<20)
			if err != nil {
				t.Fatal(err)
			}
			read(disconnected, first, content)
			disconnected.Close()
			secondContent := []byte("published while a reader remains alive\n")
			cached, err = NewDisk(origin, dir, "repo", 16<<20)
			if err != nil {
				t.Fatal(err)
			}
			defer cached.Close()
			read(cached, "HEAD", content)
			write(t, source, "sub/file", secondContent)
			commit(t, source)
			if native {
				command(t, source, "gc", "--prune=now")
			}
			if _, err := Import(ctx, origin, opts); err != nil {
				t.Fatal(err)
			}
			read(cached, "HEAD", secondContent)
			read(cached, first, content)
		})
	}
}
