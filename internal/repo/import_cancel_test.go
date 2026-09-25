package repo

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"gat/internal/store"
)

func TestImportCancellationWithUnreadSourceObjectsDoesNotPublish(t *testing.T) {
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
			random := rand.New(rand.NewSource(431))
			for i := 0; i < 32; i++ {
				data := make([]byte, 256<<10)
				random.Read(data)
				write(t, source, fmt.Sprintf("file-%03d", i), data)
			}
			commit(t, source)
			local, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := importer(ctx, local, ImportOptions{Repo: source, Progress: func(s Stats) {
					if s.Objects == 1 {
						cancel()
					}
				}})
				result <- err
			}()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation returned %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("import did not release queued source objects after cancellation")
			}
			if _, _, err := local.Get(t.Context(), "HEAD", 0, -1); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("canceled import published HEAD: %v", err)
			}
		})
	}
}
