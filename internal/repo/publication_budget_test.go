//go:build !js

package repo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gyit/internal/store"
)

type heldPublicationStore struct {
	store.Store
	started chan int
}

func (s heldPublicationStore) Put(ctx context.Context, _ string, data []byte, _ string) error {
	s.started <- len(data)
	<-ctx.Done()
	return ctx.Err()
}

func TestPublicationUploadBudgets(t *testing.T) {
	for _, tc := range []struct {
		name         string
		size, active int
	}{
		{"small objects use available bytes", 256 << 10, 12},
		{"large objects retain 24 MiB bound", 8 << 20, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := heldPublicationStore{started: make(chan int, 16)}
			u := newPublicationUploads(t.Context(), s)
			defer u.close()
			done := make(chan error, 1)
			go func() {
				data := make([]byte, tc.size)
				for i := 0; i <= tc.active; i++ {
					if err := u.Put(t.Context(), fmt.Sprintf("index/%d", i), data, ""); err != nil {
						done <- err
						return
					}
				}
				done <- nil
			}()
			defer func() { u.cancel(); <-done }()
			for range tc.active {
				select {
				case size := <-s.started:
					if size != tc.size {
						t.Fatal("uploaded incorrect buffer")
					}
				case <-time.After(time.Second):
					t.Fatal("independent uploads serialized despite available request/byte capacity")
				}
			}
			select {
			case <-s.started:
				t.Fatal("upload exceeded request/byte budget")
			case <-time.After(25 * time.Millisecond):
			}
		})
	}
}

type delayedPublicationStore struct{ store.Store }

func (delayedPublicationStore) Put(ctx context.Context, _ string, _ []byte, _ string) error {
	select {
	case <-time.After(3 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func BenchmarkPublicationSmallObjects(b *testing.B) {
	data := make([]byte, 256<<10)
	b.ReportAllocs()
	for b.Loop() {
		u := newPublicationUploads(b.Context(), delayedPublicationStore{})
		for i := range 48 {
			if err := u.Put(b.Context(), fmt.Sprintf("index/%d", i), data, ""); err != nil {
				u.close()
				b.Fatal(err)
			}
		}
		err := u.wait()
		u.close()
		if err != nil {
			b.Fatal(err)
		}
	}
}
