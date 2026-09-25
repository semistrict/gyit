package repo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"

	"gat/internal/orderedrows"
	"gat/internal/spill"
	"gat/internal/store"
)

func TestOrderedMainIndexMatchesExistingPages(t *testing.T) {
	for _, mode := range []string{"empty", "archive", "fallback", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			local, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			archive, err := orderedrows.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer archive.Close()
			fallback, err := spill.New(t.TempDir(), 1024)
			if err != nil {
				t.Fatal(err)
			}
			defer fallback.Close()
			control, err := spill.New(t.TempDir(), 1024)
			if err != nil {
				t.Fatal(err)
			}
			defer control.Close()
			count := 300
			if mode == "empty" {
				count = 0
			}
			for i := 0; i < count; i++ {
				key := []byte(fmt.Sprintf("o/%040x", i+1))
				value, err := marshal(object{Kind: "blob", Size: int64(i * 1000)})
				if err != nil {
					t.Fatal(err)
				}
				if err = control.Add(key, value); err != nil {
					t.Fatal(err)
				}
				if mode == "archive" || (mode == "mixed" && i%3 != 0) {
					err = archive.Add(t.Context(), key, value)
				} else {
					err = fallback.Add(key, value)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if err = archive.Seal(t.Context()); err != nil {
				t.Fatal(err)
			}
			idx := &index{store: local, cache: newCache(32 << 20)}
			idx.root, err = idx.updateSortedOrdered(t.Context(), fallback, archive.Next)
			if err != nil {
				t.Fatal(err)
			}
			baseline := &index{store: local, cache: newCache(32 << 20)}
			baseline.root, err = baseline.updateSorted(t.Context(), control)
			if err != nil {
				t.Fatal(err)
			}
			got, err := idx.scan(t.Context(), "", "", 301)
			if err != nil {
				t.Fatal(err)
			}
			want, err := baseline.scan(t.Context(), "", "", 301)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("index rows differ: %d != %d", len(got), len(want))
			}
			if len(got) != count {
				t.Fatalf("missing rows: %d != %d", len(got), count)
			}
			for i := 0; i < count; i++ {
				var o object
				if err = idx.get(t.Context(), fmt.Sprintf("o/%040x", i+1), &o); err != nil || o.Kind != "blob" || o.Size != int64(i*1000) {
					t.Fatalf("read row %d: %+v %v", i, o, err)
				}
			}
		})
	}
}

func TestOrderedMainIndexFailureReturnsNoRoot(t *testing.T) {
	for _, mode := range []string{"duplicate", "cursor_error", "write_error", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			local, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			backend := &failIndexStore{Store: local, fail: mode == "write_error"}
			rows, err := spill.New(t.TempDir(), 1024)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			key := []byte("o/0000000000000000000000000000000000000001")
			value, err := marshal(object{Kind: "blob", Size: 123})
			if err != nil {
				t.Fatal(err)
			}
			if err = rows.Add(key, value); err != nil {
				t.Fatal(err)
			}
			called := false
			injected := errors.New("archive stream failed")
			archive := func(context.Context) ([]byte, []byte, error) {
				if mode == "cursor_error" {
					return nil, nil, injected
				}
				if mode == "duplicate" && !called {
					called = true
					return key, value, nil
				}
				return nil, nil, io.EOF
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "canceled" {
				cancel()
			}
			idx := &index{store: backend, cache: newCache(32 << 20)}
			root, err := idx.updateSortedOrdered(ctx, rows, archive)
			if err == nil || root != (pageRef{}) {
				t.Fatalf("failure returned usable root: %+v %v", root, err)
			}
			if mode == "cursor_error" && !errors.Is(err, injected) {
				t.Fatal(err)
			}
			if mode == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			// The pull adapter must release a paused sort walk on every failure.
			if err = rows.Walk(t.Context(), func(k, v []byte) error { return nil }); err != nil {
				t.Fatal(err)
			}
		})
	}
}
