package spill_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"

	"gyit/internal/spill"
)

func TestTakePreservesOrderUpdatesAndOwnership(t *testing.T) {
	for _, count := range []int{0, 3, 2048} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			parent := t.TempDir()
			dst, err := spill.New(parent, 4096)
			if err != nil {
				t.Fatal(err)
			}
			defer dst.Close()
			for generation := range 3 {
				src, err := spill.New(parent, 4096)
				if err != nil {
					t.Fatal(err)
				}
				defer src.Close()
				for i := count - 1; i >= 0; i-- {
					if err := src.Add([]byte(fmt.Sprintf("%04d", i)), []byte(fmt.Sprint(generation))); err != nil {
						t.Fatal(err)
					}
				}
				// Walking an input may already have compacted its temporary runs.
				if generation == 1 {
					if err := src.Walk(t.Context(), func(_, _ []byte) error { return nil }); err != nil {
						t.Fatal(err)
					}
				}
				if count > 0 {
					if err := dst.Add([]byte("0000"), []byte("older destination value")); err != nil {
						t.Fatal(err)
					}
				}
				if err := dst.Take(src); err != nil {
					t.Fatal(err)
				}
				if err := src.Close(); err != nil {
					t.Fatal(err)
				}
				if err := src.Add(nil, nil); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("consumed source accepted writes: %v", err)
				}
				if err := src.Walk(t.Context(), func(_, _ []byte) error { return nil }); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("consumed source accepted reads: %v", err)
				}
			}
			if err := dst.Add([]byte("last"), []byte("newer")); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				n := 0
				err := dst.Walk(t.Context(), func(k, v []byte) error {
					wantKey, wantValue := fmt.Sprintf("%04d", n), "2"
					if n == count {
						wantKey, wantValue = "last", "newer"
					}
					if string(k) != wantKey || string(v) != wantValue {
						t.Fatalf("entry %d: %q %q", n, k, v)
					}
					n++
					return nil
				})
				if err != nil || n != count+1 {
					t.Fatalf("walk count=%d error=%v", n, err)
				}
			}
			if err := dst.Close(); err != nil {
				t.Fatal(err)
			}
			files, err := os.ReadDir(parent)
			if err != nil || len(files) != 0 {
				t.Fatalf("leaked transferred staging: %v %v", files, err)
			}
		})
	}
}

func TestTakeNestedAndRejectedTransfers(t *testing.T) {
	parent := t.TempDir()
	a, err := spill.New(parent, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := spill.New(parent, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	c, err := spill.New(parent, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	large, err := spill.New(parent, 8192)
	if err != nil {
		t.Fatal(err)
	}
	defer large.Close()
	if err := a.Take(large); err == nil {
		t.Fatal("accepted a larger run budget")
	}
	key := []byte{0, 255}
	if err := a.Add(key, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := a.Take(a); err == nil {
		t.Fatal("accepted self transfer")
	}
	if err := b.Take(a); err != nil {
		t.Fatal(err)
	}
	if err := c.Take(b); err != nil {
		t.Fatal(err)
	}
	if err := c.Take(a); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("accepted consumed source: %v", err)
	}
	if err := b.Take(c); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("accepted closed destination: %v", err)
	}
	n := 0
	if err := c.Walk(t.Context(), func(k, v []byte) error {
		if !bytes.Equal(k, key) || string(v) != "first" {
			t.Fatalf("lost nested transfer: %x %q", k, v)
		}
		n++
		return nil
	}); err != nil || n != 1 {
		t.Fatalf("count=%d error=%v", n, err)
	}
}
