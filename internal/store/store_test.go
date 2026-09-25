package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestLocalRangeAndCAS(t *testing.T) {
	ctx := context.Background()
	s, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "HEAD", []byte("abcdef"), "*"); err != nil {
		t.Fatal(err)
	}
	_, token, err := s.Get(ctx, "HEAD", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := s.Get(ctx, "HEAD", 2, 3)
	if err != nil || string(b) != "cde" {
		t.Fatal(string(b), err)
	}
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Go(func() {
			err := s.Put(ctx, "HEAD", []byte("new"), token)
			if err == nil {
				successes.Add(1)
			} else if !errors.Is(err, ErrConflict) {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("CAS allowed %d publishers", successes.Load())
	}
	if err := s.Put(ctx, "../escape", nil, ""); err == nil {
		t.Fatal("unsafe key accepted")
	}
}
