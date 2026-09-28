package main

import (
	"context"
	"errors"
	"gyit/internal/store"
	"testing"
)

func TestBenchmarkOverlayPreservesBase(t *testing.T) {
	ctx := context.Background()
	base, _ := store.NewLocal(t.TempDir())
	scratch, _ := store.NewLocal(t.TempDir())
	if err := base.Put(ctx, "HEAD", []byte("base"), "*"); err != nil {
		t.Fatal(err)
	}
	s := &benchmarkOverlay{Store: scratch, base: base, keys: map[string]bool{}}
	if b, _, err := s.Get(ctx, "HEAD", 0, -1); err != nil || string(b) != "base" {
		t.Fatalf("base read: %q %v", b, err)
	}
	if err := s.Put(ctx, "HEAD", []byte("scratch"), "*"); err != nil {
		t.Fatal(err)
	}
	b, version, err := s.Get(ctx, "HEAD", 0, -1)
	if err != nil || string(b) != "scratch" {
		t.Fatalf("overlay read: %q %v", b, err)
	}
	if err = s.Put(ctx, "HEAD", []byte("updated"), version); err != nil {
		t.Fatal(err)
	}
	if err = s.Put(ctx, "HEAD", []byte("stale"), version); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale CAS: %v", err)
	}
	if b, _, err := base.Get(ctx, "HEAD", 0, -1); err != nil || string(b) != "base" {
		t.Fatalf("base changed: %q %v", b, err)
	}
}
