package store

import (
	"sync/atomic"
	"testing"
)

func TestDiskCacheKeepsTwentyGiBFree(t *testing.T) {
	const gib = int64(1 << 30)
	for _, tt := range []struct{ total, free, used, want int64 }{
		{100 * gib, 30 * gib, 0, 4 * gib},
		{100 * gib, 21 * gib, 0, gib},
		{100 * gib, 18 * gib, 4 * gib, 2 * gib},
		{100 * gib, 15 * gib, 4 * gib, 0},
		{100 * gib, 20 * gib, 4 * gib, 4 * gib},
	} {
		if got := effectiveDiskLimit(4*gib, tt.used, tt.total, tt.free); got != tt.want {
			t.Errorf("%+v: got %d", tt, got)
		}
	}
}

func TestDiskPressureEvictsAfterLeaseRelease(t *testing.T) {
	var free atomic.Int64
	free.Store(30 << 30)
	c, err := newDiskCache(nil, t.TempDir(), "pressure", 4096, func() (int64, int64, error) { return 100 << 30, free.Load(), nil })
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	b, done, err := c.Load(t.Context(), "item", func() ([]byte, error) { return []byte("active"), nil })
	if err != nil {
		t.Fatal(err)
	}
	free.Store(19 << 30)
	c.mu.Lock()
	c.refreshLimit()
	c.makeRoom(0, 0)
	c.mu.Unlock()
	if string(b) != "active" {
		t.Fatal("pressure invalidated active mapping")
	}
	done()
	c.mu.Lock()
	used := c.used
	c.mu.Unlock()
	if used != 0 {
		t.Fatalf("cache did not shrink: %d", used)
	}
	calls := 0
	for range 2 {
		_, release, err := c.Load(t.Context(), "item", func() ([]byte, error) { calls++; return []byte("active"), nil })
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	if calls != 2 {
		t.Fatal("cache populated below free-space reserve")
	}
	free.Store(30 << 30)
	_, release, err := c.Load(t.Context(), "item", func() ([]byte, error) { return []byte("active"), nil })
	if err != nil {
		t.Fatal(err)
	}
	release()
	_, release, err = c.Load(t.Context(), "item", func() ([]byte, error) { t.Fatal("cache failed to regrow"); return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	release()
}
