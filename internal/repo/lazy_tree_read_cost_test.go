package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gyit/internal/store"
)

type lazyIO struct {
	Gets, PayloadGets, Bytes int64
}

type lazyMeasuredStore struct {
	store.Store
	delay time.Duration
	mu    sync.Mutex
	io    lazyIO
}

func (m *lazyMeasuredStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if m.delay > 0 {
		timer := time.NewTimer(m.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-timer.C:
		}
	}
	b, token, err := m.Store.Get(ctx, key, off, n)
	m.mu.Lock()
	m.io.Gets++
	m.io.Bytes += int64(len(b))
	if strings.HasPrefix(key, "packs/") {
		m.io.PayloadGets++
	}
	m.mu.Unlock()
	return b, token, err
}

func (m *lazyMeasuredStore) reset()         { m.mu.Lock(); m.io = lazyIO{}; m.mu.Unlock() }
func (m *lazyMeasuredStore) counts() lazyIO { m.mu.Lock(); defer m.mu.Unlock(); return m.io }

type lazyReadRow struct {
	Path, Arm, Operation, Cache string
	DelayMS                     int64
	Native                      bool
	Admission                   string
	Entries, StatSubset         int
	Seconds                     float64
	Over1s                      bool
	IO                          lazyIO
	CacheUsed                   int
}

func TestLazyTreeReadCosts(t *testing.T) {
	if os.Getenv("GYIT_LAZY_TREE_READ_PROBE") != "1" {
		t.Skip("opt-in source read diagnostic")
	}
	ctx := context.Background()
	f := lazyBuildReadFixture(t, ctx)
	var rows []lazyReadRow
	report := os.Getenv("GYIT_LAZY_TREE_READ_REPORT")
	if report == "" {
		t.Fatal("report path required")
	}
	defer func() {
		b, err := json.MarshalIndent(rows, "", "  ")
		if err == nil {
			err = os.WriteFile(report, append(b, '\n'), 0600)
		}
		if err != nil {
			t.Error(err)
		}
	}()
	for _, delay := range []time.Duration{0, 5 * time.Millisecond, 20 * time.Millisecond} {
		for _, c := range f.Cases {
			for _, arm := range []string{"compiled", "native"} {
				for _, operation := range []string{"names-page", "names-all", "lookup-one", "serial-stat-subset"} {
					root := c.CompiledRoot
					if arm == "native" {
						root = c.NativeRoot
					}
					m := &lazyMeasuredStore{Store: f.Backend}
					s := &Snapshot{idx: &index{store: m, cache: newCache(DefaultCacheBytes), root: root, blobRoot: f.BlobRoot}, Tree: c.OID}
					// Deliberately optimistic: routing is hot, leaves and directory payloads are cold.
					for _, ref := range append([]pageRef{root}, f.BlobRoutes...) {
						if _, err := s.idx.pageBytes(ctx, ref); err != nil {
							t.Fatal(err)
						}
					}
					m.delay = delay
					for _, state := range []string{"cold", "warm"} {
						m.reset()
						started := time.Now()
						var err error
						switch operation {
						case "names-page", "names-all":
							err = lazyCheckNames(ctx, s, c.Entries, operation == "names-all")
						case "lookup-one":
							if len(c.StatNames) == 0 {
								t.Fatal("fixture needs selected file")
							}
							err = lazyCheckStats(ctx, s, c.Entries, c.StatNames[:1])
						case "serial-stat-subset":
							err = lazyCheckNames(ctx, s, c.Entries, false)
							if err == nil {
								err = lazyCheckStats(ctx, s, c.Entries, c.StatNames)
							}
						}
						elapsed := time.Since(started).Seconds()
						if err != nil {
							t.Fatalf("%s %s %s %s: %v", c.Path, arm, operation, state, err)
						}
						s.idx.cache.mu.Lock()
						used := s.idx.cache.used
						s.idx.cache.mu.Unlock()
						if used > DefaultCacheBytes {
							t.Fatal("cache exceeded shared budget")
						}
						row := lazyReadRow{Path: c.Path, Arm: arm, Operation: operation, Cache: state, DelayMS: delay.Milliseconds(), Native: c.Native, Admission: c.Admission, Entries: len(c.Entries), StatSubset: len(c.StatNames), Seconds: elapsed, Over1s: elapsed > 1, IO: m.counts(), CacheUsed: used}
						rows = append(rows, row)
						t.Logf("path=%q arm=%s op=%s cache=%s RTT=%s seconds=%.6f over_1s=%t gets=%d bytes=%d", c.Path, arm, operation, state, delay, elapsed, row.Over1s, row.IO.Gets, row.IO.Bytes)
						if state == "warm" && row.IO.Gets != 0 {
							t.Fatal("warm small fixture operation performed I/O")
						}
					}
				}
			}
		}
	}
}

func lazyCheckNames(ctx context.Context, s *Snapshot, want []Entry, all bool) error {
	var got []Dirent
	after := ""
	for {
		page, err := s.ReadDirNames(ctx, s.Tree, after, fanout)
		if err != nil {
			return err
		}
		got = append(got, page...)
		if !all || len(page) == 0 {
			break
		}
		after = page[len(page)-1].Name
		if len(got) > len(want) {
			return fmt.Errorf("pagination repeated entries")
		}
	}
	if !all {
		want = want[:min(len(want), fanout)]
	}
	expected := make([]Dirent, len(want))
	for i, e := range want {
		expected[i] = Dirent{Name: e.Name, OID: e.OID, Mode: e.Mode, RawMode: e.RawMode}
	}
	if !reflect.DeepEqual(got, expected) {
		return fmt.Errorf("directory names differ from Git: got=%d want=%d", len(got), len(expected))
	}
	return nil
}

func lazyCheckStats(ctx context.Context, s *Snapshot, entries []Entry, names []string) error {
	for _, name := range names {
		var want Entry
		found := false
		for _, e := range entries {
			if e.Name == name {
				want = e
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("unknown stat oracle %q", name)
		}
		got, err := s.Lookup(ctx, s.Tree, name)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(got, want) {
			return fmt.Errorf("stat %q differs: got=%+v want=%+v", name, got, want)
		}
	}
	return nil
}
