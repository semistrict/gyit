package repo

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	sizewire "gyit/internal/globalsizes/wire"
	"gyit/internal/store"
)

type globalTransferStore struct {
	store.Store
	bandwidth   int64
	gets, bytes int64
}

func (m *globalTransferStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	delay := 20*time.Millisecond + time.Duration(float64(n)/float64(m.bandwidth)*float64(time.Second))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, "", ctx.Err()
	case <-timer.C:
	}
	b, token, err := m.Store.Get(ctx, key, off, n)
	m.gets++
	m.bytes += int64(len(b))
	return b, token, err
}

type globalPreloadRow struct {
	MBPerSecond                               int64
	Seconds, TransferModelSeconds             float64
	Gets, Bytes                               int64
	PayloadCapacity, ReaderMetadata, Retained uint64
	Over1s                                    bool
}
type globalReadRow struct {
	Path, Arm, Operation, Cache            string
	DelayMS                                int64
	Native                                 bool
	Admission                              string
	Seconds, ReadDirSeconds, LookupSeconds float64
	IO, LookupIO                           lazyIO
	CacheUsed                              int
	TableBytes, TotalRetained              uint64
	Entries                                int
	Over1s                                 bool
}
type globalPressureRow struct {
	Path, Arm                 string
	Round                     int
	Seconds                   float64
	IO                        lazyIO
	CacheUsed                 int
	TableBytes, TotalRetained uint64
	Over1s                    bool
}

// Stage the frozen artifact as an immutable object during fixture preparation.
// Return no borrowed bytes: only the charged slot may retain a read copy later.
func globalReadFixtureTable(t *testing.T, ctx context.Context, backend store.Store) globalSizeRef {
	t.Helper()
	f, err := os.Open(os.Getenv("GYIT_GLOBAL_SIZE_TABLE"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil || stat.Size() < 0 || stat.Size() > int64(globalTableBudget-sizewire.ReaderMetadataBytes) {
		t.Fatal("table artifact exceeds slot", err)
	}
	b := make([]byte, int(stat.Size()))
	if _, err := io.ReadFull(f, b); err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(b))
	if hash != os.Getenv("GYIT_GLOBAL_SIZE_TABLE_SHA256") {
		t.Fatal("table artifact differs from frozen manifest")
	}
	ref := globalSizeRef{Key: "index/global-sizes-" + hash, Hash: hash, Length: int64(len(b))}
	if err := backend.Put(ctx, ref.Key, b, "*"); err != nil {
		t.Fatal(err)
	}
	return ref
}
func globalRetained(t *testing.T, s *Snapshot) (int, uint64, uint64) {
	t.Helper()
	s.idx.cache.mu.Lock()
	used := s.idx.cache.used
	maximum := s.idx.cache.max
	s.idx.cache.mu.Unlock()
	table := uint64(0)
	if s.globalSizes != nil {
		table = s.globalSizes.charged.Load()
		if maximum != globalOrdinaryBudget || table > globalTableBudget {
			t.Fatal("candidate budget partition violated")
		}
	} else if maximum != DefaultCacheBytes {
		t.Fatal("control cache partition")
	}
	total := uint64(used) + table
	if total > DefaultCacheBytes {
		t.Fatal("combined32MiB budget exceeded")
	}
	return used, table, total
}
func globalCheckPageAndStat(t *testing.T, ctx context.Context, s *Snapshot, c lazyReadCase) {
	t.Helper()
	want := c.Entries[:min(len(c.Entries), fanout)]
	got, err := s.ReadDir(ctx, c.OID, "", fanout)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("pressure listing differs from Git", err)
	}
	for _, e := range want {
		got, err := s.Lookup(ctx, c.OID, e.Name)
		if err != nil || got != e {
			t.Fatal("pressure stat differs from Git", err)
		}
	}
}

func TestGlobalSizeReadCosts(t *testing.T) {
	if os.Getenv("GYIT_GLOBAL_SIZE_READ_PROBE") != "1" {
		t.Skip("opt-in fixed-source API read probe")
	}
	report := os.Getenv("GYIT_GLOBAL_SIZE_READ_REPORT")
	if report == "" {
		t.Fatal("report required")
	}
	ctx := t.Context()
	prepared := time.Now()
	fixture := lazyBuildReadFixture(t, ctx)
	ref := globalReadFixtureTable(t, ctx, fixture.Backend)
	preparationSeconds := time.Since(prepared).Seconds()
	var preloads []globalPreloadRow
	var rows []globalReadRow
	var pressure []globalPressureRow
	defer func() {
		b, err := json.MarshalIndent(map[string]any{"preparation_seconds": preparationSeconds, "preparation_over_1s": preparationSeconds > 1, "preloads": preloads, "rows": rows, "pressure": pressure, "scope": "MODELED_SNAPSHOT_API_ONLY", "membership_checked": false, "full_import_qualified": false}, "", "  ")
		if err == nil {
			err = os.WriteFile(report, append(b, '\n'), 0600)
		}
		if err != nil {
			t.Error(err)
		}
	}()
	var slot *globalSizeSlot
	var transfer *globalTransferStore
	for _, mbps := range []int64{10, 100} {
		if slot != nil {
			if err := slot.clear(ctx); err != nil {
				t.Fatal(err)
			}
		}
		transfer = &globalTransferStore{Store: fixture.Backend, bandwidth: mbps * 1000000}
		slot = newGlobalSizeSlot(transfer)
		started := time.Now()
		var capacity, metadata uint64
		if err := slot.with(ctx, ref, func(table *sizewire.Table) error {
			metadata = table.OwnedBytes()
			capacity = slot.charged.Load() - metadata
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		seconds := time.Since(started).Seconds()
		preloads = append(preloads, globalPreloadRow{MBPerSecond: mbps, Seconds: seconds, TransferModelSeconds: .02 + float64(ref.Length)/float64(mbps*1000000), Gets: transfer.gets, Bytes: transfer.bytes, PayloadCapacity: capacity, ReaderMetadata: metadata, Retained: slot.charged.Load(), Over1s: seconds > 1})
		if transfer.gets != 1 || transfer.bytes != ref.Length || slot.loads.Load() != 1 || slot.verified.Load() != 1 {
			t.Fatal("preload did not read and authenticate once")
		}
	}
	defer slot.clear(context.Background())
	for _, c := range fixture.Cases {
		for _, delay := range []time.Duration{0, 5 * time.Millisecond, 20 * time.Millisecond} {
			for _, arm := range []string{"compiled", "global-size"} {
				for _, operation := range []string{"page-stat", "all-pages"} {
					root, budget := c.CompiledRoot, DefaultCacheBytes
					if arm == "global-size" {
						root, budget = c.NativeRoot, globalOrdinaryBudget
					}
					meter := &lazyMeasuredStore{Store: fixture.Backend}
					s := &Snapshot{idx: &index{store: meter, cache: newCache(budget), root: root, blobRoot: fixture.BlobRoot}, Tree: c.OID}
					if arm == "global-size" {
						s.globalSizes = slot
						s.globalSizeRef = ref
					}
					if _, err := s.idx.pageBytes(ctx, root); err != nil {
						t.Fatal(err)
					}
					meter.delay = delay
					for _, state := range []string{"cold", "warm"} {
						meter.reset()
						started := time.Now()
						row := globalReadRow{Path: c.Path, Arm: arm, Operation: operation, Cache: state, DelayMS: delay.Milliseconds(), Native: c.Native, Admission: c.Admission}
						var got []Entry
						after := ""
						for {
							page, err := s.ReadDir(ctx, c.OID, after, fanout)
							if err != nil {
								t.Fatal(err)
							}
							got = append(got, page...)
							if operation == "page-stat" || len(page) == 0 {
								break
							}
							after = page[len(page)-1].Name
							if len(got) > len(c.Entries) {
								t.Fatal("repeated pagination")
							}
						}
						row.ReadDirSeconds = time.Since(started).Seconds()
						row.IO = meter.counts()
						want := c.Entries
						if operation == "page-stat" {
							want = want[:min(len(want), fanout)]
							meter.reset()
							lookups := time.Now()
							for _, e := range want {
								got, err := s.Lookup(ctx, c.OID, e.Name)
								if err != nil || got != e {
									t.Fatal("stat differs from Git", got, e, err)
								}
							}
							row.LookupSeconds = time.Since(lookups).Seconds()
							row.LookupIO = meter.counts()
							if row.LookupIO.Gets != 0 {
								t.Fatal("post-page stat fetched metadata")
							}
						}
						row.Seconds = time.Since(started).Seconds()
						row.Over1s = row.Seconds > 1
						if !reflect.DeepEqual(got, want) {
							t.Fatal("listing differs from Git", c.Path, arm)
						}
						row.Entries = len(got)
						row.IO.Gets += row.LookupIO.Gets
						row.IO.PayloadGets += row.LookupIO.PayloadGets
						row.IO.Bytes += row.LookupIO.Bytes
						row.CacheUsed, row.TableBytes, row.TotalRetained = globalRetained(t, s)
						if state == "warm" && row.IO.Gets != 0 {
							t.Fatal("warm directory data fetched")
						}
						if slot.loads.Load() != 1 || transfer.gets != 1 || slot.verified.Load() != 1 {
							t.Fatal("operation reloaded table")
						}
						rows = append(rows, row)
						t.Logf("path=%s arm=%s op=%s state=%s delay=%s seconds=%.6f gets=%d over1s=%t", c.Path, arm, operation, state, delay, row.Seconds, row.IO.Gets, row.Over1s)
					}
					if delay == 0 && operation == "page-stat" {
						for round := 0; round < 2; round++ {
							for i := 0; i < 40; i++ {
								s.idx.cache.put(fmt.Sprintf("probe-pressure-%d-%d", round, i), make([]byte, 1<<20))
							}
							meter.reset()
							started := time.Now()
							globalCheckPageAndStat(t, ctx, s, c)
							seconds := time.Since(started).Seconds()
							used, table, total := globalRetained(t, s)
							pressure = append(pressure, globalPressureRow{Path: c.Path, Arm: arm, Round: round, Seconds: seconds, IO: meter.counts(), CacheUsed: used, TableBytes: table, TotalRetained: total, Over1s: seconds > 1})
							if slot.loads.Load() != 1 || transfer.gets != 1 {
								t.Fatal("ordinary cache pressure evicted table")
							}
						}
					}
				}
			}
		}
	}
	if len(rows) != 72 || len(pressure) != 12 {
		t.Fatal("incomplete read matrix")
	}
}
