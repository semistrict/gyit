package repo

import (
	"context"
	"testing"
	"time"

	"gyit/internal/store"
)

type delayedHistoryData struct {
	store.Store
	keys map[string]bool
}

func (s *delayedHistoryData) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if s.keys[key] {
		timer := time.NewTimer(3 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-timer.C:
		}
	}
	return s.Store.Get(ctx, key, off, n)
}

// Five independent containers fit in the bounded request window. Fixed request
// latency makes an unnecessary second wave measurable.
func BenchmarkHistoryIndependentReads(b *testing.B) {
	benchmarkHistoryIndependentReads(b, false)
}

func BenchmarkHistorySparseIndependentReads(b *testing.B) {
	benchmarkHistoryIndependentReads(b, true)
}

func benchmarkHistoryIndependentReads(b *testing.B, sparse bool) {
	count := 5
	if sparse {
		count++
	}
	backend, ids, keys, _ := historyPrefetchFixture(b, count, false, sparse)
	want := ids
	if sparse {
		want = ids[1:]
	}
	delayed := &delayedHistoryData{Store: backend, keys: map[string]bool{}}
	for _, key := range keys {
		delayed.keys[key] = true
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		p := directoryReader(b, delayed, b.TempDir(), 32<<20)
		s := &Snapshot{SHA: ids[0], progressive: p, idx: p.index()}
		b.StartTimer()
		count := 0
		err := s.indexedFileLog(b.Context(), "file", LogOptions{Count: len(want), FullCommitIDs: true}, func(entry LogEntry) error {
			if count >= len(want) || entry.SHA != want[count] {
				b.Fatal("history order differs")
			}
			count++
			return nil
		})
		b.StopTimer()
		if err != nil || count != len(want) {
			b.Fatalf("count=%d err=%v", count, err)
		}
		if err := p.cache.disk.Close(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}
