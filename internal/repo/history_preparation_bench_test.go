//go:build !js

package repo

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"gyit/internal/store"
)

// Run with GYIT_HISTORY_SOURCE pointing at the existing packed fixture and
// -cpu=1,2,4,8. Each iteration starts with an empty 32 MiB decode cache. This
// isolates the production preparation scheduler and tree comparison from
// acquisition, staging and remote publication. The in-memory frontier belongs
// only to the benchmark; production keeps its frontier on disk.
func BenchmarkRepositoryHistoryPreparation(b *testing.B) {
	dir := os.Getenv("GYIT_HISTORY_SOURCE")
	if dir == "" {
		b.Skip("set GYIT_HISTORY_SOURCE to a packed repository")
	}
	revision := os.Getenv("GYIT_HISTORY_SHA")
	if revision == "" {
		revision = "HEAD"
	}
	tip := strings.TrimSpace(benchGit(b, dir, "", "rev-parse", "--verify", revision+"^{commit}"))
	firstParent := os.Getenv("GYIT_HISTORY_PREPARE_FIRST_PARENT") == "1"
	countArgs := []string{"rev-list", "--count", tip}
	if firstParent {
		countArgs = append(countArgs, "--first-parent")
	}
	want, err := strconv.Atoi(strings.TrimSpace(benchGit(b, dir, "", countArgs...)))
	if err != nil {
		b.Fatal(err)
	}
	if value := os.Getenv("GYIT_HISTORY_PREPARE_LIMIT"); value != "" {
		limit, err := strconv.Atoi(value)
		if err != nil || limit < 1 {
			b.Fatal("invalid GYIT_HISTORY_PREPARE_LIMIT")
		}
		want = min(want, limit)
	}
	local, err := store.NewLocal(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	p, err := NewProgressive(b.Context(), local, nil, b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	prepare := func(ctx context.Context) (int, int, error) {
		h := newHistoryPreparation(ctx, p)
		defer h.close()
		queue := []string{tip}
		seen := make(map[string]bool)
		changes := 0
		for head := 0; head < len(queue); head++ {
			sha := queue[head]
			queue[head] = ""
			if seen[sha] {
				h.forget(sha)
				continue
			}
			seen[sha] = true
			r := h.get(sha)
			if r.err != nil {
				r.close()
				return 0, 0, r.err
			}
			parents := r.commit.Parents
			if firstParent && len(parents) > 1 {
				parents = parents[:1]
			}
			for _, parent := range parents {
				queue = append(queue, hex.EncodeToString(parent))
			}
			err := r.paths.walk(ctx, func(_ string, _ []byte) error { changes++; return nil })
			r.close()
			if err != nil {
				return 0, 0, err
			}
			if len(seen) == want {
				break
			}
		}
		return len(seen), changes, nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		source, err := openHistorySource([]string{dir})
		if err != nil {
			b.Fatal(err)
		}
		ctx := context.WithValue(b.Context(), historySourceKey{}, source)
		b.StartTimer()
		count, changes, err := prepare(ctx)
		b.StopTimer()
		source.close()
		if err != nil || count != want {
			b.Fatalf("prepared %d of %d commits: %v", count, want, err)
		}
		b.ReportMetric(float64(count), "commits/op")
		b.ReportMetric(float64(changes), "changed-paths/op")
		b.StartTimer()
	}
}

// Isolate preparation and parent metadata reuse from pack decompression and
// network latency. The source owns immutable decoded objects, as in ingestion.
func BenchmarkHistoryPreparation(b *testing.B) {
	local, err := store.NewLocal(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	p, err := NewProgressive(b.Context(), local, nil, b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	source := &historySource{cache: newCache(32 << 20)}
	add := func(kind byte, name string, raw []byte) string {
		h := sha1.New()
		fmt.Fprintf(h, "%s %d%c", name, len(raw), 0)
		h.Write(raw)
		oid := hex.EncodeToString(h.Sum(nil))
		source.cache.put("progressive-object/"+oid, append([]byte{kind}, raw...))
		return oid
	}
	tree := add(2, "tree", nil)
	var tip string
	for i := range 1000 {
		parent := ""
		if tip != "" {
			parent = "parent " + tip + "\n"
		}
		raw := fmt.Sprintf("tree %s\n%sauthor A <a@example.test> %d +0000\ncommitter A <a@example.test> %d +0000\n\n%s\n", tree, parent, i, i, strings.Repeat("message ", 128))
		tip = add(1, "commit", []byte(raw))
	}
	ctx := context.WithValue(b.Context(), historySourceKey{}, source)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		h := newHistoryPreparation(ctx, p)
		sha := tip
		count := 0
		for sha != "" {
			r := h.get(sha)
			if r.err != nil {
				h.close()
				b.Fatal(r.err)
			}
			sha = ""
			if len(r.commit.Parents) != 0 {
				sha = hex.EncodeToString(r.commit.Parents[0])
			}
			count++
			r.close()
		}
		h.close()
		if count != 1000 {
			b.Fatalf("prepared %d commits", count)
		}
	}
}
