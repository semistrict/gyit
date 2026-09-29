//go:build !js

package repo

// This experiment is deliberately test-only. It measures a physical layout,
// not the production reader, incremental publication, or end-to-end latency.
// Native Git supplies expected output only. Preparation and queries use Go.

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/spill"
	"gyit/internal/store"
)

const probeGraphBlock = 4096
const probeFrontier = -2147483648

var errProbeFrontier = errors.New("layout probe reached unprepared ancestry")

type historyLayoutProbe struct {
	dir                                     string
	backend                                 store.Store
	count                                   int
	graph, ids                              []*pb.PageReference
	paths                                   *pb.PageReference
	sizes                                   map[string]int
	graphBytes, idBytes, pathBytes, changes int64
}

func buildHistoryLayoutProbe(t *testing.T, sourceDir, tip string, limit int) *historyLayoutProbe {
	t.Helper()
	ctx := t.Context()
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProgressive(ctx, local, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source, err := openHistorySource([]string{sourceDir})
	if err != nil {
		t.Fatal(err)
	}
	defer source.close()
	ctx = context.WithValue(ctx, historySourceKey{}, source)
	out := &historyLayoutProbe{dir: t.TempDir(), sizes: map[string]int{}}
	out.backend, err = store.NewLocal(out.dir)
	if err != nil {
		t.Fatal(err)
	}
	var leaves []*pb.DirectoryChild
	encoder, err := newCompressor()
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	save := func(m proto.Message) (*pb.PageReference, error) {
		raw, err := proto.Marshal(m)
		if err != nil {
			return nil, err
		}
		packed := encoder.EncodeAll(raw, nil)
		name := "paths.pack"
		switch m.(type) {
		case *pb.HistoryLayoutProbeGraph:
			name = "graph.pack"
		case *pb.HistoryLayoutProbeIDs:
			name = "ids.pack"
		}
		offset := out.sizes[name]
		f, err := os.OpenFile(filepath.Join(out.dir, name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return nil, err
		}
		_, writeErr := f.Write(packed)
		if err = errors.Join(writeErr, f.Close()); err != nil {
			return nil, err
		}
		out.sizes[name] += len(packed)
		sum := sha256.Sum256(packed)
		ref := &pb.PageReference{Pack: name, Offset: int64(offset), Length: int64(len(packed)), Hash: hex.EncodeToString(sum[:])}
		if page, ok := m.(*pb.HistoryPathPage); ok && len(page.Entries) > 0 {
			leaves = append(leaves, &pb.DirectoryChild{MaxName: page.Entries[len(page.Entries)-1].Path, Page: ref})
		}
		switch m.(type) {
		case *pb.HistoryLayoutProbeGraph:
			out.graphBytes += int64(len(packed))
		case *pb.HistoryLayoutProbeIDs:
			out.idBytes += int64(len(packed))
		default:
			out.pathBytes += int64(len(packed))
		}
		return ref, nil
	}
	start := time.Now()
	err = p.stageHistory(func(stage *historyStage) error {
		for _, name := range []string{"ordinals", "records"} {
			b, e := stage.tx.CreateBucket([]byte(name))
			if e != nil {
				return e
			}
			stage.buckets[name] = b
		}
		changes, err := spill.New(p.temp, historyChangeMemory)
		if err != nil {
			return err
		}
		defer changes.Close()
		preparation := newHistoryPreparation(ctx, p)
		defer preparation.close()
		walk := newLogTraversal(ctx, p.temp)
		defer walk.close()
		if err := walk.push(logCandidate{sha: tip}); err != nil {
			return err
		}
		for out.count < limit {
			candidate, ok, err := walk.pop()
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			r := preparation.get(candidate.sha)
			if r.err != nil {
				r.close()
				return r.err
			}
			ordinal := uint32(out.count)
			var key [4]byte
			binary.BigEndian.PutUint32(key[:], ordinal)
			if err := stage.buckets["ordinals"].Put(r.commit.Oid, key[:]); err != nil {
				r.close()
				return err
			}
			raw, err := proto.Marshal(r.commit.HistoryBatchCommit)
			if err == nil {
				err = stage.buckets["records"].Put(key[:], raw)
			}
			if err == nil {
				err = r.paths.walk(ctx, func(path string, mask []byte) error {
					k := make([]byte, len(path)+5)
					copy(k, path)
					binary.BigEndian.PutUint32(k[len(path)+1:], ordinal)
					out.changes++
					return changes.Add(k, mask)
				})
			}
			if err == nil {
				for i, parent := range r.commit.Parents {
					if err = walk.push(logCandidate{sha: hex.EncodeToString(parent), time: r.commit.ParentTimes[i]}); err != nil {
						break
					}
				}
			}
			r.close()
			if err != nil {
				return err
			}
			out.count++
			if out.count%4096 == 0 {
				if err := stage.checkpoint(); err != nil {
					return err
				}
				t.Logf("layout prepared=%d changed-paths=%d elapsed=%s", out.count, out.changes, time.Since(start))
			}
		}
		// Resolve references only after ordinals are assigned. Uncovered parents
		// remain an explicit gap, including parents newer than their children.
		block := &pb.HistoryLayoutProbeGraph{}
		ids := &pb.HistoryLayoutProbeIDs{}
		flush := func() error {
			if len(block.ParentCounts) == 0 {
				return nil
			}
			ref, err := save(block)
			if err != nil {
				return err
			}
			out.graph = append(out.graph, ref)
			ref, err = save(ids)
			if err != nil {
				return err
			}
			out.ids = append(out.ids, ref)
			block = &pb.HistoryLayoutProbeGraph{FirstOrdinal: uint32(len(out.graph) * probeGraphBlock)}
			ids = &pb.HistoryLayoutProbeIDs{}
			return nil
		}
		for ordinal := 0; ordinal < out.count; ordinal++ {
			var key [4]byte
			binary.BigEndian.PutUint32(key[:], uint32(ordinal))
			var c pb.HistoryBatchCommit
			if err := proto.Unmarshal(stage.buckets["records"].Get(key[:]), &c); err != nil {
				return err
			}
			ids.Oids = append(ids.Oids, c.Oid...)
			block.ParentCounts = append(block.ParentCounts, uint32(len(c.Parents)))
			for i, parent := range c.Parents {
				delta := int64(probeFrontier)
				if v := stage.buckets["ordinals"].Get(parent); v != nil {
					delta = int64(binary.BigEndian.Uint32(v)) - int64(ordinal)
				}
				if len(block.ParentDeltas) == 0 {
					block.TimeOrigin = c.ParentTimes[i]
				}
				block.ParentDeltas = append(block.ParentDeltas, delta)
				block.ParentTimeDeltas = append(block.ParentTimeDeltas, c.ParentTimes[i]-block.TimeOrigin)
			}
			if len(block.ParentCounts) == probeGraphBlock {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		if err := flush(); err != nil {
			return err
		}
		_, err = writeHistoryPaths(func(emit func([]byte, []byte) error) error { return changes.Walk(ctx, emit) }, make([]byte, historyFilterBytes), save)
		if err != nil {
			return err
		}
		out.paths, err = save(&pb.HistoryPathPage{Children: leaves})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := proto.Marshal(&pb.HistoryLayoutProbeCatalog{Count: uint32(out.count), Graph: out.graph, Ids: out.ids, Paths: out.paths})
	if err != nil {
		t.Fatal(err)
	}
	packed := encoder.EncodeAll(catalog, nil)
	if err := out.backend.Put(ctx, "catalog", packed, "*"); err != nil {
		t.Fatal(err)
	}
	out.sizes["catalog"] = len(packed)
	t.Logf("layout catalog=%dB", len(packed))

	t.Logf("layout commits=%d preparation+encoding=%s graph=%dB IDs=%dB all-path-postings+dictionary=%dB changed-paths=%d (display and manifest not included)", out.count, time.Since(start), out.graphBytes, out.idBytes, out.pathBytes, out.changes)
	return out
}

type probeCandidate struct {
	ordinal  uint32
	when     int64
	sequence int
}
type probeQueue []probeCandidate

func (q probeQueue) Len() int { return len(q) }
func (q probeQueue) Less(i, j int) bool {
	if q[i].when == q[j].when {
		return q[i].sequence < q[j].sequence
	}
	return q[i].when > q[j].when
}
func (q probeQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *probeQueue) Push(x any)   { *q = append(*q, x.(probeCandidate)) }
func (q *probeQueue) Pop() any     { old := *q; v := old[len(old)-1]; *q = old[:len(old)-1]; return v }

type probeMetrics struct {
	reads, bytes, rounds, visited       int
	first                               time.Duration
	firstReads, firstBytes, firstRounds int
}

// Queries read compressed layout objects only; there is no source repository fallback.
// An explicit graph window uses one contiguous object range. Reads are counted
// even if OS-cached. The graph cache keeps ONE window, bounded by block count;
// dictionary and posting decode are counted separately. The optional GCS run
// measures real network time with no reader cache. Display, abbreviation and
// mount bootstrap are excluded; loading the layout catalog IS included.
func (q *historyLayoutProbe) log(ctx context.Context, path string, count, window int, firstParent bool) ([]string, probeMetrics, error) {
	start := time.Now()
	stats := probeMetrics{}
	catalogBytes, _, err := q.backend.Get(ctx, "catalog", 0, -1)
	if err != nil {
		return nil, stats, err
	}
	stats.reads++
	stats.rounds++
	stats.bytes += len(catalogBytes)
	rawCatalog, err := decodeFrame(catalogBytes, 32<<20)
	if err != nil {
		return nil, stats, err
	}
	var catalog pb.HistoryLayoutProbeCatalog
	if err := proto.Unmarshal(rawCatalog, &catalog); err != nil {
		return nil, stats, err
	}
	view := *q
	q = &view
	q.count, q.graph, q.ids, q.paths = int(catalog.Count), catalog.Graph, catalog.Ids, catalog.Paths
	read := func(ref *pb.PageReference, m proto.Message) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, _, err := q.backend.Get(ctx, ref.Pack, ref.Offset, ref.Length)
		if err != nil {
			return err
		}
		stats.reads++
		stats.bytes += len(data)
		raw, err := decodeFrame(data, 32<<20)
		if err != nil {
			return err
		}
		return proto.Unmarshal(raw, m)
	}
	var posting *pb.HistoryPathPosting
	for ref := q.paths; ref != nil; {
		stats.rounds++
		var page pb.HistoryPathPage
		if err := read(ref, &page); err != nil {
			return nil, stats, err
		}
		if len(page.Children) > 0 {
			i := sort.Search(len(page.Children), func(i int) bool { return string(page.Children[i].MaxName) >= path })
			if i == len(page.Children) {
				break
			}
			ref = page.Children[i].Page
			continue
		}
		i := sort.Search(len(page.Entries), func(i int) bool { return string(page.Entries[i].Path) >= path })
		if i < len(page.Entries) && string(page.Entries[i].Path) == path {
			posting = page.Entries[i]
		}
		break
	}
	type graphBlock struct {
		graph   pb.HistoryLayoutProbeGraph
		offsets []int
	}
	cache := map[int]*graphBlock{}
	var lastIDs pb.HistoryLayoutProbeIDs
	lastIDBlock := -1
	get := func(ordinal uint32) ([]int64, []int64, int64, error) {
		b := int(ordinal) / probeGraphBlock
		if cache[b] == nil {
			clear(cache)
			stats.rounds++
			base := b / window * window
			end := min(base+window, len(q.graph))
			first, last := q.graph[base], q.graph[end-1]
			packed, _, err := q.backend.Get(ctx, first.Pack, first.Offset, last.Offset+last.Length-first.Offset)
			if err != nil {
				return nil, nil, 0, err
			}
			stats.reads++
			stats.bytes += len(packed)
			for i := base; i < end; i++ {
				item := &graphBlock{}
				ref := q.graph[i]
				off := ref.Offset - first.Offset
				raw, err := decodeFrame(packed[off:off+ref.Length], 32<<20)
				if err != nil {
					return nil, nil, 0, err
				}
				if err := proto.Unmarshal(raw, &item.graph); err != nil {
					return nil, nil, 0, err
				}
				item.offsets = make([]int, len(item.graph.ParentCounts)+1)
				for j, n := range item.graph.ParentCounts {
					item.offsets[j+1] = item.offsets[j] + int(n)
				}
				cache[i] = item
			}
		}
		item := cache[b]
		j := int(ordinal) % probeGraphBlock
		a, z := item.offsets[j], item.offsets[j+1]
		return item.graph.ParentDeltas[a:z], item.graph.ParentTimeDeltas[a:z], item.graph.TimeOrigin, nil
	}
	seen := make([]byte, (q.count+7)/8)
	seen[0] = 1
	queue := probeQueue{{ordinal: 0}}
	seq := 1
	var result []string
	for len(queue) > 0 && len(result) < count {
		c := heap.Pop(&queue).(probeCandidate)
		if c.ordinal == ^uint32(0) {
			return result, stats, errProbeFrontier
		}
		stats.visited++
		parents, times, origin, err := get(c.ordinal)
		if err != nil {
			return result, stats, err
		}
		var mask []byte
		if posting != nil {
			i := sort.Search(len(posting.Ordinals), func(i int) bool { return posting.Ordinals[i] >= c.ordinal })
			if i < len(posting.Ordinals) && posting.Ordinals[i] == c.ordinal {
				mask = posting.DifferentParents[i]
			}
		}
		n := len(parents)
		if firstParent && n > 1 {
			n = 1
		}
		same := -1
		for i := 0; i < n; i++ {
			if len(mask) == 0 || mask[i/8]&(1<<uint(i%8)) == 0 {
				same = i
				break
			}
		}
		show := n > 0 && same < 0 || n == 0 && len(mask) > 0 && mask[0]&1 != 0
		if show {
			b := int(c.ordinal) / probeGraphBlock
			if lastIDBlock != b {
				stats.rounds++
				if err := read(q.ids[b], &lastIDs); err != nil {
					return result, stats, err
				}
				lastIDBlock = b
			}
			pos := (int(c.ordinal) % probeGraphBlock) * 20
			result = append(result, hex.EncodeToString(lastIDs.Oids[pos:pos+20]))
			if len(result) == 1 {
				stats.first = time.Since(start)
				stats.firstReads, stats.firstBytes, stats.firstRounds = stats.reads, stats.bytes, stats.rounds
			}
			if len(result) == count {
				break
			}
		}
		for i := 0; i < n; i++ {
			if same >= 0 && i != same {
				continue
			}
			ordinal := ^uint32(0)
			if parents[i] != probeFrontier {
				target := int64(c.ordinal) + parents[i]
				if target < 0 || target >= int64(q.count) {
					return result, stats, fmt.Errorf("invalid probe parent")
				}
				ordinal = uint32(target)
				if seen[ordinal/8]&(1<<(ordinal%8)) != 0 {
					continue
				}
				seen[ordinal/8] |= 1 << (ordinal % 8)
			}
			heap.Push(&queue, probeCandidate{ordinal: ordinal, when: origin + times[i], sequence: seq})
			seq++
		}
	}
	return result, stats, nil
}

func TestHistoryLayoutProbeOrdering(t *testing.T) {
	dir := historySkewFixture(t)
	command(t, dir, "repack", "-ad")
	tip := command(t, dir, "rev-parse", "HEAD")
	q := buildHistoryLayoutProbe(t, filepath.Join(dir, ".git"), tip, 1000)
	for _, path := range []string{"hot", "nested/rare", "nested", "nested/", "missing", ""} {
		for _, fp := range []bool{false, true} {
			got, _, err := q.log(t.Context(), path, 1000, 1, fp)
			if err != nil {
				t.Fatal(err)
			}
			args := []string{"log", "--format=%H"}
			if fp {
				args = append(args, "--first-parent")
			}
			args = append(args, tip, "--")
			if path != "" {
				args = append(args, path)
			} else {
				args = append(args, ".")
			}
			want := command(t, dir, args...)
			if strings.Join(got, "\n") != want {
				t.Fatalf("%q first-parent=%t differs: %v != %s", path, fp, got, want)
			}
		}
	}
	partial := buildHistoryLayoutProbe(t, filepath.Join(dir, ".git"), tip, 3)
	_, _, err := partial.log(t.Context(), "missing", 1000, 1, false)
	if !errors.Is(err, errProbeFrontier) {
		t.Fatalf("missing ancestry must not be EOF: %v", err)
	}
}

// Cross a full graph block and a partial tail. This catches offset errors that
// the small skewed-DAG fixture cannot exercise.
func TestHistoryLayoutProbeBlocks(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	var stream strings.Builder
	stream.WriteString("blob\nmark :1\ndata 1\na\nblob\nmark :2\ndata 1\nb\n")
	for i := 0; i < probeGraphBlock+17; i++ {
		fmt.Fprintf(&stream, "commit refs/heads/main\ncommitter Test <test@example.test> %d +0000\ndata 1\nx\nM 100644 :%d nested/file\n\n", 1000000000+i, 1+i%2)
	}
	c := exec.Command("git", "-C", dir, "fast-import", "--quiet")
	c.Stdin = strings.NewReader(stream.String())
	if out, err := c.CombinedOutput(); err != nil {
		t.Fatalf("fixture: %v %s", err, out)
	}
	command(t, dir, "repack", "-ad")
	tip := command(t, dir, "rev-parse", "HEAD")
	q := buildHistoryLayoutProbe(t, filepath.Join(dir, ".git"), tip, probeGraphBlock+17)
	for _, window := range []int{1, 32} {
		got, _, err := q.log(t.Context(), "nested/", probeGraphBlock+20, window, false)
		if err != nil {
			t.Fatal(err)
		}
		want := command(t, dir, "log", "--format=%H", tip, "--", "nested/")
		if strings.Join(got, "\n") != want {
			t.Fatalf("window %d differs across block boundary", window)
		}
	}
}

func TestRepositoryHistoryLayoutProbe(t *testing.T) {
	t.Setenv("GIT_NO_LAZY_FETCH", "1")
	t.Setenv("GIT_TERMINAL_PROMPT", "0")
	dir := os.Getenv("GYIT_HISTORY_SOURCE")
	if dir == "" {
		t.Skip("set GYIT_HISTORY_SOURCE")
	}
	tip := os.Getenv("GYIT_HISTORY_SHA")
	if tip == "" {
		tip = "HEAD"
	}
	tip = strings.TrimSpace(command(t, dir, "rev-parse", tip))
	limit := 8192
	if v := os.Getenv("GYIT_HISTORY_LAYOUT_LIMIT"); v != "" {
		var err error
		limit, err = strconv.Atoi(v)
		if err != nil || limit < 1 || limit > 1500000 {
			t.Fatal("layout limit must be 1..1500000")
		}
	}
	q := buildHistoryLayoutProbe(t, dir, tip, limit)
	windows := []int{1, 32, 256}
	if location := os.Getenv("GYIT_HISTORY_LAYOUT_STORE"); location != "" {
		if !strings.Contains(location, "/diagnostics-history-layout-") {
			t.Fatal("remote layout probe requires a diagnostics-history-layout- prefix")
		}
		remote, err := store.Open(t.Context(), location, "", "")
		if err != nil {
			t.Fatal(err)
		}
		for name := range q.sizes {
			data, err := os.ReadFile(filepath.Join(q.dir, name))
			if err != nil {
				t.Fatal(err)
			}
			if err := remote.Put(t.Context(), name, data, "*"); err != nil {
				t.Fatal(err)
			}
		}
		q.backend = remote
		windows = []int{32, 256}
		t.Logf("remote layout objects published; caller must delete this diagnostic prefix after the test")
	}
	paths := []string{"lib", "Documentation/", "Kconfig", "COPYING", "README"}
	if v := os.Getenv("GYIT_HISTORY_PATH"); v != "" {
		paths = strings.Split(v, ",")
	}
	for _, path := range paths {
		for _, count := range []int{1, 10} {
			for _, window := range windows {
				t.Run(fmt.Sprintf("%s/n%d/window%d", path, count, window), func(t *testing.T) {
					started := time.Now()
					want := strings.TrimSpace(command(t, dir, "log", "--format=%H", "-n", fmt.Sprint(count), tip, "--", path))
					native := time.Since(started)
					started = time.Now()
					got, m, err := q.log(t.Context(), path, count, window, false)
					elapsed := time.Since(started)
					t.Logf("query=%s first=%s native-Git=%s visited=%d reads=%d bytes=%d waves=%d first-reads=%d first-bytes=%d first-waves=%d results=%d", elapsed, m.first, native, m.visited, m.reads, m.bytes, m.rounds, m.firstReads, m.firstBytes, m.firstRounds, len(got))
					if err != nil {
						t.Fatal(err)
					}
					if strings.Join(got, "\n") != want {
						t.Fatalf("result differs: %v != %s", got, want)
					}
				})
			}
		}
	}
}
