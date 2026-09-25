package planner

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wire "gat/internal/archive/wire"
)

func openSourceFixture(t *testing.T, prefix string) *Planner {
	t.Helper()
	p, e := OpenSource(t.Context(), prefix, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := p.Close(); e != nil {
			t.Error(e)
		}
	})
	return p
}
func key(id []byte) (out [20]byte) { copy(out[:], id); return }
func readSourceRecipe(t *testing.T, prefix string, r wire.Recipe, kind string) []byte {
	t.Helper()
	packed, e := os.ReadFile(prefix + ".pack")
	if e != nil {
		t.Fatal(e)
	}
	b, _, e := wire.ReadObject(t.Context(), kind, r, r.TargetOID, func(_ context.Context, s uint64, o, n uint32) ([]byte, error) {
		start := s*wire.SegmentSize + uint64(o)
		return packed[start : start+uint64(n)], nil
	}, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func assertMetadata(t *testing.T, p *Planner, id []byte, kind byte, size int64) {
	t.Helper()
	k, n, found, e := p.Lookup(key(id))
	if e != nil || !found || k != kind || n != size {
		t.Fatalf("metadata got%d/%d/%t want%d/%d: %v", k, n, found, kind, size, e)
	}
}

func TestSourceFullOFSREFForwardKindsAndReadback(t *testing.T) {
	a := bytes.Repeat([]byte("a"), 4096)
	b := bytes.Repeat([]byte("b"), 4096)
	c := bytes.Repeat([]byte("c"), 4096)
	tree := []byte("100644 one\x00abcdefghijklmnopqrst")
	tree2 := []byte("100644 two\x00abcdefghijklmnopqrst")
	entries := []fixtureEntry{
		{kind: 3, raw: a}, {kind: 6, raw: literalDelta(len(a), b), target: b, baseIndex: 0},
		{kind: 7, raw: literalDelta(len(b), c), target: c, baseID: objectID("blob", b)},
		{kind: 7, raw: literalDelta(len(tree), tree2), target: tree2, baseID: objectID("tree", tree), id: objectID("tree", tree2)},
		{kind: 2, raw: tree}, {kind: 1, raw: []byte("commit metadata")}, {kind: 4, raw: []byte("tag metadata")},
		{kind: 3, raw: nil},
	}
	prefix, ids := fixture(t, entries)
	p := openSourceFixture(t, prefix)
	kinds := []byte{3, 3, 3, 2, 2, 1, 4, 3}
	bodies := [][]byte{a, b, c, tree2, tree, entries[5].raw, entries[6].raw, nil}
	for i, id := range ids {
		assertMetadata(t, p, id, kinds[i], int64(len(bodies[i])))
		if kinds[i] != 2 && kinds[i] != 3 {
			continue
		}
		r, e := p.Recipe(hex.EncodeToString(id), [16]byte{1})
		if e != nil {
			t.Fatal(e)
		}
		kind := "blob"
		if kinds[i] == 2 {
			kind = "tree"
		}
		if got := readSourceRecipe(t, prefix, r, kind); !bytes.Equal(got, bodies[i]) {
			t.Fatalf("read%d mismatch", i)
		}
	}
	var all, blobs []int64
	if e := p.ForEachObject(t.Context(), func(id [20]byte, k byte, size int64) error {
		all = append(all, size)
		if k != kinds[len(all)-1] || id != key(ids[len(all)-1]) {
			t.Fatal("physical metadata order")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if e := p.ForEachBlob(t.Context(), func(_ [20]byte, size int64) error { blobs = append(blobs, size); return nil }); e != nil {
		t.Fatal(e)
	}
	if len(all) != 8 || len(blobs) != 4 || blobs[3] != 0 {
		t.Fatal(all, blobs)
	}
	if _, _, found, e := p.Lookup([20]byte{255}); e != nil || found {
		t.Fatal("missing key", e)
	}
	s := p.Stats()
	if s.SourceHeaders != 8 || s.InventoryLookups != 0 || s.PrefixInflations != 3 || s.PrefixOutputBytes > 60 || s.PrefixInputBytes > 3*PrefixInputLimit || s.TypeDPNodeVisits != 8 || s.TypeDPEdgeVisits != 3 || s.RootCommits != 1 || s.RootTrees != 1 || s.RootBlobs != 2 || s.RootTags != 1 || s.MetadataBlobs != 4 || s.MetadataBlobBytes != 12288 {
		t.Fatalf("source counters %+v", s)
	}
	if s.FullInflations != 0 || s.Reconstructions != 0 || s.PayloadCopiedBytes != 0 || s.RecordBytes != 8*40 || s.ScratchMappedBytes != s.RecordBytes+s.HashSlotBytes || s.BuildPeakScratchMappedBytes != s.ScratchMappedBytes+s.BuildStackBytes {
		t.Fatalf("bounds %+v", s)
	}
}

func sizeProgram(base, target uint64) []byte {
	return binary.AppendUvarint(binary.AppendUvarint(nil, base), target)
}

func TestSourceLargeMetadataThroughIneligibleAncestors(t *testing.T) {
	large := uint64(math.MaxUint32) + 123
	maximal := uint64(math.MaxInt64)
	rootID := objectID("blob", []byte("declared-large-root"))
	middleID := objectID("blob", []byte("declared-large-middle"))
	smallID := objectID("blob", []byte("small-child"))
	entries := []fixtureEntry{
		{kind: 7, raw: sizeProgram(large, 7), id: smallID, baseID: middleID},
		{kind: 6, raw: sizeProgram(large, large), id: middleID, baseIndex: 0},
	}
	// Use forward REF for both children; no body of the declared huge root is
	// allocated. Metadata extraction deliberately does not authenticate bodies.
	entries[1] = fixtureEntry{kind: 7, raw: sizeProgram(large, large), id: middleID, baseID: rootID}
	entries = append(entries, fixtureEntry{kind: 3, raw: []byte("unused"), id: rootID, declared: &large}, fixtureEntry{kind: 2, raw: []byte("unused-tree"), declared: &maximal})
	prefix, ids := fixture(t, entries)
	p := openSourceFixture(t, prefix)
	for i, want := range []int64{7, int64(large), int64(large), math.MaxInt64} {
		k := byte(3)
		if i == 3 {
			k = 2
		}
		assertMetadata(t, p, ids[i], k, want)
	}
	for _, id := range ids {
		if _, e := p.Recipe(hex.EncodeToString(id), [16]byte{}); !errors.Is(e, ErrLimit) {
			t.Fatalf("large chain must retain fallback: %v", e)
		}
	}
	var sizes []int64
	if e := p.ForEachBlob(t.Context(), func(_ [20]byte, n int64) error { sizes = append(sizes, n); return nil }); e != nil {
		t.Fatal(e)
	}
	if len(sizes) != 3 || sizes[0] != 7 || sizes[1] != int64(large) || sizes[2] != int64(large) {
		t.Fatal(sizes)
	}
	if p.Stats().TypeDPNodeVisits != 4 || p.Stats().TypeDPEdgeVisits != 2 {
		t.Fatal(p.Stats())
	}
}

func TestSourceUnsupportedMetadataCleansUp(t *testing.T) {
	aID, bID := objectID("blob", []byte("a")), objectID("blob", []byte("b"))
	tooLarge := uint64(math.MaxInt64) + 1
	for _, tc := range []struct {
		name    string
		entries []fixtureEntry
	}{
		{"cycle", []fixtureEntry{{kind: 7, raw: sizeProgram(1, 1), id: aID, baseID: bID}, {kind: 7, raw: sizeProgram(1, 1), id: bID, baseID: aID}}},
		{"missing", []fixtureEntry{{kind: 7, raw: sizeProgram(1, 1), id: aID, baseID: bID}}},
		{"invalid-kind", []fixtureEntry{{kind: 0, raw: []byte("bad"), id: aID}}},
		{"bad-prefix", []fixtureEntry{{kind: 3, raw: []byte("base")}, {kind: 6, raw: []byte{128, 128}, id: aID, baseIndex: 0}}},
		{"corrupt-prefix", []fixtureEntry{{kind: 3, raw: []byte("base")}, {kind: 6, raw: []byte{4, 1}, id: aID, baseIndex: 0, badCompressed: true}}},
		{"root-api-overflow", []fixtureEntry{{kind: 3, raw: []byte("tiny"), declared: &tooLarge}}},
		{"delta-api-overflow", []fixtureEntry{{kind: 3, raw: []byte("base")}, {kind: 6, raw: sizeProgram(4, tooLarge), id: aID, baseIndex: 0}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A valid but unrelated object cannot make incomplete source metadata
			// acceptable; the caller must choose its ordinary Git metadata path.
			tc.entries = append(tc.entries, fixtureEntry{kind: 3, raw: []byte("valid unrelated")})
			prefix, _ := fixture(t, tc.entries)
			tmp := t.TempDir()
			p, e := OpenSource(t.Context(), prefix, tmp)
			if p != nil || !errors.Is(e, ErrUnsupported) {
				t.Fatalf("expected complete-provider fallback: %v", e)
			}
			files, e := os.ReadDir(tmp)
			if e != nil || len(files) != 0 {
				t.Fatal("scratch leak", files, e)
			}
		})
	}
}

func compressPrefix(t *testing.T, raw []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	if _, e := w.Write(raw); e != nil {
		t.Fatal(e)
	}
	if e := w.Close(); e != nil {
		t.Fatal(e)
	}
	return b.Bytes()
}

func TestSourcePrefixBoundsAndBodyDeferral(t *testing.T) {
	var out [PrefixOutputLimit]byte
	body := append(sizeProgram(4096, 4096), bytes.Repeat([]byte{99}, 10000)...)
	n, consumed, e := metadataPrefix(out[:], compressPrefix(t, body), uint64(len(body)))
	if e != nil || n != 20 || consumed > PrefixInputLimit {
		t.Fatal(n, consumed, e)
	}
	// More than64KiB of empty stored blocks cannot consume unbounded input
	// while attempting to produce two varints. No large decoded body exists.
	packed := []byte{0x78, 0x01}
	for i := 0; i < 14000; i++ {
		packed = append(packed, 0, 0, 0, 255, 255)
	}
	n, consumed, e = metadataPrefix(out[:], packed, 2)
	if !errors.Is(e, ErrUnsupported) || n != 0 || consumed != PrefixInputLimit {
		t.Fatal(n, consumed, e)
	}
	prefix, _ := fixture(t, []fixtureEntry{{kind: 3, raw: []byte("base")}, {kind: 6, raw: []byte{4, 1}, target: []byte("x"), baseIndex: 0, packed: packed}})
	tmp := t.TempDir()
	if p, e := OpenSource(t.Context(), prefix, tmp); p != nil || !errors.Is(e, ErrUnsupported) {
		t.Fatal(p, e)
	}
	if files, e := os.ReadDir(tmp); e != nil || len(files) != 0 {
		t.Fatal(files, e)
	}
	// Root body corruption is outside metadata extraction and is rejected by
	// authenticated reads, before bytes are returned to the caller.
	prefix, ids := fixture(t, []fixtureEntry{{kind: 3, raw: []byte("corrupt-body"), badCompressed: true}})
	p := openSourceFixture(t, prefix)
	assertMetadata(t, p, ids[0], 3, 12)
	r, e := p.Recipe(hex.EncodeToString(ids[0]), [16]byte{})
	if e != nil {
		t.Fatal(e)
	}
	pack, e := os.ReadFile(prefix + ".pack")
	if e != nil {
		t.Fatal(e)
	}
	b, _, e := wire.Read(t.Context(), r, r.TargetOID, func(_ context.Context, s uint64, o, n uint32) ([]byte, error) {
		start := s*wire.SegmentSize + uint64(o)
		return pack[start : start+uint64(n)], nil
	}, nil, nil)
	if e == nil || b != nil {
		t.Fatal("corrupt root read exposed data", e)
	}
}

func TestSourceRecipeDepthWorkAndRanges(t *testing.T) {
	for _, tc := range []struct {
		name        string
		depth, size int
		gaps, limit bool
	}{{"depth64", 64, 4096, false, false}, {"depth65", 65, 4096, false, true}, {"work", 5, 600000, false, true}, {"four-ranges", 4, 4096, true, false}, {"five-ranges", 5, 4096, true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			var entries []fixtureEntry
			prior := -1
			for i := 0; i < tc.depth; i++ {
				raw := bytes.Repeat([]byte{byte(i + 1)}, tc.size)
				e := fixtureEntry{kind: 3, raw: raw}
				if prior >= 0 {
					e = fixtureEntry{kind: 6, raw: literalDelta(tc.size, raw), target: raw, baseIndex: prior}
				}
				prior = len(entries)
				entries = append(entries, e)
				if tc.gaps {
					entries = append(entries, fixtureEntry{kind: 3, raw: []byte(fmt.Sprintf("gap%d", i))})
				}
			}
			prefix, ids := fixture(t, entries)
			p := openSourceFixture(t, prefix)
			assertMetadata(t, p, ids[prior], 3, int64(tc.size))
			_, e := p.Recipe(hex.EncodeToString(ids[prior]), [16]byte{})
			if tc.limit && !errors.Is(e, ErrLimit) || !tc.limit && e != nil {
				t.Fatal(e)
			}
			if p.Stats().MetadataObjects != uint64(len(entries)) {
				t.Fatal("ineligible metadata incomplete")
			}
		})
	}
}

func TestSourceCancellationIterationAndClose(t *testing.T) {
	prefix, ids := fixture(t, []fixtureEntry{{kind: 3, raw: []byte("one")}, {kind: 2, raw: []byte("tree")}})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	tmp := t.TempDir()
	if p, e := OpenSource(ctx, prefix, tmp); p != nil || !errors.Is(e, context.Canceled) {
		t.Fatal(p, e)
	}
	if files, e := os.ReadDir(tmp); e != nil || len(files) != 0 {
		t.Fatal(files, e)
	}
	p := openSourceFixture(t, prefix)
	cause := errors.New("stop iteration")
	if e := p.ForEachBlob(t.Context(), func([20]byte, int64) error { return cause }); !errors.Is(e, cause) {
		t.Fatal(e)
	}
	if e := p.ForEachObject(ctx, func([20]byte, byte, int64) error { t.Fatal("canceled iterator called callback"); return nil }); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	iteration := make(chan error, 1)
	closed := make(chan error, 1)
	go func() {
		iteration <- p.ForEachObject(t.Context(), func([20]byte, byte, int64) error {
			select {
			case <-entered:
			default:
				close(entered)
			}
			<-release
			return nil
		})
	}()
	<-entered
	go func() { closed <- p.Close() }()
	select {
	case e := <-closed:
		t.Fatal("Close passed active callback", e)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if e := <-iteration; e != nil {
		t.Fatal(e)
	}
	if e := <-closed; e != nil {
		t.Fatal(e)
	}
	if _, _, _, e := p.Lookup(key(ids[0])); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	if e := p.ForEachBlob(t.Context(), func([20]byte, int64) error { return nil }); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	if e := p.Close(); e != nil {
		t.Fatal(e)
	}
	if files, e := os.ReadDir(filepath.Dir(p.scratchDir)); e != nil || len(files) != 0 {
		t.Fatal(files, e)
	}
}

func TestSourceConcurrentLookupsAndRecipes(t *testing.T) {
	prefix, ids := fixture(t, []fixtureEntry{{kind: 3, raw: bytes.Repeat([]byte("a"), 4096)}})
	p := openSourceFixture(t, prefix)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 20 {
				_, _, found, e := p.Lookup(key(ids[0]))
				if e != nil || !found {
					t.Error(e)
				}
				if _, e := p.Recipe(hex.EncodeToString(ids[0]), [16]byte{}); e != nil {
					t.Error(e)
				}
			}
		})
	}
	wg.Wait()
	if p.Stats().SourceHeaders != 1 || p.Stats().RecipeCalls != 160 {
		t.Fatal(p.Stats())
	}
}

func TestSourceEmptyAndOverlongMetadata(t *testing.T) {
	prefix, _ := fixture(t, nil)
	p := openSourceFixture(t, prefix)
	if e := p.ForEachObject(t.Context(), func([20]byte, byte, int64) error { t.Fatal("empty source callback"); return nil }); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if e := p.ForEachObject(ctx, func([20]byte, byte, int64) error { return nil }); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if p.Stats().MetadataObjects != 0 || p.Stats().PrefixInflations != 0 {
		t.Fatal(p.Stats())
	}
	base := bytes.Repeat([]byte("a"), 4096)
	for _, basePrefix := range []bool{false, true} {
		overlong := []byte{0x80, 0xa0, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00}
		// Overlong encoding of4096 has a valid uint64 value, but cannot be
		// represented by the unchanged mounted native delta decoder.
		program := binary.AppendUvarint(nil, 4096)
		if basePrefix {
			program = append(overlong, program...)
		} else {
			program = append(program, overlong...)
		}
		prefix, ids := fixture(t, []fixtureEntry{{kind: 3, raw: base}, {kind: 6, raw: program, target: bytes.Repeat([]byte("b"), 4096), baseIndex: 0}})
		p := openSourceFixture(t, prefix)
		assertMetadata(t, p, ids[1], 3, 4096)
		if _, e := p.Recipe(hex.EncodeToString(ids[1]), [16]byte{}); !errors.Is(e, ErrLimit) {
			t.Fatal(e)
		}
	}
}

type cancelDuringConstruction struct {
	context.Context
	cancel context.CancelFunc
	checks atomic.Int64
}

func (c *cancelDuringConstruction) Err() error {
	if c.checks.Add(1) == 6 {
		c.cancel()
	}
	return c.Context.Err()
}
func TestSourceCancellationAfterMappingsAllocated(t *testing.T) {
	prefix, _ := fixture(t, []fixtureEntry{{kind: 3, raw: []byte("one")}, {kind: 3, raw: []byte("two")}, {kind: 2, raw: []byte("tree")}})
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &cancelDuringConstruction{Context: base, cancel: cancel}
	tmp := t.TempDir()
	p, e := OpenSource(ctx, prefix, tmp)
	if p != nil || !errors.Is(e, context.Canceled) || errors.Is(e, ErrUnsupported) {
		t.Fatal(p, e)
	}
	files, e := os.ReadDir(tmp)
	if e != nil || len(files) != 0 {
		t.Fatal("partial source mappings leaked", files, e)
	}
}
