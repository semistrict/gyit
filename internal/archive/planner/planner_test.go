package planner

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	wire "gat/internal/archive/wire"
)

type metadata struct {
	kind byte
	size int64
}

func lookupFor(ids [][]byte, values []metadata) Lookup {
	m := make(map[[20]byte]metadata, len(ids))
	for i, id := range ids {
		var key [20]byte
		copy(key[:], id)
		m[key] = values[i]
	}
	return func(id [20]byte) (byte, int64, bool, error) { v, ok := m[id]; return v.kind, v.size, ok, nil }
}
func openFixture(t *testing.T, prefix string, ids [][]byte, values []metadata) *Planner {
	t.Helper()
	p, err := Open(t.Context(), prefix, lookupFor(ids, values), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	return p
}
func readFixture(t *testing.T, prefix string, recipe wire.Recipe, kind string) []byte {
	t.Helper()
	packed, err := os.ReadFile(prefix + ".pack")
	if err != nil {
		t.Fatal(err)
	}
	raw, _, err := wire.ReadObject(t.Context(), kind, recipe, recipe.TargetOID, func(_ context.Context, segment uint64, off, length uint32) ([]byte, error) {
		start := segment*wire.SegmentSize + uint64(off)
		return packed[start : start+uint64(length)], nil
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestPlannerFullOFSREFAndForwardTree(t *testing.T) {
	blob := bytes.Repeat([]byte("a"), 4096)
	b := bytes.Repeat([]byte("b"), 4096)
	c := bytes.Repeat([]byte("c"), 4096)
	// Arbitrary tree bytes here exercise the object-kind authentication path;
	// directory syntax is the mounted reader's separate bounded responsibility.
	tree := []byte("100644 one\x00abcdefghijklmnopqrst")
	tree2 := []byte("100644 two\x00abcdefghijklmnopqrst")
	rootTreeID := objectID("tree", tree)
	entries := []fixtureEntry{
		{kind: 3, raw: blob},
		{kind: 6, raw: literalDelta(len(blob), b), target: b, baseIndex: 0},
		{kind: 7, raw: literalDelta(len(b), c), target: c, baseID: objectID("blob", b)},
		{kind: 7, raw: literalDelta(len(tree), tree2), target: tree2, baseID: rootTreeID, id: objectID("tree", tree2)},
		{kind: 2, raw: tree},
	}
	prefix, ids := fixture(t, entries)
	p := openFixture(t, prefix, ids, []metadata{{3, 4096}, {3, 4096}, {3, 4096}, {2, int64(len(tree2))}, {2, int64(len(tree))}})
	for i, want := range [][]byte{blob, b, c, tree2, tree} {
		r, err := p.Recipe(hex.EncodeToString(ids[i]), [16]byte{1})
		if err != nil {
			t.Fatalf("recipe%d: %v", i, err)
		}
		kind := "blob"
		if i >= 3 {
			kind = "tree"
		}
		if got := readFixture(t, prefix, r, kind); !bytes.Equal(got, want) {
			t.Fatalf("read%d mismatch", i)
		}
	}
	stats := p.Stats()
	if stats.SourceHeaders != 5 || stats.InventoryLookups != 5 || stats.DPNodeVisits != 5 || stats.DPEdgeVisits != 3 || stats.Roots != 2 || stats.OFSParents != 1 || stats.REFParents != 2 {
		t.Fatalf("unique source work: %+v", stats)
	}
	if stats.RecipeAccepted != 5 || stats.BlobAccepted != 3 || stats.TreeAccepted != 2 || stats.PrefixInflations != 0 || stats.FullInflations != 0 || stats.Reconstructions != 0 || stats.PayloadCopiedBytes != 0 {
		t.Fatalf("work counters: %+v", stats)
	}
	if stats.ScratchMappedBytes != stats.RecordBytes+stats.HashSlotBytes || stats.BuildPeakScratchMappedBytes != stats.ScratchMappedBytes+stats.BuildStackBytes {
		t.Fatalf("scratch accounting: %+v", stats)
	}
}

func TestPlannerMalformedFamilyDoesNotPoisonValidRoot(t *testing.T) {
	good := bytes.Repeat([]byte("x"), 4096)
	a := bytes.Repeat([]byte("a"), 4096)
	b := bytes.Repeat([]byte("b"), 4096)
	aID, bID := objectID("blob", a), objectID("blob", b)
	entries := []fixtureEntry{
		{kind: 3, raw: good},
		{kind: 7, raw: literalDelta(len(b), a), target: a, baseID: bID},
		{kind: 7, raw: literalDelta(len(a), b), target: b, baseID: aID},
		{kind: 7, raw: []byte{0, 0}, target: []byte("missing"), baseID: bytes.Repeat([]byte{0xff}, 20)},
		{kind: 0, raw: []byte("bad-header"), id: objectID("blob", []byte("bad-header"))},
	}
	prefix, ids := fixture(t, entries)
	p := openFixture(t, prefix, ids, []metadata{{3, 4096}, {3, 4096}, {3, 4096}, {3, 7}, {3, 10}})
	if _, err := p.Recipe(hex.EncodeToString(ids[0]), [16]byte{}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(ids); i++ {
		if _, err := p.Recipe(hex.EncodeToString(ids[i]), [16]byte{}); !errors.Is(err, ErrMalformed) {
			t.Fatalf("bad family%d: %v", i, err)
		}
	}
	if got := p.Stats().MalformedNodes; got != 4 {
		t.Fatalf("malformed nodes %d", got)
	}
}

func TestPlannerAdmissionAndCorruptionDeferredToRead(t *testing.T) {
	root := bytes.Repeat([]byte("a"), 4096)
	small := []byte("small")
	largeTree := bytes.Repeat([]byte("t"), 65537)
	oversized := uint64(wire.ObjectLimit + 1)
	entries := []fixtureEntry{
		{kind: 3, raw: root},
		{kind: 6, raw: literalDelta(len(root), small), target: small, baseIndex: 0},
		{kind: 2, raw: largeTree},
		{kind: 3, raw: []byte("unused"), id: objectID("blob", []byte("large")), declared: &oversized},
		{kind: 3, raw: []byte("corrupt"), badCompressed: true},
	}
	prefix, ids := fixture(t, entries)
	p := openFixture(t, prefix, ids, []metadata{{3, 4096}, {3, 5}, {2, int64(len(largeTree))}, {3, int64(oversized)}, {3, 7}})
	for _, i := range []int{2, 3} {
		if _, err := p.Recipe(hex.EncodeToString(ids[i]), [16]byte{}); !errors.Is(err, wire.ErrLimit) {
			t.Fatalf("limit%d: %v", i, err)
		}
	}
	r, err := p.Recipe(hex.EncodeToString(ids[4]), [16]byte{})
	if err != nil {
		t.Fatalf("body must be deferred: %v", err)
	}
	packed, err := os.ReadFile(prefix + ".pack")
	if err != nil {
		t.Fatal(err)
	}
	// A small target derived from a larger base is valid when its absolute
	// reconstruction work stays bounded; admission no longer uses a size ratio.
	smallRecipe, err := p.Recipe(hex.EncodeToString(ids[1]), [16]byte{})
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := wire.Read(t.Context(), smallRecipe, smallRecipe.TargetOID, func(_ context.Context, s uint64, o, n uint32) ([]byte, error) {
		start := s*wire.SegmentSize + uint64(o)
		return packed[start : start+uint64(n)], nil
	}, nil, nil)
	if err != nil || !bytes.Equal(got, small) {
		t.Fatalf("bounded small target read = %q, %v", got, err)
	}
	_, _, err = wire.Read(t.Context(), r, r.TargetOID, func(_ context.Context, s uint64, o, n uint32) ([]byte, error) {
		start := s*wire.SegmentSize + uint64(o)
		return packed[start : start+uint64(n)], nil
	}, nil, nil)
	if err == nil {
		t.Fatal("read accepted corrupt compressed bytes")
	}
	if p.Stats().SourceHeaders != uint64(len(entries)) {
		t.Fatal("did not scan all source headers once")
	}
}

func TestPlannerDepthWorkAndRangeAdmission(t *testing.T) {
	for _, tc := range []struct {
		name         string
		levels, size int
		gaps         bool
		wantLimit    bool
	}{
		{"depth64", 64, 4096, false, false},
		{"depth65", 65, 4096, false, true},
		{"work", 5, 600000, false, true},
		{"four-ranges", 4, 4096, true, false},
		{"five-ranges", 5, 4096, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var entries []fixtureEntry
			var values []metadata
			prior := -1
			for i := 0; i < tc.levels; i++ {
				raw := bytes.Repeat([]byte{byte(i + 1)}, tc.size)
				entry := fixtureEntry{kind: 3, raw: raw}
				if i > 0 {
					entry = fixtureEntry{kind: 6, raw: literalDelta(tc.size, raw), target: raw, baseIndex: prior}
				}
				prior = len(entries)
				entries = append(entries, entry)
				values = append(values, metadata{3, int64(tc.size)})
				if tc.gaps {
					entries = append(entries, fixtureEntry{kind: 3, raw: []byte(fmt.Sprintf("gap%d", i))})
					values = append(values, metadata{3, int64(len(fmt.Sprintf("gap%d", i)))})
				}
			}
			prefix, ids := fixture(t, entries)
			p := openFixture(t, prefix, ids, values)
			_, err := p.Recipe(hex.EncodeToString(ids[prior]), [16]byte{})
			if tc.wantLimit {
				if !errors.Is(err, ErrLimit) {
					t.Fatalf("want limit: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPlannerRangeGapCanBeFilledByDescendant(t *testing.T) {
	// Parent has five disjoint ranges. Its physically earlier REF child fills
	// one gap and is eligible. This guards against inheriting a nonmonotone
	// range rejection during parent-first metadata DP.
	var entries []fixtureEntry
	var values []metadata
	var mainIDs [][]byte
	for i := 0; i < 6; i++ {
		mainIDs = append(mainIDs, objectID("blob", bytes.Repeat([]byte{byte(i + 1)}, 4096)))
	}
	for i := 0; i < 5; i++ {
		raw := bytes.Repeat([]byte{byte(i + 1)}, 4096)
		e := fixtureEntry{kind: 3, raw: raw}
		if i > 0 {
			e = fixtureEntry{kind: 7, raw: literalDelta(4096, raw), target: raw, baseID: mainIDs[i-1]}
		}
		entries = append(entries, e)
		values = append(values, metadata{3, 4096})
		if i == 0 {
			raw = bytes.Repeat([]byte{6}, 4096)
			entries = append(entries, fixtureEntry{kind: 7, raw: literalDelta(4096, raw), target: raw, baseID: mainIDs[4]})
			values = append(values, metadata{3, 4096})
		} else if i < 4 {
			text := []byte(fmt.Sprintf("gap%d", i))
			entries = append(entries, fixtureEntry{kind: 3, raw: text})
			values = append(values, metadata{3, int64(len(text))})
		}
	}
	prefix, ids := fixture(t, entries)
	p := openFixture(t, prefix, ids, values)
	if _, err := p.Recipe(hex.EncodeToString(mainIDs[4]), [16]byte{}); !errors.Is(err, ErrLimit) {
		t.Fatalf("parent must have five ranges: %v", err)
	}
	r, err := p.Recipe(hex.EncodeToString(ids[1]), [16]byte{})
	if err != nil {
		t.Fatalf("gap-filled descendant: %v", err)
	}
	if got := readFixture(t, prefix, r, "blob"); !bytes.Equal(got, bytes.Repeat([]byte{6}, 4096)) {
		t.Fatal("descendant mismatch")
	}
}

func TestPlannerMetadataFailuresCancellationAndClose(t *testing.T) {
	prefix, ids := fixture(t, []fixtureEntry{{kind: 3, raw: []byte("ok")}, {kind: 2, raw: []byte("tree")}})
	for _, tc := range []struct {
		name   string
		lookup Lookup
	}{
		{"wrong-kind", lookupFor(ids, []metadata{{2, 2}, {2, 4}})},
		{"wrong-size", lookupFor(ids, []metadata{{3, 9}, {2, 4}})},
		{"missing", func(id [20]byte) (byte, int64, bool, error) {
			if bytes.Equal(id[:], ids[0]) {
				return 0, 0, false, nil
			}
			return 2, 4, true, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Open(t.Context(), prefix, tc.lookup, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err = p.Recipe(hex.EncodeToString(ids[0]), [16]byte{}); !errors.Is(err, ErrMalformed) {
				t.Fatalf("metadata mismatch: %v", err)
			}
			if _, err = p.Recipe(hex.EncodeToString(ids[1]), [16]byte{}); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, cancelCase := range []bool{false, true} {
		tmp := t.TempDir()
		ctx, cancel := context.WithCancel(t.Context())
		failure := errors.New("inventory failed")
		p, err := Open(ctx, prefix, func([20]byte) (byte, int64, bool, error) {
			if cancelCase {
				cancel()
				return 3, 2, true, nil
			}
			return 0, 0, false, failure
		}, tmp)
		cancel()
		if p != nil || err == nil {
			t.Fatalf("construction failure: %v", err)
		}
		if cancelCase && !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel: %v", err)
		}
		if !cancelCase && !errors.Is(err, failure) {
			t.Fatalf("I/O: %v", err)
		}
		files, e := os.ReadDir(tmp)
		if e != nil || len(files) != 0 {
			t.Fatalf("scratch leaked: %v %v", files, e)
		}
	}
	p := openFixture(t, prefix, ids, []metadata{{3, 2}, {2, 4}})
	if !p.Has(hex.EncodeToString(ids[0])) || p.Has("invalid") || p.Has(fmt.Sprintf("%040x", 0)) {
		t.Fatal("membership")
	}
	if _, err := p.Recipe(fmt.Sprintf("%040x", 0), [16]byte{}); !errors.Is(err, ErrLimit) {
		t.Fatalf("missing: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 64; j++ {
				_, err := p.Recipe(hex.EncodeToString(ids[0]), [16]byte{})
				if err != nil && !errors.Is(err, ErrClosed) {
					t.Error(err)
				}
				p.Has(hex.EncodeToString(ids[1]))
				_ = p.Stats()
			}
		}()
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(p.scratchDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("scratch close: %v", err)
	}
	if _, err := p.Recipe(hex.EncodeToString(ids[0]), [16]byte{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}

func TestPlannerMalformedReverseIndex(t *testing.T) {
	prefix, ids := fixture(t, []fixtureEntry{{kind: 3, raw: []byte("one")}, {kind: 3, raw: []byte("two")}})
	b, err := os.ReadFile(prefix + ".rev")
	if err != nil {
		t.Fatal(err)
	}
	copy(b[16:20], b[12:16])
	if err = os.WriteFile(prefix+".rev", b, 0600); err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	p, err := Open(t.Context(), prefix, lookupFor(ids, []metadata{{3, 3}, {3, 3}}), tmp)
	if p != nil || !errors.Is(err, ErrMalformed) {
		t.Fatalf("invalid reverse order accepted: %v", err)
	}
	files, e := filepath.Glob(filepath.Join(tmp, "*"))
	if e != nil || len(files) != 0 {
		t.Fatalf("leaked files %v %v", files, e)
	}
}
