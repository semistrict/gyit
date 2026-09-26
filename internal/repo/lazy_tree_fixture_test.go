package repo

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"gyit/internal/packfile"
	storagev1 "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/gitdelta"
	"gyit/internal/store"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

const lazyFixtureTip = "f0100363d8c374bd8e9ea7c9ba02744f0b802ca4"
const lazyFixtureRankGap = 4155 // 4,027 absent blobs plus the maximum 128 leaf items.

type lazyReadFixture struct {
	Backend    store.Store
	BlobRoot   pageRef
	BlobRoutes []pageRef
	Cases      []lazyReadCase
}

type lazyReadCase struct {
	Path, OID                string
	CompiledRoot, NativeRoot pageRef // Main o/tree catalog roots, not directory refs.
	Entries                  []Entry
	StatNames                []string // Proven separate leaves; first names page only.
	Native                   bool
	Admission                string
}

type lazyFixtureInput struct {
	lazyReadCase
	raw   []byte
	ranks map[string]int64
}

func lazyReadTSV(t *testing.T, path string, fields int) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.Comma, r.FieldsPerRecord = '\t', fields
	rows, err := r.ReadAll()
	if err != nil || len(rows) < 2 {
		t.Fatalf("read fixture TSV %s: %v", path, err)
	}
	return rows[1:]
}

func lazyFixtureInputs(t *testing.T, dir string) []lazyFixtureInput {
	t.Helper()
	parse := func(s string, base int) int64 {
		n, err := strconv.ParseInt(s, base, 64)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	provenance, err := os.ReadFile(filepath.Join(dir, "provenance.tsv"))
	if err != nil || !bytes.Contains(provenance, []byte("tip\t"+lazyFixtureTip+"\n")) ||
		!bytes.Contains(provenance, []byte("extra_blobs\t4027\n")) ||
		!bytes.Contains(provenance, []byte("strict_rank_gap\t4155\n")) {
		t.Fatal("wrong fixed fixture provenance", err)
	}
	rows := lazyReadTSV(t, filepath.Join(dir, "cases.tsv"), 7)
	if len(rows) != 3 {
		t.Fatal("fixture needs exactly three fixed directories")
	}
	paths := []string{".", "include/linux", "arch/x86/include/asm"}
	in := make([]lazyFixtureInput, len(rows))
	wantCounts, wantAnchors := make([]int, len(rows)), make([]int, len(rows))
	for i, row := range rows {
		if parse(row[0], 10) != int64(i) || row[1] != paths[i] {
			t.Fatal("fixed directory selection changed")
		}
		raw, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("tree-%d.raw", i)))
		if err != nil {
			t.Fatal(err)
		}
		h := sha1.New()
		fmt.Fprintf(h, "tree %d\x00", len(raw))
		h.Write(raw)
		if len(raw) > 1<<20 || int64(len(raw)) != parse(row[3], 10) || fmt.Sprintf("%x", h.Sum(nil)) != row[2] || fmt.Sprintf("%x", sha256.Sum256(raw)) != row[6] {
			t.Fatal("fixture tree identity mismatch")
		}
		in[i] = lazyFixtureInput{lazyReadCase: lazyReadCase{Path: row[1], OID: row[2]}, raw: raw, ranks: make(map[string]int64)}
		wantCounts[i], wantAnchors[i] = int(parse(row[4], 10)), int(parse(row[5], 10))
	}
	for _, row := range lazyReadTSV(t, filepath.Join(dir, "entries.tsv"), 8) {
		i := int(parse(row[0], 10))
		if i < 0 || i >= len(in) {
			t.Fatal("bad case ordinal")
		}
		name, err := hex.DecodeString(row[1])
		if err != nil {
			t.Fatal(err)
		}
		e := Entry{Name: string(name), OID: row[2], Mode: uint32(parse(row[3], 8)), RawMode: uint32(parse(row[4], 8)), Size: parse(row[5], 10)}
		if len(in[i].Entries) > 0 && in[i].Entries[len(in[i].Entries)-1].Name >= e.Name {
			t.Fatal("entries not strictly name sorted")
		}
		in[i].Entries = append(in[i].Entries, e)
		if row[7] == "1" {
			if len(in[i].Entries) > fanout || e.Mode == 0040000 || e.Mode == 0160000 {
				t.Fatal("stat anchor not a first-page file")
			}
			in[i].StatNames = append(in[i].StatNames, e.Name)
			in[i].ranks[e.OID] = parse(row[6], 10)
		} else if row[7] != "0" {
			t.Fatal("invalid anchor flag")
		}
	}
	for i := range in {
		c := &in[i]
		if len(c.Entries) != wantCounts[i] || len(c.StatNames) != wantAnchors[i] || len(c.StatNames) == 0 || len(c.ranks) != len(c.StatNames) {
			t.Fatal("fixture entry/anchor count mismatch")
		}
		var ranks []int64
		for _, rank := range c.ranks {
			ranks = append(ranks, rank)
		}
		sort.Slice(ranks, func(i, j int) bool { return ranks[i] < ranks[j] })
		for j := 1; j < len(ranks); j++ {
			if ranks[j]-ranks[j-1] <= lazyFixtureRankGap {
				t.Fatal("stat anchors lack strict rank-gap proof")
			}
		}
	}
	return in
}

// One real protobuf leaf per selected OID minimizes size-leaf bytes. Only the
// proven-separated anchor subset is a GET lower bound; non-anchor groupings
// are synthetic and must not be measured as full-directory stat costs.
func lazyFixtureBlobCatalog(t *testing.T, ctx context.Context, backend store.Store, sizes map[string]int64) (pageRef, []pageRef) {
	t.Helper()
	w := &indexWriter{ctx: ctx, store: backend, prefix: "lazy-fixture-blobs"}
	var ids []string
	for oid := range sizes {
		ids = append(ids, oid)
	}
	sort.Strings(ids)
	var level []*storagev1.DirectBlobChild
	for _, oid := range ids {
		id, err := hex.DecodeString(oid)
		if err != nil || len(id) != 20 || sizes[oid] < 0 {
			t.Fatal("invalid fixture blob")
		}
		r := &storagev1.DirectBlobPart{Oid: id, Size: uint64(sizes[oid])}
		if r.Size > 0 {
			// This fixture supports metadata reads only. No blob bodies are
			// fabricated; accidental content reads fail on the absent pack.
			r.Chunk = &storagev1.ChunkRecord{Pack: "packs/size-only", Length: 1, Hash: strings.Repeat("0", 64)}
		}
		raw, err := proto.Marshal(&storagev1.DirectBlobPage{Items: []*storagev1.DirectBlobPart{r}})
		if err != nil {
			t.Fatal(err)
		}
		ref, err := w.saveBytes(raw)
		if err != nil {
			t.Fatal(err)
		}
		level = append(level, &storagev1.DirectBlobChild{MinOid: id, Page: encodePageRef(ref)})
	}
	if len(level) == 0 {
		t.Fatal("empty fixture blob catalog")
	}
	var routes []pageRef
	for {
		var next []*storagev1.DirectBlobChild
		for off := 0; off < len(level); off += fanout {
			children := level[off:min(off+fanout, len(level))]
			raw, err := proto.Marshal(&storagev1.DirectBlobPage{Children: children})
			if err != nil || len(raw) > directBlobPageBytes {
				t.Fatal("invalid fixture route", err)
			}
			ref, err := w.saveBytes(raw)
			if err != nil {
				t.Fatal(err)
			}
			routes = append(routes, ref)
			next = append(next, &storagev1.DirectBlobChild{MinOid: children[0].MinOid, Page: encodePageRef(ref)})
		}
		if len(next) == 1 {
			if err := w.flush(); err != nil {
				t.Fatal(err)
			}
			return decodePageRef(next[0].Page), routes
		}
		level = next
	}
}

func lazyFixtureMainRoot(t *testing.T, ctx context.Context, backend store.Store, prefix, oid string, size int, directory pageRef) pageRef {
	t.Helper()
	w := &indexWriter{ctx: ctx, store: backend, prefix: prefix}
	v, err := marshal(object{Kind: "tree", Size: int64(size), Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	edge, err := w.save(page{Items: []item{{Key: "o/" + oid, Value: v}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	return edge.ID
}

func lazyBuildReadFixture(t *testing.T, parent context.Context) *lazyReadFixture {
	t.Helper()
	if os.Getenv("GYIT_LAZY_TREE_READ_PROBE") != "1" {
		t.Fatal("real-source fixture requires explicit read qualifier gate")
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	dir := os.Getenv("GYIT_LAZY_TREE_FIXTURE_DATA")
	if dir == "" {
		t.Fatal("GYIT_LAZY_TREE_FIXTURE_DATA is required")
	}
	in := lazyFixtureInputs(t, dir)
	source := filepath.Clean(filepath.Join(dir, "../../../../../.testdata/linux-repo.git"))
	pack := filepath.Join(source, "objects/pack/pack-cf7a0650530097935c5e669c7fe6ecc98b2f9489")
	r, err := packrecipe.OpenDeferred(ctx, pack, source)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &lazyReadFixture{Backend: backend}
	sizes := make(map[string]int64)
	for _, c := range in {
		for _, e := range c.Entries {
			if e.Mode != 0040000 && e.Mode != 0160000 {
				sizes[e.OID] = e.Size
			}
		}
	}
	f.BlobRoot, f.BlobRoutes = lazyFixtureBlobCatalog(t, ctx, backend, sizes)
	for i, input := range in {
		c := input.lazyReadCase
		st := &stage{tmp: t.TempDir()}
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			t.Fatal(err)
		}
		w := &directoryWriter{stage: st, tmp: t.TempDir(), encoder: encoder, pages: &indexWriter{ctx: ctx, store: backend, prefix: fmt.Sprintf("dirs-lazy-fixture-%d", i)}}
		b := &directoryBuilder{writer: w}
		for _, e := range c.Entries {
			oid, err := hex.DecodeString(e.OID)
			if err != nil {
				t.Fatal(err)
			}
			if err := b.add(&storagev1.NamedEntry{Name: []byte(e.Name), Oid: oid, Mode: e.Mode, RawMode: e.RawMode, Size: e.Size}); err != nil {
				t.Fatal(err)
			}
		}
		compiled, err := b.finish()
		if err != nil {
			t.Fatal(err)
		}
		if err := w.pages.flush(); err != nil {
			t.Fatal(err)
		}
		encoder.Close()
		if err := st.close(); err != nil {
			t.Fatal(err)
		}
		c.CompiledRoot = lazyFixtureMainRoot(t, ctx, backend, fmt.Sprintf("lazy-fixture-compiled-%d", i), c.OID, len(input.raw), compiled)
		out, err := r.ConvertTree(c.OID, nil)
		if errors.Is(err, gitdelta.ErrLimit) {
			c.NativeRoot = c.CompiledRoot
			c.Admission = fmt.Sprintf("compiled fallback: native tree admission limit; raw_bytes=%d", len(input.raw))
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if out.Size != len(input.raw) || out.Hash != "git-tree-sha1:"+c.OID {
				t.Fatal("native converter target identity mismatch")
			}
			packs := &packWriter{ctx: ctx, store: backend, prefix: fmt.Sprintf("nativechain-tree-fixture-%d", i), stats: &Stats{}}
			var base *chunkBase
			payload := len(out.Data)
			if out.Base != nil {
				location, err := packs.add(out.Base.Packed, out.Base.Hash)
				if err != nil {
					t.Fatal(err)
				}
				v := chunkLocation(location)
				base, payload = &v, payload+len(out.Base.Packed)
			}
			location, err := packs.add(out.Data, out.Hash)
			if err != nil {
				t.Fatal(err)
			}
			location.Base = base
			if err := packs.flush(); err != nil {
				t.Fatal(err)
			}
			descriptor, err := marshal(location)
			if err != nil {
				t.Fatal(err)
			}
			nativePages := &indexWriter{ctx: ctx, store: backend, prefix: fmt.Sprintf("native-trees-fixture-%d", i)}
			native, err := nativePages.saveBytes(descriptor)
			if err != nil {
				t.Fatal(err)
			}
			if err := nativePages.flush(); err != nil {
				t.Fatal(err)
			}
			c.NativeRoot = lazyFixtureMainRoot(t, ctx, backend, fmt.Sprintf("lazy-fixture-native-%d", i), c.OID, len(input.raw), native)
			c.Native = true
			c.Admission = fmt.Sprintf("actual native converter; raw_bytes=%d payload_bytes=%d full_frame=%t", len(input.raw), payload, out.Full)
		}
		f.Cases = append(f.Cases, c)
		t.Logf("fixture path=%q entries=%d first_page_stat_anchors=%d %s", c.Path, len(c.Entries), len(c.StatNames), c.Admission)
	}
	return f
}

func TestLazyTreeFixtureMinimalCatalog(t *testing.T) {
	ctx := t.Context()
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first, second := strings.Repeat("1", 40), strings.Repeat("d", 40)
	root, routes := lazyFixtureBlobCatalog(t, ctx, backend, map[string]int64{first: 12345, second: 0})
	idx := &index{store: backend, cache: newCache(32 << 20), blobRoot: root}
	for _, ref := range routes {
		if _, err := idx.pageBytes(ctx, ref); err != nil {
			t.Fatal(err)
		}
	}
	s := &Snapshot{idx: idx}
	for id, want := range map[string]int64{first: 12345, second: 0} {
		got, _, err := s.readBlobPart(ctx, id, 0)
		if err != nil || got != want {
			t.Fatalf("size got=%d want=%d: %v", got, want, err)
		}
	}
	if len(routes) != 1 {
		t.Fatal("tiny fixture route shape changed")
	}
}

func TestLazyTreeFixtureSourceSetup(t *testing.T) {
	if os.Getenv("GYIT_LAZY_TREE_READ_PROBE") != "1" {
		t.Skip("opt-in fixed source fixture")
	}
	f := lazyBuildReadFixture(t, t.Context())
	for _, c := range f.Cases {
		for _, root := range []pageRef{c.CompiledRoot, c.NativeRoot} {
			s := &Snapshot{idx: &index{store: f.Backend, cache: newCache(32 << 20), root: root, blobRoot: f.BlobRoot}, Tree: c.OID}
			var all []Entry
			after := ""
			for {
				page, err := s.ReadDir(t.Context(), c.OID, after, fanout)
				if err != nil {
					t.Fatal(err)
				}
				if len(page) == 0 {
					break
				}
				all = append(all, page...)
				after = page[len(page)-1].Name
			}
			if fmt.Sprint(all) != fmt.Sprint(c.Entries) {
				t.Fatal("compiled/native entries differ from fixed Git oracle")
			}
		}
	}
}
