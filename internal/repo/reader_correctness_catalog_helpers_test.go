package repo

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gyit/internal/store"
	"google.golang.org/protobuf/encoding/protowire"
)

func catalogTinyFacts(t *testing.T) (string, catalogFactsSpec, string) {
	t.Helper()
	// The independent facts intentionally arrive in a different order, with a
	// tag between ordinary identities. The tag must not enter the o/ catalog.
	raw := fmt.Sprintf("%040x tree 9\n%040x tag 13\n%040x blob 0\n%040x commit 17\n", 3, 2, 1, 4)
	name := filepath.Join(t.TempDir(), "facts.tsv")
	if err := os.WriteFile(name, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	spec := catalogFactsSpec{SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(raw))), Bytes: int64(len(raw)), Population: map[string]catalogPopulation{"blob": {1, 0}, "tree": {1, 9}, "commit": {1, 17}, "tag": {1, 13}}}
	want := fmt.Sprintf("%040x blob 0\n%040x tree 9\n%040x commit 17\n", 1, 3, 4)
	return name, spec, want
}

func catalogTinyObject(t *testing.T, id int, kind string, size int64) item {
	t.Helper()
	b, err := marshal(object{Kind: kind, Size: size})
	if err != nil {
		t.Fatal(err)
	}
	return item{Key: fmt.Sprintf("o/%040x", id), Value: b}
}
func catalogTinyIndex(t *testing.T, leaves [][]item, maxima []string, depth int) (*index, *store.Local) {
	t.Helper()
	ctx := t.Context()
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w := &indexWriter{ctx: ctx, store: backend, prefix: "catalog-verification-test"}
	var children []edge
	for i, items := range leaves {
		child, err := w.save(page{Items: items})
		if err != nil {
			t.Fatal(err)
		}
		if i < len(maxima) && maxima[i] != "" {
			child.Max = maxima[i]
		}
		children = append(children, child)
	}
	root := children[0]
	if len(children) > 1 {
		root, err = w.save(page{Children: children})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < depth; i++ {
		root, err = w.save(page{Children: []edge{root}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = w.flush(); err != nil {
		t.Fatal(err)
	}
	return &index{store: backend, root: root.ID}, backend
}

func TestCorrectnessCatalogExpectedSort(t *testing.T) {
	facts, spec, want := catalogTinyFacts(t)
	var report catalogReport
	path, err := catalogPrepareExpected(t.Context(), facts, t.TempDir(), spec, &report)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want || report.FactsSHA256 != spec.SHA256 {
		t.Fatalf("expected canonical independent sort: %q %+v", got, report)
	}
	t.Run("wrong checksum rejected", func(t *testing.T) {
		bad := spec
		bad.SHA256 = strings.Repeat("0", 64)
		if _, err := catalogPrepareExpected(t.Context(), facts, t.TempDir(), bad, &catalogReport{}); err == nil || !strings.Contains(err.Error(), "SHA256 mismatch") {
			t.Fatalf("wrong source checksum accepted: %v", err)
		}
	})
	t.Run("wrong census rejected", func(t *testing.T) {
		bad := spec
		bad.Population = map[string]catalogPopulation{"blob": {99, 0}}
		if _, err := catalogPrepareExpected(t.Context(), facts, t.TempDir(), bad, &catalogReport{}); err == nil || !strings.Contains(err.Error(), "population mismatch") {
			t.Fatalf("wrong expected census accepted: %v", err)
		}
	})
}

func TestCorrectnessCatalogCompleteAndStructuralErrors(t *testing.T) {
	_, spec, want := catalogTinyFacts(t)
	rows := []item{catalogTinyObject(t, 1, "blob", 0), catalogTinyObject(t, 3, "tree", 9), catalogTinyObject(t, 4, "commit", 17)}
	for _, tc := range []struct {
		name      string
		leaves    [][]item
		maxima    []string
		depth     int
		expected  string
		wantError string
	}{
		{name: "complete including unrelated namespace", leaves: [][]item{{{Key: "c/000", Value: []byte{1}}}, rows[:2], rows[2:]}, expected: want},
		{name: "duplicate across leaf boundary", leaves: [][]item{rows[:2], rows[1:]}, expected: want, wantError: "strictly ordered"},
		{name: "missing middle identity", leaves: [][]item{{rows[0], rows[2]}}, expected: want, wantError: "identity mismatch"},
		{name: "missing final identity", leaves: [][]item{rows[:2]}, expected: want, wantError: "missing stored object"},
		{name: "extra final identity", leaves: [][]item{append(append([]item{}, rows...), catalogTinyObject(t, 5, "blob", 4))}, expected: want, wantError: "after expected EOF"},
		{name: "wrong kind", leaves: [][]item{{rows[0], catalogTinyObject(t, 3, "blob", 9), rows[2]}}, expected: want, wantError: "expected tree 9"},
		{name: "wrong size", leaves: [][]item{{rows[0], catalogTinyObject(t, 3, "tree", 8), rows[2]}}, expected: want, wantError: "expected tree 9"},
		{name: "false routing bound", leaves: [][]item{rows[:1], rows[1:]}, maxima: []string{fmt.Sprintf("o/%040x", 2)}, expected: want, wantError: "differs from child maximum"},
		{name: "depth bounded", leaves: [][]item{rows}, depth: 16, expected: want, wantError: "bounded depth"},
		{name: "duplicate expected identity", leaves: [][]item{rows}, expected: strings.Replace(want, fmt.Sprintf("%040x tree", 3), fmt.Sprintf("%040x tree", 1), 1), wantError: "expected identities are not strictly ordered"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idx, _ := catalogTinyIndex(t, tc.leaves, tc.maxima, tc.depth)
			path := filepath.Join(t.TempDir(), "expected.sorted")
			if err := os.WriteFile(path, []byte(tc.expected), 0600); err != nil {
				t.Fatal(err)
			}
			report := catalogReport{Expected: spec.Population, Compared: make(map[string]catalogPopulation)}
			err := catalogCompare(t.Context(), idx, path, &report)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("expected %q, got %v", tc.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if report.ObjectRows != 3 || report.Rows != 4 || report.Pages != 4 || report.MaxDepth != 2 {
				t.Fatalf("incomplete structural walk: %+v", report)
			}
		})
	}
}

func TestCorrectnessCatalogBoundsAndCancellation(t *testing.T) {
	for _, bad := range [][]byte{nil, {0x0a, 0}, append(protowire.AppendBytes(protowire.AppendTag(nil, 1, 2), nil), protowire.AppendBytes(protowire.AppendTag(nil, 2, 2), nil)...)} {
		// A single empty IndexItem reaches the key validation stage; only the
		// top-level empty and mixed shapes are expected to fail preflight.
		if len(bad) == 2 {
			continue
		}
		if err := catalogPageShape(bad); err == nil {
			t.Fatalf("invalid page shape accepted: %x", bad)
		}
	}
	var bomb []byte
	for i := 0; i < 129; i++ {
		bomb = protowire.AppendBytes(protowire.AppendTag(bomb, 1, 2), nil)
	}
	if err := catalogPageShape(bomb); err == nil {
		t.Fatal("unbounded protobuf allocation count accepted")
	}
	idx, _ := catalogTinyIndex(t, [][]item{{catalogTinyObject(t, 1, "blob", 0)}}, nil, 0)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := catalogWalk(ctx, idx, &catalogReport{}, func(string, []byte) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled walk: %v", err)
	}
	bad := idx.root
	bad.Hash = strings.Repeat("0", 64)
	if _, err := catalogLoadPage(t.Context(), idx.store, bad); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("unauthenticated page accepted: %v", err)
	}
	if err := catalogWalk(t.Context(), idx, &catalogReport{}, func(string, []byte) error { return fmt.Errorf("consumer stopped") }); err == nil || !strings.Contains(err.Error(), "consumer stopped") {
		t.Fatalf("consumer failure lost: %v", err)
	}
}

func TestCorrectnessCatalogReadOnlyLifecycle(t *testing.T) {
	facts, spec, _ := catalogTinyFacts(t)
	idx, backend := catalogTinyIndex(t, [][]item{{catalogTinyObject(t, 1, "blob", 0), catalogTinyObject(t, 3, "tree", 9), catalogTinyObject(t, 4, "commit", 17)}}, nil, 0)
	b, err := marshal(manifest{Version: formatVersion, Format: "sha1", Root: idx.root})
	if err != nil {
		t.Fatal(err)
	}
	if err = backend.Put(t.Context(), "HEAD", b, "*"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []bool{false, true} {
		t.Run(fmt.Sprintf("invalid facts %t", bad), func(t *testing.T) {
			want := spec
			if bad {
				want.SHA256 = strings.Repeat("0", 64)
			}
			report, err := catalogVerify(t.Context(), backend, facts, t.TempDir(), want)
			if (err != nil) != bad {
				t.Fatalf("verification result: %v %+v", err, report)
			}
			if !report.CleanupVerified || !report.HeadUnchanged || report.Completed == bad {
				t.Fatalf("lifecycle checks not completed: %+v", report)
			}
			if _, err = os.Stat(report.TempPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("scratch directory retained: %v", err)
			}
		})
	}
}
