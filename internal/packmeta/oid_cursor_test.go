package planner

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

func cursorFixture(t *testing.T) (string, [][]byte, []fixtureEntry) {
	t.Helper()
	base := bytes.Repeat([]byte("base"), 1024)
	target := append([]byte(nil), base...)
	target[0] = 'B'
	baseID := objectID("blob", base)
	tree := append([]byte("100644 file\x00"), baseID...)
	commit := []byte(fmt.Sprintf("tree %x\nauthor Test <test@example.test> 1 +0000\ncommitter Test <test@example.test> 1 +0000\n\nlocal\n", objectID("tree", tree)))
	tag := []byte(fmt.Sprintf("object %x\ntype commit\ntag local\ntagger Test <test@example.test> 1 +0000\n\nlocal tag\n", objectID("commit", commit)))
	// No refs are created. All rows, including a forward REF delta, must appear
	// regardless of reachability, kind, and physical placement.
	entries := []fixtureEntry{
		{kind: 7, raw: literalDelta(len(base), target), target: target, baseID: baseID},
		{kind: 1, raw: commit},
		{kind: 3, raw: nil},
		{kind: 4, raw: tag},
		{kind: 2, raw: tree},
		{kind: 3, raw: base},
	}
	prefix, ids := fixture(t, entries)
	return prefix, ids, entries
}

func TestOIDCursorSortedOwnedAndRecipeInterleave(t *testing.T) {
	prefix, ids, entries := cursorFixture(t)
	p := openSourceFixture(t, prefix)
	cursor, err := p.NewOIDCursor(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[[20]byte]SourceObject, len(ids))
	order := append([][]byte(nil), ids...)
	sort.Slice(order, func(i, j int) bool { return bytes.Compare(order[i], order[j]) < 0 })
	physicalDiffers := false
	for n, id := range ids {
		kind, size := entries[n].kind, len(entries[n].raw)
		if kind == 7 {
			kind, size = 3, len(entries[n].target)
		}
		want[key(id)] = SourceObject{OID: key(id), Kind: kind, Size: int64(size)}
		physicalDiffers = physicalDiffers || !bytes.Equal(id, order[n])
	}
	if !physicalDiffers {
		t.Fatal("fixture must distinguish OID order from pack order")
	}
	for ordinal, id := range order {
		row, ok, err := cursor.Next()
		expected := want[key(id)]
		expected.Ordinal = uint32(ordinal)
		if err != nil || !ok || row != expected {
			t.Fatalf("row %d: %+v %t %v want %+v", ordinal, row, ok, err, expected)
		}
		kind, size, found, err := p.Lookup(row.OID)
		if err != nil || !found || kind != row.Kind || size != row.Size {
			t.Fatal("cursor/Lookup mismatch", row, kind, size, found, err)
		}
		if row.Kind == 2 || row.Kind == 3 && row.Size > 0 {
			recipe, err := p.Recipe(hex.EncodeToString(row.OID[:]), [16]byte{})
			if err != nil || recipe.TargetOID != row.OID {
				t.Fatal("Recipe must be safe between cursor steps", err)
			}
		}
		// Mutation of an owned result cannot change the source index or next row.
		row.OID[0] ^= 0xff
		if !p.Has(hex.EncodeToString(id)) {
			t.Fatal("returned OID aliases source mapping")
		}
	}
	for range 2 {
		if row, ok, err := cursor.Next(); row != (SourceObject{}) || ok || err != nil {
			t.Fatal("expected stable EOF", row, ok, err)
		}
	}
	if _, _, found, err := p.Lookup([20]byte{}); err != nil || found {
		t.Fatal("cursor changed source membership", found, err)
	}
}

func TestOIDCursorCancellationCloseAndEmpty(t *testing.T) {
	prefix, _, _ := cursorFixture(t)
	p := openSourceFixture(t, prefix)
	ctx, cancel := context.WithCancel(t.Context())
	cursor, err := p.NewOIDCursor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := cursor.Next(); err != nil || !ok {
		t.Fatal(ok, err)
	}
	cancel()
	if row, ok, err := cursor.Next(); row != (SourceObject{}) || ok || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled cursor returned data", row, ok, err)
	}
	if _, err := p.NewOIDCursor(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled cursor construction", err)
	}
	live, err := p.NewOIDCursor(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := live.Next(); err != nil || !ok {
		t.Fatal(ok, err)
	}
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("idle cursor retained a planner lifecycle lock")
	}
	if row, ok, err := live.Next(); row != (SourceObject{}) || ok || !errors.Is(err, ErrClosed) {
		t.Fatal("closed source returned data", row, ok, err)
	}
	if _, err := p.NewOIDCursor(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatal("closed cursor construction", err)
	}
	if files, err := os.ReadDir(p.scratchDir); !os.IsNotExist(err) {
		t.Fatal("owned source scratch retained", files, err)
	}

	emptyPrefix, _ := fixture(t, nil)
	empty := openSourceFixture(t, emptyPrefix)
	emptyCursor, err := empty.NewOIDCursor(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if row, ok, err := emptyCursor.Next(); row != (SourceObject{}) || ok || err != nil {
		t.Fatal("empty source", row, ok, err)
	}
	sourceCtx, stopSource := context.WithCancel(t.Context())
	source, err := OpenSource(sourceCtx, emptyPrefix, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close() })
	sourceCursor, err := source.NewOIDCursor(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	stopSource()
	if _, ok, err := sourceCursor.Next(); ok || !errors.Is(err, context.Canceled) {
		t.Fatal("source cancellation ignored after empty EOF", ok, err)
	}
}

func TestOIDCursorConcurrentExactOnce(t *testing.T) {
	prefix, ids, _ := cursorFixture(t)
	p := openSourceFixture(t, prefix)
	cursor, err := p.NewOIDCursor(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	rows := make(chan SourceObject, len(ids))
	var group sync.WaitGroup
	for range 4 {
		group.Go(func() {
			for {
				row, ok, err := cursor.Next()
				if err != nil {
					t.Error(err)
					return
				}
				if !ok {
					return
				}
				rows <- row
			}
		})
	}
	group.Wait()
	close(rows)
	seen := map[uint32]bool{}
	for row := range rows {
		if seen[row.Ordinal] {
			t.Fatal("duplicate cursor ordinal", row.Ordinal)
		}
		seen[row.Ordinal] = true
	}
	if len(seen) != len(ids) {
		t.Fatal("missing cursor rows", len(seen), len(ids))
	}
}

func TestOIDCursorRejectsMalformedSourceBeforeIteration(t *testing.T) {
	prefix, _, _ := cursorFixture(t)
	idx, err := os.ReadFile(prefix + ".idx")
	if err != nil {
		t.Fatal(err)
	}
	copy(idx[1032+20:1032+40], idx[1032:1032+20])
	if err = os.WriteFile(prefix+".idx", idx, 0600); err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()
	p, err := OpenSource(t.Context(), prefix, scratch)
	if p != nil || !errors.Is(err, ErrUnsupported) || !errors.Is(err, ErrMalformed) {
		t.Fatal("duplicate OID index must not produce a cursor source", p, err)
	}
	if files, err := os.ReadDir(scratch); err != nil || len(files) != 0 {
		t.Fatal("malformed-source scratch retained", files, err)
	}
}
