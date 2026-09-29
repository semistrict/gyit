//go:build !js

package repo

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
)

func storeHistoryRecipes(t *testing.T, p *Progressive, recipes map[string]*pb.ProgressiveObject) {
	t.Helper()
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p.store = backend
	w := &indexWriter{ctx: t.Context(), store: backend, prefix: "origin-test"}
	var items []item
	for oid, recipe := range recipes {
		value, err := marshal(recipe)
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, item{Key: "g/" + oid, Value: value})
	}
	// These tests deliberately publish one recipe at a time.
	if len(items) != 1 {
		t.Fatal("expected one recipe")
	}
	e, err := w.save(page{Items: items})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	p.root = e.ID
}

// A remote recipe does not inherit the trust of bytes that happen to exist in
// the local acquisition. Repeat after offset caching to check that distinction.
func TestHistoryStoredRecipeChecksIdentity(t *testing.T) {
	p, ctx, oid, want, _ := historyRecipeFixture(t)
	source := ctx.Value(historySourceKey{}).(*historySource)
	source.indexedByGit = true
	source.cache = newCache(1 << 20)
	recipe, _, err := source.locate(oid)
	if err != nil {
		t.Fatal(err)
	}
	recipe.Size = int64(len(want))
	wrongOID := strings.Repeat("f", 40)
	storeHistoryRecipes(t, p, map[string]*pb.ProgressiveObject{wrongOID: recipe})
	for range 3 {
		_, release, err := p.borrowObject(ctx, wrongOID)
		release()
		if err == nil || !strings.Contains(err.Error(), "object identity mismatch") {
			t.Fatalf("accepted incorrect stored identity: %v", err)
		}
	}
	// The local, correctly indexed name must still resolve to the original bytes.
	got, release, err := p.borrowObject(ctx, oid)
	defer release()
	if err != nil || !bytes.Equal(got[1:], want) {
		t.Fatalf("valid local object: %v", err)
	}
}

// Removing the base from the acquisition index forces REF_DELTA lookup through
// Store. Test both locally available base bytes and a remotely fetched base.
func TestHistoryDeltaOriginSurvivesCache(t *testing.T) {
	for _, remoteBytes := range []bool{false, true} {
		t.Run(fmt.Sprintf("remote-bytes=%v", remoteBytes), func(t *testing.T) {
			p, ctx, oid, want, _ := historyRecipeFixture(t)
			source := ctx.Value(historySourceKey{}).(*historySource)
			source.indexedByGit = true
			source.cache = newCache(1 << 20)
			recipe, _, err := source.locate(oid)
			if err != nil {
				t.Fatal(err)
			}
			pack := &source.packs[0]
			r := bytes.NewReader(pack.data[recipe.Offset:])
			_, _, _, baseID, err := progressiveHeader(r, recipe.Offset)
			if err != nil {
				t.Fatal(err)
			}
			baseRecipe, _, err := source.locate(baseID)
			if err != nil {
				t.Fatal(err)
			}
			baseRecipe.Size = int64(len(want) - 1)
			// Leave the valid delta object in the local index, but not its base.
			index := make([]byte, 1072+28)
			id, _ := hex.DecodeString(oid)
			copy(index[1032:], id)
			binary.BigEndian.PutUint32(index[1032+24:], uint32(recipe.Offset))
			pack.index, pack.count = index, 1
			if remoteBytes {
				changed := bytes.Clone(want[:len(want)-1])
				changed[0] ^= 1
				baseCtx, remote := packedDecodeFixture(t, changed, int64(len(changed)), "")
				remote.Pack = strings.Repeat("b", 40)
				storeHistoryRecipes(t, p, map[string]*pb.ProgressiveObject{baseID: remote})
				raw := baseCtx.Value(historySourceKey{}).(*historySource).packs[0].data
				if err := p.store.Put(ctx, progressivePackKey(remote.Pack, 0), raw, ""); err != nil {
					t.Fatal(err)
				}
			} else {
				storeHistoryRecipes(t, p, map[string]*pb.ProgressiveObject{baseID: baseRecipe})
			}
			for range 3 {
				budget := int64(256 << 20)
				_, localOnly, err := p.decodeObject(ctx, recipe, 0, &budget)
				if err != nil || localOnly {
					t.Fatalf("stored delta base treated as local: local=%v err=%v", localOnly, err)
				}
				got, release, err := p.borrowObject(ctx, oid)
				release()
				if remoteBytes {
					if err == nil || !strings.Contains(err.Error(), "object identity mismatch") {
						t.Fatalf("accepted delta reconstructed from wrong remote base: %v", err)
					}
				} else if err != nil || !bytes.Equal(got[1:], want) {
					t.Fatalf("valid mixed delta: %v", err)
				}
			}
		})
	}
}
