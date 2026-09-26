package planner

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"testing"

	wire "gyit/internal/archive/wire"
)

func TestTreeRangeProviderAdmissionAndReadback(t *testing.T) {
	for _, kind := range []byte{2, 3} {
		for _, depth := range []int{4, 5, 16, 17} {
			t.Run(fmt.Sprintf("kind%d/ranges%d", kind, depth), func(t *testing.T) {
				var entries []fixtureEntry
				var targets [][]byte
				prior := -1
				metadataByOID := map[[20]byte]struct {
					kind byte
					size int64
				}{}
				// Uniform-length, valid tree entry bytes also serve as blob
				// bodies. Their >4KiB size avoids unrelated blob policy limits.
				for version := 0; version < depth; version++ {
					var body bytes.Buffer
					for entry := 0; entry < 128; entry++ {
						fmt.Fprintf(&body, "100644 v%02d-f%04d\x00", version, entry)
						body.Write(bytes.Repeat([]byte{1}, 20))
					}
					raw := body.Bytes()
					kindName := "tree"
					if kind == 3 {
						kindName = "blob"
					}
					id := objectID(kindName, raw)
					e := fixtureEntry{kind: kind, raw: raw, id: id}
					if prior >= 0 {
						e = fixtureEntry{kind: 6, raw: literalDelta(len(targets[len(targets)-1]), raw), target: raw, baseIndex: prior, id: id}
					}
					prior = len(entries)
					entries = append(entries, e)
					targets = append(targets, raw)
					var key [20]byte
					copy(key[:], id)
					metadataByOID[key] = struct {
						kind byte
						size int64
					}{kind, int64(len(raw))}
					if version+1 < depth {
						gap := []byte(fmt.Sprintf("unrelated gap %02d", version))
						gapID := objectID("blob", gap)
						entries = append(entries, fixtureEntry{kind: 3, raw: gap, id: gapID})
						copy(key[:], gapID)
						metadataByOID[key] = struct {
							kind byte
							size int64
						}{3, int64(len(gap))}
					}
				}
				prefix, ids := fixture(t, entries)
				_ = metadataByOID // OpenSource derives its own metadata.
				p, err := Open(t.Context(), prefix, func(id [20]byte) (byte, int64, bool, error) {
					value, ok := metadataByOID[id]
					return value.kind, value.size, ok, nil
				}, t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := p.Close(); err != nil {
						t.Error(err)
					}
				})
				recipe, err := p.Recipe(hex.EncodeToString(ids[prior]), [16]byte{1})
				admitted := depth <= 4 || kind == 2 && depth <= 16
				if !admitted {
					if !errors.Is(err, ErrLimit) {
						t.Fatalf("out-of-policy recipe admitted: %v", err)
					}
					if p.Stats().RecipeAccepted != 0 || p.Stats().RecipeLimitFallbacks != 1 {
						t.Fatal("fallback counters", p.Stats())
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				ranges, err := wire.Plan(recipe, -1)
				if err != nil || len(ranges) != depth {
					t.Fatalf("physical ranges %d want%d: %v", len(ranges), depth, err)
				}
				packed, err := os.ReadFile(prefix + ".pack")
				if err != nil {
					t.Fatal(err)
				}
				kindName := "tree"
				if kind == 3 {
					kindName = "blob"
				}
				raw, metrics, err := wire.ReadObject(t.Context(), kindName, recipe, recipe.TargetOID, func(_ context.Context, segment uint64, offset, length uint32) ([]byte, error) {
					start := segment*wire.SegmentSize + uint64(offset)
					return packed[start : start+uint64(length)], nil
				}, nil, nil)
				if err != nil || !bytes.Equal(raw, targets[len(targets)-1]) {
					t.Fatalf("authenticated %s bytes differ: %v", kindName, err)
				}
				if metrics.Fetches != depth || metrics.FetchedBytes > wire.PackedLimit || metrics.DecodedWork > wire.WorkLimit {
					t.Fatalf("read bounds %+v", metrics)
				}
				if p.Stats().RecipeAccepted != 1 || p.Stats().RecipeLimitFallbacks != 0 {
					t.Fatal("admission counters", p.Stats())
				}
			})
		}
	}
}
