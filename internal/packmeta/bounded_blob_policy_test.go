package planner

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"
	"testing"

	wire "gat/internal/archive/wire"
)

func TestBoundedBlobProviderAdmissionAndReadback(t *testing.T) {
	for _, tc := range []struct {
		name         string
		root, target int
	}{
		{"small-target", 1024, 2048},
		{"large-root", 8192, 4096},
		{"both", 8192, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := bytes.Repeat([]byte{'a'}, tc.root)
			target := bytes.Repeat([]byte{'b'}, tc.target)
			prefix, ids := fixture(t, []fixtureEntry{
				{kind: 3, raw: root},
				{kind: 6, raw: literalDelta(len(root), target), target: target, baseIndex: 0},
			})
			metadataByOID := map[[20]byte]int64{}
			for i, body := range [][]byte{root, target} {
				var id [20]byte
				copy(id[:], ids[i])
				metadataByOID[id] = int64(len(body))
			}
			_ = metadataByOID
			p, err := OpenSource(t.Context(), prefix, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := p.Close(); err != nil {
					t.Error(err)
				}
			})
			recipe, err := p.Recipe(hex.EncodeToString(ids[1]), [16]byte{1})
			if err != nil {
				t.Fatal("bounded native chain should be admitted", err)
			}
			if len(recipe.Frames) != 2 || recipe.Frames[0].Size != uint32(len(root)) || recipe.Frames[1].Size != uint32(len(target)) {
				t.Fatal("fixture failed to exercise exact native chain", recipe)
			}
			ranges, err := wire.Plan(recipe, -1)
			if err != nil || len(ranges) > 4 {
				t.Fatal("blob hard range bound", ranges, err)
			}
			packed, err := os.ReadFile(prefix + ".pack")
			if err != nil {
				t.Fatal(err)
			}
			raw, metrics, err := wire.ReadObject(t.Context(), "blob", recipe, recipe.TargetOID, func(_ context.Context, segment uint64, offset, length uint32) ([]byte, error) {
				start := segment*wire.SegmentSize + uint64(offset)
				return packed[start : start+uint64(length)], nil
			}, nil, nil)
			if err != nil || !bytes.Equal(raw, target) {
				t.Fatal("authenticated target mismatch", err)
			}
			if metrics.Fetches > 4 || metrics.FetchedBytes > wire.PackedLimit || metrics.DecodedWork > wire.WorkLimit {
				t.Fatal("hard read bounds", metrics)
			}
			s := p.Stats()
			if s.RecipeAccepted != 1 || s.FullInflations != 0 || s.Reconstructions != 0 || s.PayloadCopiedBytes != 0 {
				t.Fatal("planner work changed", s)
			}
		})
	}
}
