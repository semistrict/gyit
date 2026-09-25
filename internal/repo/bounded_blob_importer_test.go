package repo

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	wire "gat/internal/archive/wire"
	"gat/internal/store"
)

// Both delta choices are explicit in the synthetic native pack, independent of
// Git's packing heuristics. The empty blob must remain an identity with no data.
func boundedBlobImportFixture(t *testing.T) (source, tip, prefix string, bodies map[string][]byte) {
	t.Helper()
	bodies = map[string][]byte{
		"base":  bytes.Repeat([]byte{'a'}, 8192),
		"ratio": bytes.Repeat([]byte{'b'}, 4096),
		"small": []byte("tinydata"),
		"empty": nil,
	}
	var tree bytes.Buffer
	for _, name := range []string{"base", "empty", "ratio", "small"} {
		fmt.Fprintf(&tree, "100644 %s\x00", name)
		tree.Write(boundedBlobObjectID("blob", bodies[name]))
	}
	commit := []byte(fmt.Sprintf("tree %x\nauthor Test <test@example.test> 1000000000 +0000\ncommitter Test <test@example.test> 1000000000 +0000\n\nbounded blob fixture\n", boundedBlobObjectID("tree", tree.Bytes())))
	entries := []boundedBlobFixtureEntry{
		{kind: 3, raw: bodies["base"]},
		{kind: 6, raw: boundedBlobLiteralDelta(len(bodies["base"]), bodies["ratio"]), target: bodies["ratio"], baseIndex: 0},
		{kind: 6, raw: boundedBlobLiteralDelta(len(bodies["base"]), bodies["small"]), target: bodies["small"], baseIndex: 0},
		{kind: 3, raw: nil},
		{kind: 2, raw: tree.Bytes()},
		{kind: 1, raw: commit},
	}
	prefix, ids := boundedBlobFixture(t, entries)
	source = filepath.Dir(filepath.Dir(filepath.Dir(prefix)))
	tip = hex.EncodeToString(ids[len(ids)-1])
	archiveImportInput(t, source, "", "update-ref", "HEAD", tip)
	return
}

func TestBoundedBlobImportPolicyAndEmptyReadback(t *testing.T) {
	for _, useArchive := range []bool{true, false} {
		t.Run(fmt.Sprintf("metadata-%t", useArchive), func(t *testing.T) {
			packMetadataTestEnv(t)
			source, tip, prefix, bodies := boundedBlobImportFixture(t)
			archiveImportEnv(t, prefix)
			capture := deferredCapture(t)
			backend, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			scratch := t.TempDir()
			opt := ImportOptions{Repo: source, TempDir: scratch, CompressionWorkers: 3}
			if !useArchive {
				opt.DeltaDepth = 1
			}
			stats, err := Import(t.Context(), backend, opt)
			if err != nil {
				t.Fatal(err)
			}
			counts := capture()
			if stats.Objects != 6 || stats.Blobs != 4 {
				t.Fatalf("object population: %+v", stats)
			}
			wantVersion := legacyFormatVersion
			if useArchive {
				wantVersion = formatVersion
				if stats.ImportMode != "archive" || counts["archive_blob_admitted"] != 3 || counts["archive_blob_raw_bytes"] != 8192+4096+8 || counts["archive_blob_limits"] != 0 {
					t.Fatalf("nonempty native admission and empty bypass: stats=%+v counts=%+v", stats, counts)
				}
				if counts["pack_metadata_objects"] != 6 || counts["archive_plan_reused"] != 1 {
					t.Fatal("native provider not exercised", counts)
				}
			} else if stats.ImportMode != "reachable" || counts["archive_blob_admitted"] != 0 {
				t.Fatal("explicit chunk conversion not exercised", stats, counts)
			}
			m, _, err := readHead(t.Context(), backend)
			if err != nil || m.Version != wantVersion || m.HistoryCount != 1 {
				t.Fatal("manifest compatibility", m, err)
			}
			// Removing the source before constructing a new reader proves that all
			// native bodies are read from immutable object-store segments.
			if err = os.Rename(source, filepath.Join(t.TempDir(), "retired-source")); err != nil {
				t.Fatal(err)
			}
			r, err := New(backend, DefaultCacheBytes)
			if err != nil {
				t.Fatal(err)
			}
			s, err := r.Open(t.Context(), tip)
			if err != nil {
				t.Fatal(err)
			}
			page, err := s.ReadDir(t.Context(), s.Tree, "", 128)
			if err != nil || len(page) != len(bodies) {
				t.Fatal("complete root directory", page, err)
			}
			for name, want := range bodies {
				e, err := s.Resolve(t.Context(), name)
				if err != nil || e.Size != int64(len(want)) {
					t.Fatal("file size", name, e, err)
				}
				size, c, err := s.readBlobPart(t.Context(), e.OID, 0)
				if err != nil || size != int64(len(want)) {
					t.Fatal("catalog size", name, size, err)
				}
				if len(want) == 0 {
					if c != (chunk{}) {
						t.Fatal("empty blob must not own a content descriptor", c)
					}
				} else if useArchive {
					if c.ArchiveRecipe == "" {
						t.Fatal("native policy admission fell back", name)
					}
					recipeHash := sha256.Sum256([]byte(c.ArchiveRecipe))
					recipe, err := wire.DecodeRecipe([]byte(c.ArchiveRecipe), hex.EncodeToString(recipeHash[:]))
					if err != nil {
						t.Fatal(err)
					}
					if name == "ratio" || name == "small" {
						if len(recipe.Frames) != 2 || recipe.Frames[0].Size != 8192 {
							t.Fatal("original larger root was not preserved", name, recipe)
						}
					}
				}
				if !useArchive && c.ArchiveRecipe != "" {
					t.Fatal("conversion retained an archive recipe")
				}
				got := make([]byte, len(want)+1)
				n, err := s.ReadAt(t.Context(), e.OID, got, 0)
				if err != io.EOF || n != len(want) || !bytes.Equal(got[:n], want) {
					t.Fatal("immutable exact readback", name, n, err)
				}
			}
			if files, err := os.ReadDir(scratch); err != nil || len(files) != 0 {
				t.Fatal("import scratch leak", files, err)
			}
		})
	}
}
