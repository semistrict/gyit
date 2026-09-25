package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"

	"gat/internal/store"
)

// A real Git source with enough blobs for direct import and a long, skewed
// version history that activates bounded speculative compression.
func speculativeImportFixture(t *testing.T, format string) string {
	t.Helper()
	source := blobPipelineFixture(t, format)
	body, err := git(t.Context(), source, "show", "HEAD:large").Output()
	if err != nil {
		t.Fatal(err)
	}
	head := command(t, source, "rev-parse", "HEAD")
	var input bytes.Buffer
	for revision := 3; revision < 35; revision++ {
		fmt.Fprintf(&input, "commit refs/heads/main\ncommitter Test <test@example.test> %d +0000\ndata 7\nfixture\n", 1700000000+revision)
		if revision == 3 {
			fmt.Fprintf(&input, "from %s\n", head)
		}
		body[revision*100] ^= 0x71
		fmt.Fprintf(&input, "M 100644 inline large\ndata %d\n", len(body))
		input.Write(body)
		input.WriteString("\n\n")
	}
	cmd := git(t.Context(), source, "fast-import", "--quiet")
	cmd.Stdin = &input
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fixture: %v %s", err, out)
	}
	return source
}

func TestSpeculativePublishedHistoryMatchesSerial(t *testing.T) {
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			source := speculativeImportFixture(t, format)
			var snapshots [2]*Snapshot
			var stores [2]store.Store
			for i, workers := range []int{1, 4} {
				local, err := store.NewLocal(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				stores[i] = local
				scratch := t.TempDir()
				stats, err := importWithMetadataThreshold(t.Context(), local, ImportOptions{Repo: source, TempDir: scratch, CompressionWorkers: workers}, 0)
				if err != nil {
					t.Fatal(err)
				}
				if stats.Blobs != 1136 || stats.DeltaChunks < 30 {
					t.Fatalf("fixture did not import expected delta history: %+v", stats)
				}
				r, err := New(local, 32<<20)
				if err != nil {
					t.Fatal(err)
				}
				snapshots[i], err = r.OpenRevision(t.Context(), "HEAD", "")
				if err != nil {
					t.Fatal(err)
				}
				files, err := os.ReadDir(scratch)
				if err != nil || len(files) != 0 {
					t.Fatalf("scratch remains: %v %v", files, err)
				}
			}
			var signature func(context.Context, store.Store, chunkBase) string
			signature = func(ctx context.Context, s store.Store, c chunkBase) string {
				data, _, err := s.Get(ctx, c.Pack, c.Offset, c.Length)
				if err != nil {
					t.Fatal(err)
				}
				result := fmt.Sprintf("%s:%d:%x", c.Hash, c.Length, sha256.Sum256(data))
				if c.Base != nil {
					result += "/" + signature(ctx, s, *c.Base)
				}
				return result
			}
			revisions := strings.Fields(command(t, source, "rev-list", "HEAD"))
			if len(revisions) != 35 {
				t.Fatal("fixture history length")
			}
			for _, revision := range revisions {
				oid := command(t, source, "rev-parse", revision+":large")
				want, err := git(t.Context(), source, "cat-file", "blob", oid).Output()
				if err != nil {
					t.Fatal(err)
				}
				for part, off := int64(0), 0; off < len(want); part, off = part+1, off+ChunkSize {
					var encoded [2]string
					n := min(ChunkSize, len(want)-off)
					for i, snapshot := range snapshots {
						got := make([]byte, n)
						read, err := snapshot.ReadAt(t.Context(), oid, got, int64(off))
						if err != nil || read != n || !bytes.Equal(got, want[off:off+n]) {
							t.Fatalf("native Git read mismatch %s build%d part%d: %v", revision, i, part, err)
						}
						var c chunk
						if err := snapshot.idx.get(t.Context(), chunkKey(oid, part), &c); err != nil {
							t.Fatal(err)
						}
						encoded[i] = signature(t.Context(), stores[i], chunkLocation(c))
					}
					if encoded[0] != encoded[1] {
						t.Fatalf("compressed payload/dependencies differ at %s part%d", revision, part)
					}
				}
			}
		})
	}
}
