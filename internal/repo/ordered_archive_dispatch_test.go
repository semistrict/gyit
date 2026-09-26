package repo

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	storagev1 "gyit/internal/gen/gyit/storage/v1"
	metaplan "gyit/internal/packmeta"
	"gyit/internal/store"
	"google.golang.org/protobuf/proto"
)

func orderedArchiveMixedFixture(t *testing.T) (string, string, string) {
	t.Helper()
	source, tip, pack, orphan := archiveImportFixture(t)
	// This fixture's one intentionally loose unreachable blob belongs to the
	// earlier fallback test, not the qualified single-pack input used here.
	if err := os.RemoveAll(filepath.Join(source, ".git", "objects", orphan[:2])); err != nil {
		t.Fatal(err)
	}
	return source, tip, pack
}

func orderedArchiveTestInfo(t *testing.T, ctx context.Context, pack, tmp string) *sourceInfo {
	t.Helper()
	p, err := metaplan.OpenSource(ctx, pack, tmp)
	if err != nil {
		t.Fatal(err)
	}
	copyCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	close(done)
	a := &sourceArchiveImport{ctx: copyCtx, cancel: cancel, done: done, prefix: pack, planner: metadataRecipeAdapter{p}}
	return &sourceInfo{metadata: p, archive: a}
}

func TestOrderedArchiveDispatchMixedPartitionAndOnceOnlyAdmission(t *testing.T) {
	source, _, pack := orderedArchiveMixedFixture(t)
	allLocalTestEnv(t, pack)
	tmp := t.TempDir()
	info := orderedArchiveTestInfo(t, t.Context(), pack, tmp)
	t.Cleanup(func() { _ = info.close() })
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stream, err := newOrderedArchiveInventory(t.Context(), tmp, info, backend)
	if err != nil {
		t.Fatal(err)
	}
	fallback, err := io.ReadAll(stream)
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}
	d := info.ordered
	var stats Stats
	if err = d.finish(&stats); err != nil {
		t.Fatal(err)
	}
	first := stats
	if err = d.finish(&stats); err != nil || stats != first {
		t.Fatal("dispatch counted twice", stats, first, err)
	}
	seen := map[string]string{}
	for _, row := range strings.Split(strings.TrimSpace(string(fallback)), "\n") {
		f := strings.Fields(row)
		if len(f) != 3 {
			t.Fatal("fallback row", row)
		}
		if seen[f[0]] != "" {
			t.Fatal("duplicate fallback", f[0])
		}
		seen[f[0]] = f[1] + " fallback"
	}
	var expectedCalls, treeFallbacks, emptyBypass, largeBypass, commits int64
	if err = info.metadata.ForEachObject(t.Context(), func(id [20]byte, kind byte, size int64) error {
		oid := hex.EncodeToString(id[:])
		if kind == 1 {
			commits++
			if seen[oid] != "commit fallback" {
				t.Fatalf("commit missed existing worker route %s", oid)
			}
		}
		if kind == 2 {
			expectedCalls++
			if size > 64<<10 {
				treeFallbacks++
				if seen[oid] != "tree fallback" {
					t.Fatal("wide tree did not fall back")
				}
			}
		}
		if kind == 3 {
			if size == 0 {
				emptyBypass++
				if seen[oid] != "blob fallback" {
					t.Fatal("empty blob did not bypass")
				}
			} else if size > ChunkSize {
				largeBypass++
				if seen[oid] != "blob fallback" {
					t.Fatal("large blob did not bypass")
				}
			} else {
				expectedCalls++
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if treeFallbacks != 1 || emptyBypass != 1 || largeBypass == 0 || commits != 7 {
		t.Fatal("mixed fixture coverage", treeFallbacks, emptyBypass, largeBypass, commits)
	}
	var mainRows, blobRows int64
	for {
		key, value, e := d.Main.Next(t.Context())
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		oid := strings.TrimPrefix(string(key), "o/")
		if len(oid) != 40 || seen[oid] != "" {
			t.Fatal("duplicate or malformed main identity", string(key), seen[oid])
		}
		var o object
		if e = unmarshal(value, &o); e != nil {
			t.Fatal(e)
		}
		seen[oid] = o.Kind + " archived"
		mainRows++
		want := archiveImportInput(t, source, oid+"\n", "cat-file", "--batch-check=%(objecttype) %(objectsize)")
		if want != fmt.Sprintf("%s %d", o.Kind, o.Size) {
			t.Fatal("source identity differs", oid, o, want)
		}
		if o.Kind == "tree" && (!isArchiveTree(o.Directory) || o.Directory.Length <= 0) {
			t.Fatal("tree representation changed", o)
		}
	}
	for {
		key, value, e := d.Blobs.Next(t.Context())
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		var p storagev1.DirectBlobPart
		if e = proto.Unmarshal(value, &p); e != nil {
			t.Fatal(e)
		}
		if len(key) != 28 || !bytes.Equal(key[:20], p.Oid) || binary.BigEndian.Uint64(key[20:]) != 0 || p.Part != 0 || p.Size == 0 || p.Chunk == nil || len(p.Chunk.ArchiveRecipe) == 0 {
			t.Fatal("joined direct part shape", p.String())
		}
		oid := hex.EncodeToString(p.Oid)
		if seen[oid] != "blob archived" || p.Chunk.Hash != "git-sha1:"+oid {
			t.Fatal("direct/main mismatch", oid, p.String())
		}
		blobRows++
	}
	if uint64(len(seen)) != info.metadata.Stats().MetadataObjects || mainRows != stats.Objects || blobRows != stats.Blobs || stats.Blobs == 0 {
		t.Fatal("object partition incomplete", len(seen), info.metadata.Stats(), stats, mainRows, blobRows)
	}
	if got := info.metadata.Stats().RecipeCalls; got != uint64(expectedCalls) {
		t.Fatal("admission repeated or missed", got, expectedCalls)
	}
	if d.Main.Metrics().Rows != uint64(mainRows) || d.Blobs.Metrics().Rows != uint64(blobRows) {
		t.Fatal("spool accounting", d.Main.Metrics(), d.Blobs.Metrics())
	}
	if err = info.close(); err != nil {
		t.Fatal(err)
	}
	if files, e := os.ReadDir(tmp); e != nil || len(files) != 0 {
		t.Fatal("dispatch/provider scratch leaked", files, e)
	}
}

func TestOrderedArchiveImportMixedReadback(t *testing.T) {
	source, tip, pack := orderedArchiveMixedFixture(t)
	allLocalTestEnv(t, pack)
	capture := deferredCapture(t)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	stats, err := importWithMetadataThreshold(t.Context(), backend, ImportOptions{Repo: source, TempDir: tmp, CompressionWorkers: 3}, 0)
	if err != nil {
		t.Fatal(err)
	}
	c := capture()
	if stats.Objects != c["pack_metadata_objects"] || stats.Objects != c["ordered_selected_objects"] || c["ordered_main_rows"] != c["archive_blob_admitted"]+c["tree_native_admitted"] || c["ordered_direct_rows"] != c["archive_blob_admitted"] {
		t.Fatal("ordered count preservation", stats, c)
	}
	if c["archive_blob_admitted"] == 0 || c["tree_native_admitted"] == 0 || c["tree_native_fallback"] != 1 || c["tree_body_requests"] != 1 || c["ordered_blob_bypasses"] < 2 {
		t.Fatal("mixed routes missing", c)
	}
	if c["archive_recipe_calls"] != c["archive_blob_admitted"]+c["archive_blob_limits"]+c["tree_total"] {
		t.Fatal("worker repeated archive admission", c)
	}
	if c["tree_identities_staged"] != c["tree_total"] {
		t.Fatal("tree stats lost ordered identities", c)
	}
	m, _, err := readHead(t.Context(), backend)
	if err != nil || m.Version != formatVersion || m.HistoryCount != 7 || len(m.Tips) != 2 {
		t.Fatal("reachable roots changed", m, err)
	}
	for _, oid := range strings.Fields(archiveImportInput(t, source, "", "rev-list", "--all")) {
		deferredCheckReadback(t, backend, source, oid, []string{"fast.txt", "small", "empty", "large", "dir/child"})
	}
	r, err := New(backend, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	s, err := r.Open(t.Context(), tip)
	if err != nil {
		t.Fatal(err)
	}
	wide, err := s.Resolve(t.Context(), "wide")
	if err != nil {
		t.Fatal(err)
	}
	var o object
	if err = s.idx.get(t.Context(), "o/"+wide.OID, &o); err != nil || isArchiveTree(o.Directory) {
		t.Fatal("wide tree fallback changed", o, err)
	}
	var total int
	after := ""
	for {
		page, e := s.ReadDir(t.Context(), wide.OID, after, 128)
		if e != nil {
			t.Fatal(e)
		}
		total += len(page)
		if len(page) < 128 {
			break
		}
		after = page[len(page)-1].Name
	}
	if total != 2100 {
		t.Fatal("wide tree truncated", total)
	}
	if files, e := os.ReadDir(tmp); e != nil || len(files) != 0 {
		t.Fatal("import scratch leaked", files, e)
	}
	t.Run("source failure during replacement preserves publication", func(t *testing.T) {
		before, token, err := backend.Get(t.Context(), "HEAD", 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		bin := t.TempDir()
		attempt := filepath.Join(bin, "source-command-attempted")
		script := "#!/bin/sh\nprintf attempted > '" + strings.ReplaceAll(attempt, "'", "'\\''") + "'\nexit 77\n"
		if err = os.WriteFile(filepath.Join(bin, "git"), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
		if err = os.WriteFile(filepath.Join(source, "bounded-root-history"), []byte("owned replacement fixture\n"), 0600); err != nil {
			t.Fatal(err)
		}
		scratch := t.TempDir()
		_, err = Import(t.Context(), backend, ImportOptions{Repo: source, TempDir: scratch, CompressionWorkers: 3})
		if err == nil || !strings.Contains(err.Error(), "read source repository") {
			t.Fatal("source failure during replacement was not returned", err)
		}
		attempted, err := os.ReadFile(attempt)
		if err != nil || string(attempted) != "attempted" {
			t.Fatal("replacement did not attempt the source command", err)
		}
		after, current, err := backend.Get(t.Context(), "HEAD", 0, -1)
		if err != nil || token != current || !bytes.Equal(before, after) {
			t.Fatal("source failure changed published HEAD", err)
		}
		replacementScratchEmpty(t, scratch)
	})
}

func TestOrderedArchiveDispatchCloseJoinsBlockedProducer(t *testing.T) {
	// More than 64KiB of commit rows blocks the producer inside its bounded pipe
	// write. No payload admission or body reconstruction is required by this test.
	const count = 1400
	entries := make([]boundedBlobFixtureEntry, count)
	for i := range entries {
		entries[i] = boundedBlobFixtureEntry{kind: 1, raw: []byte("tree " + strings.Repeat("0", 40) + "\nauthor A <a@b> 1 +0000\ncommitter A <a@b> 1 +0000\n\n" + strconv.Itoa(i) + "\n")}
	}
	pack, _ := boundedBlobFixture(t, entries)
	tmp := t.TempDir()
	info := orderedArchiveTestInfo(t, t.Context(), pack, tmp)
	t.Cleanup(func() { _ = info.close() })
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stream, err := newOrderedArchiveInventory(t.Context(), tmp, info, backend)
	if err != nil {
		t.Fatal(err)
	}
	var first [1]byte
	if n, e := stream.Read(first[:]); e != nil || n != 1 {
		t.Fatal("first bounded write", n, e)
	}
	select {
	case <-info.ordered.done:
		t.Fatal("producer unexpectedly completed without pipe drainage")
	default:
	}
	closed := make(chan error, 1)
	go func() { closed <- stream.Close() }()
	select {
	case err = <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream close failed to join producer")
	}
	if err = info.close(); !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("aborted producer error lost", err)
	}
	if files, e := os.ReadDir(tmp); e != nil || len(files) != 0 {
		t.Fatal("aborted dispatch scratch leaked", files, e)
	}
}

func TestOrderedArchiveImportFailureNeverPublishes(t *testing.T) {
	TestAllLocalCommitFailureNeverPublishes(t)
}
func TestOrderedArchiveImportCASPreservesWinner(t *testing.T) {
	TestAllLocalCommitCASPreservesWinner(t)
}
func TestOrderedArchiveImportCancellationJoinsCopy(t *testing.T) {
	TestAllLocalCommitCancellationJoinsCopy(t)
}
