package repo

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metaplan "gyit/internal/packmeta"
	"gyit/internal/store"
)

type packMetadataFixture struct {
	source, tip, pack                    string
	orphanBlob, orphanTree, orphanCommit string
	orphanBody                           string
	local                                []packMetadataObject
	parents                              map[string][]string
}
type packMetadataObject struct {
	oid, kind string
	size      int64
}

// The input is wholly owned by this test. Every previously reachable object and
// all three orphan objects are explicitly included in the replacement pack.
// Only then are that fixture's old pack and loose-object directories removed.
func makePackMetadataFixture(t *testing.T) packMetadataFixture {
	t.Helper()
	source, tip, oldPack := deferredFixture(t)
	f := packMetadataFixture{source: source, tip: tip, orphanBody: "orphan file retained independently of commit reachability\n", parents: map[string][]string{}}
	f.orphanBlob = archiveImportInput(t, source, f.orphanBody, "hash-object", "-w", "--stdin")
	f.orphanTree = archiveImportInput(t, source, fmt.Sprintf("100644 blob %s\torphan.txt\n", f.orphanBlob), "mktree")
	f.orphanCommit = archiveImportInput(t, source, "orphan commit must not enter history\n", "commit-tree", f.orphanTree)
	for _, line := range strings.Split(archiveImportInput(t, source, "", "rev-list", "--parents", "--all"), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		f.parents[fields[0]] = append([]string(nil), fields[1:]...)
	}
	if len(f.parents) != 6 {
		t.Fatalf("fixture history=%d, want6", len(f.parents))
	}
	var ids strings.Builder
	for _, line := range strings.Split(archiveImportInput(t, source, "", "cat-file", "--batch-all-objects", "--batch-check=%(objectname) %(objecttype) %(objectsize)"), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			t.Fatalf("fixture inventory %q", line)
		}
		size, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		f.local = append(f.local, packMetadataObject{fields[0], fields[1], size})
		ids.WriteString(fields[0])
		ids.WriteByte('\n')
	}
	prefix := filepath.Join(source, ".git", "objects", "pack", "pack")
	hash := archiveImportInput(t, source, ids.String(), "-c", "pack.writeReverseIndex=true", "pack-objects", "--window=50", "--depth=20", prefix)
	if len(hash) != 40 {
		t.Fatalf("pack hash %q", hash)
	}
	f.pack = prefix + "-" + hash
	if f.pack == oldPack {
		t.Fatal("orphan objects did not change the physical pack")
	}
	file, err := os.Open(f.pack + ".pack")
	if err != nil {
		t.Fatal(err)
	}
	var header [12]byte
	_, readErr := file.ReadAt(header[:], 0)
	err = errors.Join(readErr, file.Close())
	if err != nil || string(header[:4]) != "PACK" || binary.BigEndian.Uint32(header[8:]) != uint32(len(f.local)) {
		t.Fatalf("pack must contain every local object: %x: %v", header, err)
	}
	if _, err = os.Stat(f.pack + ".rev"); err != nil {
		t.Fatal("reverse index missing", err)
	}
	oldFiles, err := filepath.Glob(oldPack + ".*")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range oldFiles {
		if filepath.Dir(path) != filepath.Join(source, ".git", "objects", "pack") {
			t.Fatal("old pack escaped owned fixture")
		}
		if err = os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	objects := filepath.Join(source, ".git", "objects")
	entries, err := os.ReadDir(objects)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || len(entry.Name()) != 2 {
			continue
		}
		if _, err = strconv.ParseUint(entry.Name(), 16, 8); err != nil {
			continue
		}
		if err = os.RemoveAll(filepath.Join(objects, entry.Name())); err != nil {
			t.Fatal(err)
		}
	}
	packs, err := filepath.Glob(filepath.Join(objects, "pack", "*.pack"))
	if err != nil || len(packs) != 1 || packs[0] != f.pack+".pack" {
		t.Fatal("single physical fixture pack required", packs, err)
	}
	// These lookups now have no alternate or loose source to hide a packing
	// omission. They check every exact originally enumerated identity.
	for _, obj := range f.local {
		got := archiveImportInput(t, source, obj.oid+"\n", "cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)")
		want := fmt.Sprintf("%s %s %d", obj.oid, obj.kind, obj.size)
		if got != want {
			t.Fatalf("packed fixture mismatch %q want%q", got, want)
		}
	}
	return f
}

func TestPackMetadataImportAllLocalObjectsReachableHistory(t *testing.T) {
	f := makePackMetadataFixture(t)
	archiveImportEnv(t, f.pack)
	capture := deferredCapture(t)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()
	stats, err := importWithMetadataThreshold(t.Context(), backend, ImportOptions{Repo: f.source, TempDir: scratch, CompressionWorkers: 3}, 0)
	if err != nil {
		t.Fatal(err)
	}
	counts := capture()
	if counts["pack_metadata_objects"] != int64(len(f.local)) || counts["archive_plan_reused"] != 1 {
		t.Fatalf("integrated metadata/planner path was not used: %+v", counts)
	}
	if _, exists := counts["inventory_map_rows"]; exists {
		t.Fatal("fast metadata path rebuilt the ordinary inventory map", counts)
	}
	if counts["archive_blob_admitted"] == 0 || counts["tree_native_admitted"] == 0 {
		t.Fatal("native blob/tree coverage missing", counts)
	}
	if stats.Phase != "done" || stats.Objects != int64(len(f.local)) {
		t.Fatalf("all local objects: stats%+v local%d", stats, len(f.local))
	}
	m, _, err := readHead(t.Context(), backend)
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != formatVersion || m.HistoryCount != uint64(len(f.parents)) || len(m.Tips) != 1 {
		t.Fatalf("manifest/history scope changed: %+v", m)
	}
	r, err := New(backend, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	s, err := r.Open(t.Context(), f.tip)
	if err != nil {
		t.Fatal(err)
	}
	var blobs int64
	for _, obj := range f.local {
		if obj.kind != "tree" && obj.kind != "blob" {
			continue
		}
		var stored object
		if err = s.idx.get(t.Context(), "o/"+obj.oid, &stored); err != nil || stored.Kind != obj.kind || stored.Size != obj.size {
			t.Fatalf("all-local identity %s: %+v: %v", obj.oid, stored, err)
		}
		if obj.kind == "blob" {
			blobs++
			size, _, err := s.readBlobPart(t.Context(), obj.oid, 0)
			if err != nil || size != obj.size {
				t.Fatalf("blob size %s: %d want%d: %v", obj.oid, size, obj.size, err)
			}
		}
	}
	if stats.Blobs != blobs {
		t.Fatalf("blob population %d want%d", stats.Blobs, blobs)
	}
	var orphan object
	if err = s.idx.get(t.Context(), "o/"+f.orphanCommit, &orphan); err != nil || orphan.Kind != "commit" || orphan.Tree != f.orphanTree {
		t.Fatal("orphan commit identity missing", orphan, err)
	}
	var orphanInfo commitInfo
	if err = s.idx.get(t.Context(), "c/"+f.orphanCommit, &orphanInfo); err != nil || string(orphanInfo.Message) != "orphan commit must not enter history\n" {
		t.Fatal("orphan display metadata missing", orphanInfo, err)
	}
	var orphanParents parents
	if err = s.idx.get(t.Context(), "p/"+f.orphanCommit, &orphanParents); err != nil || len(orphanParents.Parents) != 0 {
		t.Fatal("orphan root parent record missing", orphanParents, err)
	}
	var position historyPosition
	if err = s.history.get(t.Context(), "g/"+f.orphanCommit, &position); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("orphan commit entered history", err)
	}
	cursor := &historyCursor{idx: s.history}
	seen := map[string]bool{}
	for pos := uint64(1); pos <= m.HistoryCount; pos++ {
		node, err := cursor.get(t.Context(), pos)
		if err != nil {
			t.Fatal(err)
		}
		oid := hex.EncodeToString(node.Oid)
		want, exists := f.parents[oid]
		if !exists || seen[oid] {
			t.Fatalf("unexpected history commit %s", oid)
		}
		seen[oid] = true
		var info commitInfo
		if err = s.idx.get(t.Context(), "c/"+oid, &info); err != nil {
			t.Fatal("display metadata missing", err)
		}
		var stored parents
		if err = s.idx.get(t.Context(), "p/"+oid, &stored); err != nil || strings.Join(stored.Parents, " ") != strings.Join(want, " ") {
			t.Fatalf("stored parents %s: %+v want%v: %v", oid, stored, want, err)
		}
		var got []string
		for _, parent := range append([]uint64(nil), node.Parents...) {
			base, err := cursor.get(t.Context(), parent)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, hex.EncodeToString(base.Oid))
		}
		if strings.Join(got, " ") != strings.Join(want, " ") {
			t.Fatalf("history parents %s: %v want%v", oid, got, want)
		}
		deferredCheckReadback(t, backend, f.source, oid, []string{"fast.txt", "small", "empty", "large", "dir/child"})
	}
	page, err := s.ReadDir(t.Context(), f.orphanTree, "", 128)
	if err != nil || len(page) != 1 || page[0].Name != "orphan.txt" || page[0].OID != f.orphanBlob || page[0].Size != int64(len(f.orphanBody)) {
		t.Fatal("orphan tree must retain exact file size", page, err)
	}
	buf := make([]byte, len(f.orphanBody))
	if n, err := s.ReadAt(t.Context(), f.orphanBlob, buf, 0); err != nil || n != len(buf) || !bytes.Equal(buf, []byte(f.orphanBody)) {
		t.Fatal("orphan file payload missing", string(buf), err)
	}
	if orphanSnapshot, err := r.Open(t.Context(), f.orphanCommit); err != nil || orphanSnapshot.Tree != f.orphanTree {
		t.Fatal("catalog-only commit must be an addressable checkout", err)
	}
	if files, err := os.ReadDir(scratch); err != nil || len(files) != 0 {
		t.Fatal("scratch leaked", files, err)
	}
}

func packMetadataTestEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{"GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_SHALLOW_FILE"} {
		value, present := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			var err error
			if present {
				err = os.Setenv(name, value)
			} else {
				err = os.Unsetenv(name)
			}
			if err != nil {
				t.Error(err)
			}
		})
	}
}

func TestPackMetadataImportQualification(t *testing.T) {
	f := makePackMetadataFixture(t)
	archiveImportEnv(t, f.pack)
	packMetadataTestEnv(t)
	resolved, err := filepath.EvalSymlinks(f.pack + ".pack")
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := strings.TrimSuffix(resolved, ".pack")
	if prefix, ok, err := qualifyPackSource(t.Context(), f.source, 20); err != nil || !ok || prefix != wantPrefix {
		t.Fatalf("baseline single-pack source must qualify: %q %t %v", prefix, ok, err)
	}
	objects := filepath.Join(f.source, ".git", "objects")
	for _, name := range []string{"loose", "promisor", "alternate", "shallow"} {
		t.Run(name, func(t *testing.T) {
			switch name {
			case "loose":
				oid := archiveImportInput(t, f.source, "qualification-only loose object\n", "hash-object", "-w", "--stdin")
				looseDir := filepath.Join(objects, oid[:2])
				t.Cleanup(func() {
					if err := os.RemoveAll(looseDir); err != nil {
						t.Error(err)
					}
				})
			case "promisor", "alternate", "shallow":
				path, body := f.pack+".promisor", []byte(nil)
				if name == "alternate" {
					path, body = filepath.Join(objects, "info", "alternates"), []byte(t.TempDir()+"\n")
				}
				if name == "shallow" {
					path, body = filepath.Join(f.source, ".git", "shallow"), []byte(f.tip+"\n")
				}
				if err := os.WriteFile(path, body, 0600); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := os.Remove(path); err != nil {
						t.Error(err)
					}
				})

			}
			if prefix, ok, err := qualifyPackSource(t.Context(), f.source, 20); err != nil || ok || prefix != "" {
				t.Fatalf("unsupported source %s must decline native metadata: %q %t %v", name, prefix, ok, err)
			}
			scratch := t.TempDir()
			info, used, err := loadPackSourceInfo(t.Context(), scratch, f.source, 20)
			if err != nil || used || info != nil {
				t.Fatalf("unsupported source allocated a provider: %v %t %v", info, used, err)
			}
			if files, err := os.ReadDir(scratch); err != nil || len(files) != 0 {
				t.Fatal("qualification leaked scratch", files, err)
			}
		})
	}
}

// Reuse the original intended failure/CAS/cancellation contracts with the new
// public entry. Assert provider traces so these cannot pass merely
// by silently exercising the old metadata path.
func packMetadataRunInherited(t *testing.T, expectedProviders int64, run func(*testing.T)) {
	t.Helper()
	packMetadataTestEnv(t)
	var providers atomic.Int64
	deferredTraceHook = func(name string, value int64) {
		if name == "pack_metadata_objects" && value > 0 {
			providers.Add(1)
		}
	}
	t.Cleanup(func() { deferredTraceHook = nil })
	run(t)
	if providers.Load() != expectedProviders {
		t.Fatalf("expected %d integrated native providers, observed %d", expectedProviders, providers.Load())
	}
}

func TestPackMetadataImportFailureNeverPublishes(t *testing.T) {
	packMetadataRunInherited(t, 3, TestArchiveImportFailureNeverPublishes)
}
func TestPackMetadataImportCASPreservesWinner(t *testing.T) {
	packMetadataRunInherited(t, 1, TestArchiveImportCASPreservesWinner)
}
func TestPackMetadataImportCancellationJoinsCopy(t *testing.T) {
	packMetadataRunInherited(t, 1, TestArchiveImportCancellationJoinsCopy)
}

func TestPackMetadataImportInventoryCloseJoinsBeforeProviderClose(t *testing.T) {
	// One bounded input stream creates enough distinct short blobs to force a
	// 64KiB pipe flush while ForEachObject still owns its provider read lock.
	source := t.TempDir()
	command(t, source, "init", "-q")
	var commands strings.Builder
	const blobs = 1400
	for i := 0; i < blobs; i++ {
		body := fmt.Sprintf("metadata %04d\n", i)
		fmt.Fprintf(&commands, "blob\nmark :%d\ndata %d\n%s", i+1, len(body), body)
	}
	commands.WriteString("commit refs/heads/pipe-lifetime\ncommitter Test <test@example.test> 1 +0000\ndata 10\npipe test\n")
	for i := 0; i < blobs; i++ {
		fmt.Fprintf(&commands, "M 100644 :%d f%04d\n", i+1, i)
	}
	commands.WriteString("\ndone\n")
	archiveImportInput(t, source, commands.String(), "fast-import", "--quiet")
	command(t, source, "symbolic-ref", "HEAD", "refs/heads/pipe-lifetime")
	command(t, source, "-c", "pack.writeReverseIndex=true", "repack", "-adf", "--window=0", "--depth=0")
	packs, err := filepath.Glob(filepath.Join(source, ".git", "objects", "pack", "*.pack"))
	if err != nil || len(packs) != 1 {
		t.Fatal("one tiny lifetime pack required", packs, err)
	}
	keyHex := archiveImportInput(t, source, "", "rev-parse", "HEAD:f0000")
	scratch := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p, err := metaplan.OpenSource(ctx, strings.TrimSuffix(packs[0], ".pack"), scratch)
	if err != nil {
		t.Fatal(err)
	}
	info := &sourceInfo{metadata: p}
	if p.Stats().MetadataBlobs != blobs {
		t.Fatal("lifetime fixture blob population", p.Stats())
	}
	inventory := newPackInventory(ctx, p)
	t.Cleanup(func() { _ = inventory.Close(); _ = info.close() })
	firstRead := make(chan error, 1)
	go func() {
		var first [1]byte
		n, err := inventory.Read(first[:])
		if err == nil && n != 1 {
			err = fmt.Errorf("first metadata read returned %d", n)
		}
		firstRead <- err
	}()
	select {
	case err := <-firstRead:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("inventory never reached its first buffered write")
	}
	// Only one byte has been consumed from the first full 64KiB write. Remaining
	// metadata exceeds that buffer, so this is the callback-held-lock case,
	// rather than the final Flush after ForEachObject has released its lock.
	select {
	case <-inventory.done:
		t.Fatal("producer completed without draining pipe")
	default:
	}
	closed := make(chan error, 1)
	go func() { closed <- inventory.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("inventory close did not join its producer")
	}
	select {
	case <-inventory.done:
	default:
		t.Fatal("Close returned with producer alive")
	}
	if err = inventory.Close(); err != nil {
		t.Fatal("inventory close is not idempotent", err)
	}
	id, err := hex.DecodeString(keyHex)
	if err != nil {
		t.Fatal(err)
	}
	kind, size, found, err := info.lookup(id)
	if err != nil || !found || kind != 3 || size != int64(len("metadata 0000\n")) {
		t.Fatalf("stream closure invalidated provider: %d %d %t %v", kind, size, found, err)
	}
	if err = info.close(); err != nil {
		t.Fatal(err)
	}
	var key [20]byte
	copy(key[:], id)
	if _, _, _, err = p.Lookup(key); !errors.Is(err, metaplan.ErrClosed) {
		t.Fatal("provider still owns live mappings after close", err)
	}
	if files, err := os.ReadDir(scratch); err != nil || len(files) != 0 {
		t.Fatal("provider/pipe lifecycle leaked scratch", files, err)
	}
}
