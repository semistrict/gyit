package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	storagev1 "gat/internal/gen/gat/storage/v1"
	"gat/internal/store"
	"google.golang.org/protobuf/proto"
)

// A wide directory with one changed file per revision isolates metadata
// amplification from blob compression and network transfer.
func directoryHistory(tb testing.TB, versions, files int) (string, string) {
	tb.Helper()
	dir := tb.TempDir()
	run := func(args ...string) string {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			tb.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("init", "--bare", "-q")
	var input bytes.Buffer
	for revision := 0; revision < versions; revision++ {
		fmt.Fprintf(&input, "commit refs/heads/main\ncommitter Test <test@example.test> %d +0000\ndata 7\nfixture\n", 1000000000+revision)
		start, end := 0, files
		if revision > 0 {
			start, end = revision%files, revision%files+1
		}
		for i := start; i < end; i++ {
			content := fmt.Sprintf("file %d revision %d\n", i, revision)
			fmt.Fprintf(&input, "M 100644 inline file-%06d\ndata %d\n%s", i, len(content), content)
		}
		input.WriteByte('\n')
	}
	cmd := exec.Command("git", "-C", dir, "fast-import", "--quiet")
	cmd.Stdin = &input
	if out, err := cmd.CombinedOutput(); err != nil {
		tb.Fatalf("fast-import: %v: %s", err, out)
	}
	run("symbolic-ref", "HEAD", "refs/heads/main")
	return dir, run("rev-parse", "HEAD")
}

func directoryStoreBytes(tb testing.TB, root string) int64 {
	tb.Helper()
	var total int64
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err == nil {
			total += info.Size()
		}
		return err
	})
	if err != nil {
		tb.Fatal(err)
	}
	return total
}

func TestDirectoryHistorySpaceAndLazyReads(t *testing.T) {
	source, sha := directoryHistory(t, 64, 512)
	root := t.TempDir()
	local, err := store.NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Import(t.Context(), local, ImportOptions{Repo: source, DeltaDepth: 1}); err != nil {
		t.Fatal(err)
	}
	s := &countedStore{Store: local}
	r, _ := New(s, 64<<10)
	snap, err := r.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	after := ""
	for {
		entries, err := snap.ReadDir(t.Context(), snap.Tree, after, 73)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			break
		}
		for _, e := range entries {
			names = append(names, e.Name)
			if e.Size < 1 || e.Mode != 0100644 {
				t.Fatalf("invalid stat for %q: %+v", e.Name, e)
			}
		}
		after = entries[len(entries)-1].Name
	}
	want := command(t, source, "ls-tree", "--name-only", sha)
	if strings.Join(names, "\n") != want {
		t.Fatal("directory differs from Git")
	}
	if s.packGets != 0 {
		t.Fatalf("listing fetched %d blob ranges", s.packGets)
	}
	// Changing one of 512 names over 64 revisions must not duplicate every
	// historical entry in the published catalog. This budget includes all data.
	if size := directoryStoreBytes(t, root); size > 1<<20 {
		t.Fatalf("directory history occupies %d bytes, budget 1 MiB", size)
	}
}

func TestWideDirectoryPaginationAndLookup(t *testing.T) {
	// Exceeds both the in-memory sorting budget and one level of routing pages.
	source, sha := directoryHistory(t, 2, 17000)
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Import(t.Context(), local, ImportOptions{Repo: source, DisableDeltas: true}); err != nil {
		t.Fatal(err)
	}
	s := &countedStore{Store: local}
	r, _ := New(s, 64<<10)
	snap, err := r.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"file-000000", "file-000127", "file-000128", "file-016383", "file-016384", "file-016999"} {
		s.reset()
		e, err := snap.Lookup(t.Context(), snap.Tree, name)
		if err != nil || e.Name != name {
			t.Fatalf("lookup %s: %+v %v", name, e, err)
		}
		if s.gets > 8 || s.bytes > 128<<10 || s.packGets != 0 {
			t.Fatalf("unbounded lookup: gets=%d bytes=%d blobs=%d", s.gets, s.bytes, s.packGets)
		}
	}
	if _, err := snap.Lookup(t.Context(), snap.Tree, "file-017000"); !IsNotFound(err) {
		t.Fatalf("missing entry: %v", err)
	}
	var names []string
	after := ""
	for {
		entries, err := snap.ReadDir(t.Context(), snap.Tree, after, 127)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			break
		}
		for _, e := range entries {
			names = append(names, e.Name)
		}
		after = entries[len(entries)-1].Name
	}
	if strings.Join(names, "\n") != command(t, source, "ls-tree", "--name-only", sha) {
		t.Fatal("paged directory differs from Git")
	}
	checkObjectViewParity(t, t.Context(), r, snap, source, "", []string{"cat-file", "tree", snap.Tree})
}

func TestEmptyDirectoryRoundTrip(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q")
	tree := command(t, source, "mktree")
	sha := command(t, source, "commit-tree", tree, "-m", "empty")
	command(t, source, "update-ref", "HEAD", sha)
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Import(t.Context(), local, ImportOptions{Repo: source, DeltaDepth: 1}); err != nil {
		t.Fatal(err)
	}
	r, _ := New(local, 64<<10)
	snap, err := r.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := snap.ReadDir(t.Context(), tree, "", 128)
	if err != nil || len(entries) != 0 {
		t.Fatalf("empty directory: %v %v", entries, err)
	}
	if _, err := snap.Lookup(t.Context(), tree, "missing"); !IsNotFound(err) {
		t.Fatalf("missing entry: %v", err)
	}
	checkObjectViewParity(t, t.Context(), r, snap, source, "", []string{"cat-file", "tree", tree})
}

func TestInterleavedImportPreservesFileVersions(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q")
	base := make([]byte, 64<<10)
	rand.New(rand.NewSource(849)).Read(base)
	contents := func(version, file int) []byte {
		data := bytes.Clone(base)
		copy(data, fmt.Sprintf("file %06d version %06d", file, version))
		if file == 0 {
			data = append(data, bytes.Repeat(base, 17)...)
		}
		return data
	}
	var revisions []string
	for version := 0; version < 12; version++ {
		for file := 0; file < 12; file++ {
			write(t, source, fmt.Sprintf("file-%02d", file), contents(version, file))
		}
		revisions = append(revisions, commit(t, source))
	}
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stats, err := Import(t.Context(), local, ImportOptions{Repo: source, CompressionWorkers: 4, DeltaDepth: 2})
	if err != nil {
		t.Fatal(err)
	}
	if stats.DeltaChunks == 0 {
		t.Fatal("fixture did not exercise delta reads")
	}
	r, _ := New(local, 2*ChunkSize)
	for version, sha := range revisions {
		snap, err := r.Open(t.Context(), sha)
		if err != nil {
			t.Fatal(err)
		}
		for file := 0; file < 12; file++ {
			entry, err := snap.Resolve(t.Context(), fmt.Sprintf("file-%02d", file))
			if err != nil {
				t.Fatal(err)
			}
			want := contents(version, file)
			got := make([]byte, len(want))
			n, err := snap.ReadAt(t.Context(), entry.OID, got, 0)
			if err != nil || n != len(want) || !bytes.Equal(got, want) {
				t.Fatalf("version %d file %d differs: %d %v", version, file, n, err)
			}
		}
	}
}

func TestLegacyDirectoryReadersSurviveImport(t *testing.T) {
	for _, version := range []uint32{4, 5} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			source := t.TempDir()
			command(t, source, "init", "-q")
			write(t, source, "file", []byte("old"))
			first := commit(t, source)
			tree := command(t, source, "rev-parse", "HEAD^{tree}")
			blob := command(t, source, "rev-parse", "HEAD:file")
			local, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			wire := func(message proto.Message) []byte {
				b, err := proto.Marshal(message)
				if err != nil {
					t.Fatal(err)
				}
				return b
			}
			// A v4/v5 wire fixture deliberately contains no Directory field. Names
			// live in t/ keys, as documented for existing stores.
			page := &storagev1.IndexPage{Items: []*storagev1.IndexItem{
				{Key: "o/" + first, Value: wire(&storagev1.ObjectRecord{Kind: storagev1.ObjectKind_OBJECT_KIND_COMMIT, Tree: tree})},
				{Key: "o/" + tree, Value: wire(&storagev1.ObjectRecord{Kind: storagev1.ObjectKind_OBJECT_KIND_TREE})},
				{Key: "o/" + blob, Value: wire(&storagev1.ObjectRecord{Kind: storagev1.ObjectKind_OBJECT_KIND_BLOB, Size: 3})},
				{Key: "t/" + tree + "/66696c65", Value: wire(&storagev1.DirectoryEntry{Oid: blob, Mode: 0100644, Size: 3})},
			}}
			sort.Slice(page.Items, func(i, j int) bool { return page.Items[i].Key < page.Items[j].Key })
			raw := wire(page)
			ref := &storagev1.PageReference{Pack: "index/legacy/00000000", Length: int64(len(raw)), Hash: fmt.Sprintf("%x", sha256.Sum256(raw))}
			if err := local.Put(t.Context(), ref.Pack, raw, ""); err != nil {
				t.Fatal(err)
			}
			manifest := &storagev1.Manifest{Version: version, Format: "sha1", RootPage: ref, Tips: []string{first}}
			if err := local.Put(t.Context(), "HEAD", wire(manifest), "*"); err != nil {
				t.Fatal(err)
			}
			r, _ := New(local, 64<<10)
			old, err := r.Open(t.Context(), first)
			if err != nil {
				t.Fatal(err)
			}
			check := func(snap *Snapshot, size int64) {
				entries, err := snap.ReadDir(t.Context(), snap.Tree, "", 128)
				if err != nil || len(entries) != 1 || entries[0].Name != "file" || entries[0].Size != size {
					t.Fatalf("legacy listing: %+v %v", entries, err)
				}
				e, err := snap.Resolve(t.Context(), "file")
				if err != nil || e.Size != size {
					t.Fatalf("legacy lookup: %+v %v", e, err)
				}
			}
			check(old, 3)
			write(t, source, "file", []byte("updated"))
			next := commit(t, source)
			if _, err := Import(t.Context(), local, ImportOptions{Repo: source, DeltaDepth: 1}); err != nil {
				t.Fatal(err)
			}
			current, err := r.Open(t.Context(), next)
			if err != nil {
				t.Fatal(err)
			}
			check(current, 7)
			check(old, 3)
		})
	}
}

func BenchmarkImportDirectoryHistory(b *testing.B) {
	source, _ := directoryHistory(b, 256, 2048)
	root := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		path := filepath.Join(root, fmt.Sprint(i))
		s, err := store.NewLocal(path)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := Import(context.Background(), s, ImportOptions{Repo: source}); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		b.ReportMetric(float64(directoryStoreBytes(b, path)), "store-bytes")
		if err := os.RemoveAll(path); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}

// Many versions of independent files expose serialization caused by scheduling
// an entire path's history onto one compression worker before feeding the next.
func BenchmarkImportBlobHistory(b *testing.B) {
	source := b.TempDir()
	if out, err := exec.Command("git", "-C", source, "init", "--bare", "-q").CombinedOutput(); err != nil {
		b.Fatalf("init: %v: %s", err, out)
	}
	cmd := exec.Command("git", "-C", source, "fast-import", "--quiet")
	input, err := cmd.StdinPipe()
	if err != nil {
		b.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		b.Fatal(err)
	}
	random := rand.New(rand.NewSource(381))
	base := make([]byte, 256<<10)
	random.Read(base)
	for version := 0; version < 32; version++ {
		fmt.Fprintf(input, "commit refs/heads/main\ncommitter Test <test@example.test> %d +0000\ndata 7\nfixture\n", 1000000000+version)
		for file := 0; file < 64; file++ {
			data := bytes.Clone(base)
			copy(data, fmt.Sprintf("file %06d revision %06d", file, version))
			fmt.Fprintf(input, "M 100644 inline file-%06d\ndata %d\n", file, len(data))
			input.Write(data)
			fmt.Fprintln(input)
		}
		fmt.Fprintln(input)
	}
	input.Close()
	if err := cmd.Wait(); err != nil {
		b.Fatalf("fast-import: %v: %s", err, &stderr)
	}
	root := b.TempDir()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		path := filepath.Join(root, fmt.Sprint(i))
		s, err := store.NewLocal(path)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		stats, err := Import(b.Context(), s, ImportOptions{Repo: source})
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		b.ReportMetric(float64(directoryStoreBytes(b, path)), "store-bytes")
		b.ReportMetric(float64(stats.DeltaChunks), "delta-chunks")
		if err := os.RemoveAll(path); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}
