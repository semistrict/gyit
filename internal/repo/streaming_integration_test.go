package repo

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"gyit/internal/store"
)

func TestStreamingFallbackKeepsOriginalOrder(t *testing.T) {
	type row struct {
		oid, hint string
		size      int64
	}
	rows := []row{
		{strings.Repeat("1", 40), "same path", 80},
		{strings.Repeat("2", 40), "same path", 180},
		{strings.Repeat("3", 40), "other", 40},
		{strings.Repeat("4", 40), "same path", 120},
		{strings.Repeat("5", 40), "", 20},
	}
	run := func(native bool) (string, int64, []uint64) {
		tmp := t.TempDir()
		ids, err := os.Create(filepath.Join(tmp, "ids"))
		if err != nil {
			t.Fatal(err)
		}
		defer ids.Close()
		var input strings.Builder
		for i, r := range rows {
			if !native && i == 1 {
				continue
			}
			fmt.Fprintf(&input, "%s blob %d %s\n", r.oid, r.size, r.hint)
		}
		sizes := &blobSizes{parent: tmp}
		var own func(string, string, int64) (bool, error)
		var replay func(func(string, string, int64) error) error
		if native {
			own = func(oid, hint string, size int64) (bool, error) {
				return oid == rows[1].oid || oid == rows[0].oid || oid == rows[4].oid, nil
			}
			replay = func(emit func(string, string, int64) error) error {
				// Worker completion order must not change fallback candidate order.
				for _, i := range []int{4, 0} {
					r := rows[i]
					if err := emit(r.oid, r.hint, r.size); err != nil {
						return err
					}
				}
				return nil
			}
		}
		got, err := prepareImportMetadataWithNative(t.Context(), ids, tmp, true, 3, nil, io.NopCloser(strings.NewReader(input.String())), sizes, nil, own, replay)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(ids)
		if err != nil {
			t.Fatal(err)
		}
		return string(data), got.records, got.laneBytes
	}
	want, count, weights := run(false)
	got, gotCount, gotWeights := run(true)
	if got != want || gotCount != count || !reflect.DeepEqual(gotWeights, weights) {
		t.Fatalf("fallback changed: got %q count=%d weights=%v; want %q count=%d weights=%v", got, gotCount, gotWeights, want, count, weights)
	}
}

func TestStreamingImportCountsOnlyReachableObjects(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q")
	small := bytes.Repeat([]byte("native source body\n"), 512)
	large := bytes.Repeat([]byte("fallback body\n"), ChunkSize/14+100)
	write(t, source, "small", small)
	write(t, source, "large", large)
	write(t, source, "empty", nil)
	head := commit(t, source)
	command(t, source, "repack", "-ad")
	packs, err := filepath.Glob(filepath.Join(source, ".git", "objects", "pack", "*.pack"))
	if err != nil || len(packs) != 1 {
		t.Fatalf("pack fixture %v %v", packs, err)
	}
	unreachablePath := filepath.Join(t.TempDir(), "unreachable")
	if err := os.WriteFile(unreachablePath, []byte("not reachable from any ref"), 0600); err != nil {
		t.Fatal(err)
	}
	unreachable := command(t, source, "hash-object", "-w", unreachablePath)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()
	stats, err := importWithMetadataThreshold(t.Context(), backend, ImportOptions{Repo: source, TempDir: scratch, CompressionWorkers: 3}, 0)
	if err != nil {
		t.Fatal(err)
	}
	// One commit, one tree, three distinct blobs; inventory's unreachable blob
	// must neither be converted nor exposed in the published identity index.
	if stats.Objects != 5 || stats.Blobs != 3 {
		t.Fatalf("double counted or imported unreachable: %+v", stats)
	}
	wantBytes := int64(len(small) + len(large))
	for _, oid := range []string{head, command(t, source, "rev-parse", head+"^{tree}")} {
		n, err := strconv.ParseInt(command(t, source, "cat-file", "-s", oid), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		wantBytes += n
	}
	if stats.Bytes != wantBytes {
		t.Fatalf("source bytes counted incorrectly: got %d want %d", stats.Bytes, wantBytes)
	}
	r, err := New(backend, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := r.Open(t.Context(), head)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string][]byte{"small": small, "large": large, "empty": {}} {
		entry, err := snapshot.Resolve(t.Context(), name)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(want)+1)
		n, err := snapshot.ReadAt(t.Context(), entry.OID, got, 0)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if !bytes.Equal(got[:n], want) {
			t.Fatalf("%s differs", name)
		}
	}
	var absent object
	if err := snapshot.idx.get(t.Context(), "o/"+unreachable, &absent); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unreachable object exposed: %v", err)
	}
	if files, err := os.ReadDir(scratch); err != nil || len(files) != 0 {
		t.Fatalf("scratch remains: %v %v", files, err)
	}
}

func TestStreamingInventoryFailureFallsBackBeforeNativeOpen(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q")
	write(t, source, "file", []byte("reachable body\n"))
	head := commit(t, source)
	corrupt := filepath.Join(source, ".git", "objects", "ab", strings.Repeat("c", 38))
	if err := os.MkdirAll(filepath.Dir(corrupt), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(corrupt, []byte("unreadable unreachable object"), 0600); err != nil {
		t.Fatal(err)
	}
	// An unreadable unreachable loose object must not prevent the ordinary
	// reachable-only path from importing valid reachable objects.
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()
	stats, err := importWithMetadataThreshold(t.Context(), backend, ImportOptions{Repo: source, TempDir: scratch, CompressionWorkers: 3}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Objects != 3 || stats.Blobs != 1 {
		t.Fatalf("reachable fallback counts: %+v", stats)
	}
	r, err := New(backend, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Open(t.Context(), head); err != nil {
		t.Fatal(err)
	}
	if files, err := os.ReadDir(scratch); err != nil || len(files) != 0 {
		t.Fatalf("scratch remains: %v %v", files, err)
	}
}
