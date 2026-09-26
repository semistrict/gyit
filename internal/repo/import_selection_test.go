package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"gyit/internal/store"
)

func packedImportFixture(t *testing.T) (string, string, string) {
	t.Helper()
	source := filepath.Join(t.TempDir(), "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	command(t, source, "init", "-q", "--object-format=sha1")
	var revision string
	for i := 0; i < 3; i++ {
		write(t, source, "dir/file", []byte(strings.Repeat("shared contents\n", 512)+fmt.Sprint(i)))
		revision = commit(t, source)
	}
	prefix := repackImportFixture(t, source)
	return source, revision, prefix
}

func repackImportFixture(t *testing.T, source string) string {
	t.Helper()
	command(t, source, "-c", "pack.writeReverseIndex=true", "repack", "-adf")
	command(t, source, "prune-packed")
	packs, err := filepath.Glob(filepath.Join(source, ".git/objects/pack/*.pack"))
	if err != nil || len(packs) != 1 {
		t.Fatalf("source packs %v: %v", packs, err)
	}
	return strings.TrimSuffix(packs[0], ".pack")
}

func readImportFixture(ctx context.Context, snapshot *Snapshot, want string) error {
	entry, err := snapshot.Resolve(ctx, "dir/file")
	if err != nil {
		return err
	}
	got := make([]byte, len(want))
	n, err := snapshot.ReadAt(ctx, entry.OID, got, 0)
	if err != nil || n != len(got) || string(got) != want {
		return fmt.Errorf("read %d/%d bytes, error %v, equal %v", n, len(got), err, string(got) == want)
	}
	return nil
}

func TestImportAutomaticallyRetainsPackedSource(t *testing.T) {
	for _, reverseIndex := range []bool{true, false} {
		t.Run(fmt.Sprintf("reverse-index-%t", reverseIndex), func(t *testing.T) {
			source, revision, prefix := packedImportFixture(t)
			body := strings.Repeat("shared contents\n", 512) + "2"
			if !reverseIndex {
				if err := os.Remove(prefix + ".rev"); err != nil {
					t.Fatal(err)
				}
			}
			pack, err := os.ReadFile(prefix + ".pack")
			if err != nil {
				t.Fatal(err)
			}
			before := sha256.Sum256(pack)
			backend, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			tmp := t.TempDir()
			stats, err := Import(t.Context(), backend, ImportOptions{Repo: source, TempDir: tmp, CompressionWorkers: 2})
			if err != nil {
				t.Fatal(err)
			}
			if stats.ImportMode != "archive" || stats.FallbackReason != "" {
				t.Fatalf("ordinary import did not retain the source pack: %+v", stats)
			}
			manifest, _, err := readHead(t.Context(), backend)
			if err != nil || manifest.Version != formatVersion || !manifestHasGlobalSizes(manifest.Version) {
				t.Fatalf("published format %d: %v", manifest.Version, err)
			}
			pack, err = os.ReadFile(prefix + ".pack")
			if err != nil || sha256.Sum256(pack) != before {
				t.Fatalf("source pack changed: %v", err)
			}
			if !reverseIndex {
				if _, err := os.Stat(prefix + ".rev"); !os.IsNotExist(err) {
					t.Fatalf("import wrote source reverse index: %v", err)
				}
			}
			if files, err := os.ReadDir(tmp); err != nil || len(files) != 0 {
				t.Fatalf("import staging was not removed: %v, %v", files, err)
			}
			if err := os.RemoveAll(source); err != nil {
				t.Fatal(err)
			}
			repository, err := New(backend, 32<<20)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := repository.Open(t.Context(), revision)
			if err != nil {
				t.Fatal(err)
			}
			if err := readImportFixture(t.Context(), snapshot, body); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestImportPackedSourceConfigurationIsPerInvocation(t *testing.T) {
	type fixture struct {
		source, revision, body string
		backend                store.Store
	}
	fixtures := make([]fixture, 2)
	for i := range fixtures {
		source, _, _ := packedImportFixture(t)
		body := fmt.Sprintf("independent source %d\n", i)
		write(t, source, "dir/file", []byte(body))
		revision := commit(t, source)
		repackImportFixture(t, source)
		backend, err := store.NewLocal(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		fixtures[i] = fixture{source, revision, body, backend}
	}
	var wg sync.WaitGroup
	for _, f := range fixtures {
		wg.Go(func() {
			stats, err := Import(t.Context(), f.backend, ImportOptions{Repo: f.source, CompressionWorkers: 2})
			if err != nil || stats.ImportMode != "archive" {
				t.Errorf("import mode %s: %v", stats.ImportMode, err)
				return
			}
			r, err := New(f.backend, 32<<20)
			if err != nil {
				t.Error(err)
				return
			}
			snapshot, err := r.Open(t.Context(), f.revision)
			if err != nil {
				t.Error(err)
				return
			}
			if err := readImportFixture(t.Context(), snapshot, f.body); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}

func TestImportArchiveCanFallBackOnLaterUpdate(t *testing.T) {
	source, oldRevision, _ := packedImportFixture(t)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stats, err := Import(t.Context(), backend, ImportOptions{Repo: source, CompressionWorkers: 2})
	if err != nil || stats.ImportMode != "archive" {
		t.Fatalf("initial import %+v: %v", stats, err)
	}
	r, err := New(backend, 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	old, err := r.Open(t.Context(), oldRevision)
	if err != nil {
		t.Fatal(err)
	}
	body := "new loose object after the initial packed import\n"
	write(t, source, "dir/file", []byte(body))
	revision := commit(t, source)
	stats, err = Import(t.Context(), backend, ImportOptions{Repo: source, CompressionWorkers: 2})
	if err != nil || stats.ImportMode != "reachable" || stats.FallbackReason == "" {
		t.Fatalf("fallback import %+v: %v", stats, err)
	}
	m, _, err := readHead(t.Context(), backend)
	if err != nil || m.Version != legacyFormatVersion || m.Blobs != (pageRef{}) {
		t.Fatalf("fallback manifest %+v: %v", m, err)
	}
	current, err := r.Open(t.Context(), revision)
	if err != nil {
		t.Fatal(err)
	}
	if err := readImportFixture(t.Context(), current, body); err != nil {
		t.Fatal(err)
	}
	if err := readImportFixture(t.Context(), old, strings.Repeat("shared contents\n", 512)+"2"); err != nil {
		t.Fatal(err)
	}
}

func TestImportExplicitOptionsUseReachableConversion(t *testing.T) {
	source, _, _ := packedImportFixture(t)
	for _, modify := range []func(*ImportOptions){
		func(o *ImportOptions) { o.Revision = "HEAD~1" },
		func(o *ImportOptions) { o.DisableDeltas = true },
		func(o *ImportOptions) { o.DeltaDepth = 2 },
		func(o *ImportOptions) { o.DeltaCandidates = 2 },
		func(o *ImportOptions) { o.CompressionWorkers = 1 },
	} {
		opt := ImportOptions{Repo: source, CompressionWorkers: 2}
		modify(&opt)
		backend, err := store.NewLocal(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		stats, err := Import(t.Context(), backend, opt)
		if err != nil || stats.ImportMode != "reachable" || stats.FallbackReason == "" {
			t.Fatalf("explicit options %+v selected %+v: %v", opt, stats, err)
		}
	}
}

func TestGeneratedReverseIndexMatchesGit(t *testing.T) {
	source, _, prefix := packedImportFixture(t)
	want, err := os.ReadFile(prefix + ".rev")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(prefix + ".rev"); err != nil {
		t.Fatal(err)
	}
	generated, err := prepareArchivePack(t.Context(), prefix, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(generated + ".rev")
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("reverse index differs from Git: %v", err)
	}
	if command(t, source, "rev-parse", "HEAD") == "" {
		t.Fatal("source was modified")
	}
}
