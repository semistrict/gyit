package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gyit/internal/store"
)

func archiveImportInput(t *testing.T, source, input string, args ...string) string {
	t.Helper()
	c := git(t.Context(), source, args...)
	c.Stdin = strings.NewReader(input)
	c.Env = append(c.Environ(), "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
	var stderr bytes.Buffer
	c.Stderr = &stderr
	b, err := c.Output()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, stderr.String())
	}
	return strings.TrimSpace(string(b))
}
func archiveImportFixture(t *testing.T) (source, tip, pack, orphan string) {
	source, _, _ = deferredFixture(t)
	small := archiveImportInput(t, source, "", "rev-parse", "HEAD:small")
	var wide strings.Builder
	for i := 0; i < 2100; i++ {
		fmt.Fprintf(&wide, "100644 blob %s\tfile-%04d\n", small, i)
	}
	wideOID := archiveImportInput(t, source, wide.String(), "mktree")
	root := archiveImportInput(t, source, "", "ls-tree", "HEAD")
	root = archiveImportInput(t, source, root+fmt.Sprintf("\n040000 tree %s\twide\n", wideOID), "mktree")
	parent := archiveImportInput(t, source, "", "rev-parse", "HEAD")
	tip = archiveImportInput(t, source, "wide fallback\n", "commit-tree", root, "-p", parent)
	archiveImportInput(t, source, "", "update-ref", "HEAD", tip)
	archiveImportInput(t, source, "", "update-ref", "refs/heads/older", parent)
	archiveImportInput(t, source, "", "-c", "pack.writeReverseIndex=true", "repack", "-adf", "--window=50", "--depth=20")
	packs, err := filepath.Glob(filepath.Join(source, ".git/objects/pack/*.pack"))
	if err != nil || len(packs) != 1 {
		t.Fatal(packs, err)
	}
	pack = strings.TrimSuffix(packs[0], ".pack")
	orphan = archiveImportInput(t, source, "orphan must remain unreachable\n", "hash-object", "-w", "--stdin")
	return
}
func archiveImportEnv(t *testing.T, pack string) {
	t.Helper()
	packMetadataTestEnv(t)
	if _, err := os.Stat(pack + ".idx"); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveImportHistoryFallbackAndReadback(t *testing.T) {
	source, tip, pack, orphan := archiveImportFixture(t)
	// A complete single pack qualifies automatically. The separate loose-source
	// integration test covers reachable conversion and unreachable-object exclusion.
	if err := os.RemoveAll(filepath.Join(source, ".git", "objects", orphan[:2])); err != nil {
		t.Fatal(err)
	}
	archiveImportEnv(t, pack)
	capture := deferredCapture(t)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()
	stats, err := importWithMetadataThreshold(t.Context(), backend, ImportOptions{Repo: source, TempDir: scratch, CompressionWorkers: 3}, 0)
	if err != nil {
		t.Fatal(err)
	}
	counts := capture()
	if counts["archive_blob_admitted"] <= 0 || counts["tree_native_admitted"] <= 0 || counts["tree_native_fallback"] != 1 || counts["tree_body_requests"] != 1 {
		t.Fatalf("missing archive/fallback coverage: %+v", counts)
	}
	if counts["tree_native_fallback"]+counts["tree_native_admitted"] != counts["tree_identities_staged"] {
		t.Fatal("tree ownership counts", counts)
	}
	fi, err := os.Stat(pack + ".pack")
	if err != nil {
		t.Fatal(err)
	}
	if counts["archive_source_bytes"] != fi.Size() || counts["archive_segments"] != 1 {
		t.Fatal("source copy accounting", counts)
	}
	m, _, err := readHead(t.Context(), backend)
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != formatVersion || m.HistoryCount != 7 || len(m.Tips) != 2 || stats.Objects <= 0 {
		t.Fatal("manifest/counts", m, stats)
	}
	idx := &index{store: backend, cache: newCache(DefaultCacheBytes), root: m.Root}
	var o object
	if err = idx.get(t.Context(), "o/"+orphan, &o); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("unreachable identity published", err)
	}
	r, err := New(backend, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	s, err := r.Open(t.Context(), tip)
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.Resolve(t.Context(), "wide")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.idx.get(t.Context(), "o/"+e.OID, &o); err != nil || isArchiveTree(o.Directory) {
		t.Fatal("oversized tree must be compiled", o, err)
	}
	var total int
	after := ""
	for {
		page, err := s.ReadDir(t.Context(), e.OID, after, 128)
		if err != nil {
			t.Fatal(err)
		}
		total += len(page)
		if len(page) < 128 {
			break
		}
		after = page[len(page)-1].Name
	}
	if total != 2100 {
		t.Fatal("fallback directory pagination", total)
	}
	tips := strings.Fields(archiveImportInput(t, source, "", "rev-list", "--all"))
	for _, id := range tips {
		deferredCheckReadback(t, backend, source, id, []string{"fast.txt", "small", "empty", "large", "dir/child"})
	}
	if files, err := os.ReadDir(scratch); err != nil || len(files) != 0 {
		t.Fatal("scratch leak", files, err)
	}
}

type archiveImportFailStore struct {
	store.Store
	prefix       string
	cause        error
	headAttempts atomic.Int64
}

func (s *archiveImportFailStore) Put(ctx context.Context, key string, b []byte, condition string) error {
	if key == "HEAD" {
		s.headAttempts.Add(1)
	}
	if strings.HasPrefix(key, s.prefix) {
		return s.cause
	}
	return s.Store.Put(ctx, key, b, condition)
}
func TestArchiveImportFailureNeverPublishes(t *testing.T) {
	source, _, pack := deferredFixture(t)
	archiveImportEnv(t, pack)
	for _, prefix := range []string{"packs/archive-", "index/global-sizes-", "index/tree-archive-"} {
		t.Run(prefix, func(t *testing.T) {
			backend, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			cause := errors.New("injected immutable write failure")
			failing := &archiveImportFailStore{Store: backend, prefix: prefix, cause: cause}
			scratch := t.TempDir()
			_, err = importWithMetadataThreshold(t.Context(), failing, ImportOptions{Repo: source, TempDir: scratch, CompressionWorkers: 3}, 0)
			if !errors.Is(err, cause) {
				t.Fatal("original failure lost", err)
			}
			if failing.headAttempts.Load() != 0 {
				t.Fatal("publication attempted before immutable outputs durable")
			}
			if _, _, err = backend.Get(t.Context(), "HEAD", 0, -1); !errors.Is(err, store.ErrNotFound) {
				t.Fatal("failed import published", err)
			}
			if files, err := os.ReadDir(scratch); err != nil || len(files) != 0 {
				t.Fatal("scratch leak", files, err)
			}
		})
	}
}

type archiveRacingWriter struct {
	store.Store
	winner []byte
}

func (s archiveRacingWriter) Put(ctx context.Context, key string, b []byte, condition string) error {
	if key == "HEAD" {
		if condition != "*" {
			return fmt.Errorf("publication lost create-only CAS")
		}
		if err := s.Store.Put(ctx, key, s.winner, "*"); err != nil {
			return err
		}
	}
	return s.Store.Put(ctx, key, b, condition)
}
func TestArchiveImportCASPreservesWinner(t *testing.T) {
	source, _, pack := deferredFixture(t)
	archiveImportEnv(t, pack)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	winner := []byte("concurrent publication wins")
	_, err = importWithMetadataThreshold(t.Context(), archiveRacingWriter{backend, winner}, ImportOptions{Repo: source, TempDir: t.TempDir(), CompressionWorkers: 3}, 0)
	if !errors.Is(err, store.ErrConflict) {
		t.Fatal("CAS conflict lost", err)
	}
	got, _, err := backend.Get(t.Context(), "HEAD", 0, -1)
	if err != nil || !bytes.Equal(got, winner) {
		t.Fatal("winner changed", string(got), err)
	}
}

type archiveBlockedCopy struct {
	store.Store
	entered, exited chan struct{}
}

func (s archiveBlockedCopy) Put(ctx context.Context, key string, b []byte, condition string) error {
	if strings.HasPrefix(key, "packs/archive-") {
		close(s.entered)
		defer close(s.exited)
		<-ctx.Done()
		return ctx.Err()
	}
	return s.Store.Put(ctx, key, b, condition)
}

func TestArchiveImportCancellationJoinsCopy(t *testing.T) {
	source, _, pack := deferredFixture(t)
	archiveImportEnv(t, pack)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	blocked := archiveBlockedCopy{backend, make(chan struct{}), make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	scratch := t.TempDir()
	done := make(chan error, 1)
	go func() {
		_, err := importWithMetadataThreshold(ctx, blocked, ImportOptions{Repo: source, TempDir: scratch, CompressionWorkers: 3}, 0)
		done <- err
	}()
	select {
	case <-blocked.entered:
	case err := <-done:
		t.Fatal("import stopped before copy", err)
	case <-time.After(2 * time.Second):
		t.Fatal("archive copy did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("cancellation lost", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation failed to join import")
	}
	select {
	case <-blocked.exited:
	default:
		t.Fatal("returned with copy still running")
	}
	if _, _, err = backend.Get(t.Context(), "HEAD", 0, -1); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("canceled import published", err)
	}
	if files, err := os.ReadDir(scratch); err != nil || len(files) != 0 {
		t.Fatal("cancellation leaked scratch", files, err)
	}
}
