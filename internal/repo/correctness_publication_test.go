package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gyit/internal/store"
)

type replacementFixture struct{ source, old, next, pack, oldBody, nextBody string }

func makeReplacementFixture(t *testing.T) replacementFixture {
	t.Helper()
	f := replacementFixture{source: t.TempDir(), oldBody: strings.Repeat("old version\n", 512), nextBody: strings.Repeat("replacement version\n", 513)}
	archiveImportInput(t, f.source, "", "init", "-b", "main")
	f.old = replacementCommit(t, f.source, f.oldBody, "")
	archiveImportInput(t, f.source, "", "update-ref", "refs/heads/main", f.old)
	f.pack = replacementPack(t, f.source)
	if err := os.WriteFile(filepath.Join(f.source, "bounded-root-history"), []byte("owned replacement fixture\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return f
}

func replacementCommit(t *testing.T, source, body, parent string) string {
	t.Helper()
	blob := archiveImportInput(t, source, body, "hash-object", "-w", "--stdin")
	tree := archiveImportInput(t, source, "100644 blob "+blob+"\tfile\n", "mktree")
	args := []string{"commit-tree", tree}
	if parent != "" {
		args = append(args, "-p", parent)
	}
	return archiveImportInput(t, source, "fixture commit\n", args...)
}

func replacementPack(t *testing.T, source string) string {
	t.Helper()
	archiveImportInput(t, source, "", "-c", "pack.writeReverseIndex=true", "repack", "-adf")
	packs, err := filepath.Glob(filepath.Join(source, ".git/objects/pack/*.pack"))
	if err != nil || len(packs) != 1 {
		t.Fatal("single fixture pack", packs, err)
	}
	return strings.TrimSuffix(packs[0], ".pack")
}

// Call only after the initial import. The old store cannot already contain the
// new commit, tree, blob, or their global-size entries.
func appendReplacementFixture(t *testing.T, f *replacementFixture) {
	t.Helper()
	f.next = replacementCommit(t, f.source, f.nextBody, f.old)
	archiveImportInput(t, f.source, "", "update-ref", "refs/heads/main", f.next)
	oldPack := f.pack
	f.pack = replacementPack(t, f.source)
	if f.pack == oldPack {
		t.Fatal("new payloads did not change the source pack")
	}
}

func replacementRead(ctx context.Context, s *Snapshot, sha, body string) error {
	if s.SHA != sha {
		return fmt.Errorf("snapshot SHA %s, want %s", s.SHA, sha)
	}
	entries, err := s.ReadDir(ctx, s.Tree, "", 128)
	if err != nil {
		return err
	}
	if len(entries) != 1 || entries[0].Name != "file" || entries[0].Size != int64(len(body)) {
		return fmt.Errorf("directory changed: %+v", entries)
	}
	entry, err := s.Resolve(ctx, "file")
	if err != nil {
		return err
	}
	got := make([]byte, len(body))
	n, err := s.ReadAt(ctx, entry.OID, got, 0)
	if err != nil || n != len(got) || string(got) != body {
		return fmt.Errorf("read %d/%d, err %v, bytes equal %v", n, len(got), err, string(got) == body)
	}
	return nil
}

func replacementScratchEmpty(t *testing.T, paths ...string) {
	t.Helper()
	for _, path := range paths {
		files, err := os.ReadDir(path)
		if err != nil || len(files) != 0 {
			t.Errorf("scratch %s leaked: %v, %v", path, files, err)
		}
	}
}

type replacementBarrier struct {
	store.Store
	token   string
	ready   chan<- []byte
	release <-chan struct{}
}

func (s replacementBarrier) Put(ctx context.Context, key string, data []byte, condition string) error {
	if key == "HEAD" {
		if condition != s.token || condition == "" || condition == "*" {
			return fmt.Errorf("replacement lost original HEAD token")
		}
		select {
		case s.ready <- bytes.Clone(data):
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Store.Put(ctx, key, data, condition)
}

// Exercise public Import, real complete generation writes, and real local CAS.
// The two writers stop only at publication; no manually fabricated manifest wins.
func TestCorrectnessReplacementPublication(t *testing.T) {
	f := makeReplacementFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	initialScratch := t.TempDir()
	if _, err = Import(ctx, backend, ImportOptions{Repo: f.source, TempDir: initialScratch, CompressionWorkers: 3}); err != nil {
		t.Fatal("initial public import", err)
	}
	oldHead, token, err := backend.Get(ctx, "HEAD", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := New(backend, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	old, err := reader.OpenRevision(ctx, "main", "")
	if err != nil {
		t.Fatal(err)
	}
	if err = replacementRead(ctx, old, f.old, f.oldBody); err != nil {
		t.Fatal(err)
	}
	appendReplacementFixture(t, &f)
	if _, err = reader.Open(ctx, f.next); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("initial store already contains the new commit", err)
	}

	type outcome struct {
		stats Stats
		err   error
	}
	done := make(chan outcome, 2)
	ready := make(chan []byte, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	scratch := []string{t.TempDir(), t.TempDir()}
	for _, path := range scratch {
		go func(path string) {
			stats, e := Import(ctx, replacementBarrier{backend, token, ready, release}, ImportOptions{Repo: f.source, TempDir: path, CompressionWorkers: 3})
			done <- outcome{stats, e}
		}(path)
	}
	// A reader performs immutable snapshot reads while both generations build.
	readCtx, stopReader := context.WithCancel(ctx)
	readDone := make(chan error, 1)
	go func() {
		for {
			if e := replacementRead(readCtx, old, f.old, f.oldBody); e != nil {
				if readCtx.Err() != nil {
					readDone <- nil
				} else {
					readDone <- e
				}
				return
			}
			select {
			case <-readCtx.Done():
				readDone <- nil
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	defer func() {
		stopReader()
		if e := <-readDone; e != nil {
			t.Error(e)
		}
	}()
	offered := make([][]byte, 0, 2)
	for len(offered) < 2 {
		select {
		case b := <-ready:
			offered = append(offered, b)
		case out := <-done:
			t.Fatalf("writer ended before CAS barrier: %v", out.err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	unchanged, unchangedToken, err := backend.Get(ctx, "HEAD", 0, -1)
	if err != nil || !bytes.Equal(unchanged, oldHead) || unchangedToken != token {
		t.Fatal("unpublished outputs changed HEAD", err)
	}
	stillOld, err := reader.OpenRevision(ctx, "main", "")
	if err != nil {
		t.Fatal(err)
	}
	if err = replacementRead(ctx, stillOld, f.old, f.oldBody); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	var winner Stats
	wins, conflicts := 0, 0
	for range 2 {
		select {
		case out := <-done:
			if out.err == nil {
				wins++
				winner = out.stats
			} else if errors.Is(out.err, store.ErrConflict) {
				conflicts++
			} else {
				t.Fatal("replacement import", out.err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("wins=%d conflicts=%d", wins, conflicts)
	}
	published, _, err := backend.Get(ctx, "HEAD", 0, -1)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(published)) != winner.Generation {
		t.Fatal("CAS winner differs from published generation", err)
	}
	if !bytes.Equal(published, offered[0]) && !bytes.Equal(published, offered[1]) {
		t.Fatal("published manifest was not produced by either importer")
	}
	m, _, err := readHead(ctx, backend)
	if err != nil || m.Version != formatVersion || m.HistoryCount != 2 {
		t.Fatal("replacement manifest", m, err)
	}
	fresh, err := New(backend, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []*Repository{reader, fresh} {
		next, e := r.OpenRevision(ctx, "main", "")
		if e != nil {
			t.Fatal(e)
		}
		if e = replacementRead(ctx, next, f.next, f.nextBody); e != nil {
			t.Fatal(e)
		}
		byID, e := r.Open(ctx, f.next)
		if e != nil {
			t.Fatal("new commit missing after publication", e)
		}
		if e = replacementRead(ctx, byID, f.next, f.nextBody); e != nil {
			t.Fatal(e)
		}
	}
	if err = replacementRead(ctx, old, f.old, f.oldBody); err != nil {
		t.Fatal(err)
	}
	replacementScratchEmpty(t, append(scratch, initialScratch)...)
}

func TestCorrectnessReplacementFailurePreservesHead(t *testing.T) {
	f := makeReplacementFixture(t)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Import(t.Context(), backend, ImportOptions{Repo: f.source, TempDir: t.TempDir(), CompressionWorkers: 3}); err != nil {
		t.Fatal(err)
	}
	original, token, err := backend.Get(t.Context(), "HEAD", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	appendReplacementFixture(t, &f)
	assertOld := func(t *testing.T) {
		t.Helper()
		data, current, e := backend.Get(t.Context(), "HEAD", 0, -1)
		if e != nil || !bytes.Equal(data, original) || current != token {
			t.Fatal("failed update changed HEAD", e)
		}
		reader, e := New(backend, DefaultCacheBytes)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = reader.Open(t.Context(), f.next); !errors.Is(e, store.ErrNotFound) {
			t.Fatal("failed update exposed the new commit", e)
		}
		snapshot, e := reader.OpenRevision(t.Context(), "main", "")
		if e != nil {
			t.Fatal(e)
		}
		if e = replacementRead(t.Context(), snapshot, f.old, f.oldBody); e != nil {
			t.Fatal(e)
		}
	}
	t.Run("immutable-write", func(t *testing.T) {
		cause := errors.New("injected replacement archive write failure")
		failing := &archiveImportFailStore{Store: backend, prefix: "packs/archive-", cause: cause}
		scratch := t.TempDir()
		_, e := Import(t.Context(), failing, ImportOptions{Repo: f.source, TempDir: scratch, CompressionWorkers: 3})
		if !errors.Is(e, cause) || failing.headAttempts.Load() != 0 {
			t.Fatal("failed update reached publication or lost write error", e)
		}
		assertOld(t)
		replacementScratchEmpty(t, scratch)
	})
	t.Run("cancel-during-copy", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		entered, exited := make(chan struct{}), make(chan struct{})
		blocked := archiveBlockedCopy{backend, entered, exited}
		scratch := t.TempDir()
		done := make(chan error, 1)
		go func() {
			_, e := Import(ctx, blocked, ImportOptions{Repo: f.source, TempDir: scratch, CompressionWorkers: 3})
			done <- e
		}()
		select {
		case <-entered:
		case e := <-done:
			t.Fatal("copy not reached", e)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		cancel()
		if e := <-done; !errors.Is(e, context.Canceled) {
			t.Fatal("cancellation lost", e)
		}
		select {
		case <-exited:
		default:
			t.Fatal("copy worker not joined")
		}
		assertOld(t)
		replacementScratchEmpty(t, scratch)
	})
}
