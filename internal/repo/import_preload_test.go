package repo

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gat/internal/store"
)

func TestPreloadedImportConcurrentPublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	root := t.TempDir()
	ready, release := make(chan string, 2), make(chan struct{})
	results := make(chan error, 2)
	var heads [2]string
	for i := range heads {
		source := t.TempDir()
		command(t, source, "init", "-q")
		write(t, source, "file", []byte{byte('a' + i)})
		heads[i] = commit(t, source)
		local, err := store.NewLocal(root)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			_, err := importWithMetadataThreshold(ctx, &publicationStore{Store: local, ready: ready, release: release}, ImportOptions{Repo: source, CompressionWorkers: 4}, 0)
			results <- err
		}()
	}
	for range heads {
		select {
		case token := <-ready:
			if token != "*" {
				t.Fatalf("fresh publication token %q", token)
			}
		case err := <-results:
			t.Fatalf("failed before publication: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	local, err := store.NewLocal(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := local.Get(ctx, "HEAD", 0, -1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("early publication: %v", err)
	}
	close(release)
	success, conflict := 0, 0
	for range heads {
		select {
		case err := <-results:
			switch {
			case err == nil:
				success++
			case errors.Is(err, store.ErrConflict):
				conflict++
			default:
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("success=%d conflict=%d", success, conflict)
	}
	r, err := New(local, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	visible := 0
	for i, head := range heads {
		snapshot, err := r.Open(ctx, head)
		if err != nil {
			continue
		}
		visible++
		file, err := snapshot.Resolve(ctx, "file")
		if err != nil {
			t.Fatal(err)
		}
		body := make([]byte, 1)
		if n, err := snapshot.ReadAt(ctx, file.OID, body, 0); err != nil || n != 1 || body[0] != byte('a'+i) {
			t.Fatalf("published read: %q %v", body, err)
		}
	}
	if visible != 1 {
		t.Fatalf("visible generations=%d", visible)
	}
}

func TestMetadataPreloadSourceGate(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q")
	write(t, source, "file", []byte("data"))
	commit(t, source)
	if !sourceSupportsMetadataPreload(t.Context(), source, 0) {
		t.Fatal("local inventory disabled")
	}
	if sourceSupportsMetadataPreload(t.Context(), source, 1_000_000) {
		t.Fatal("small source selected for full inventory")
	}
	alternate := t.TempDir()
	command(t, alternate, "clone", "-q", "--shared", source, "repo")
	if sourceSupportsMetadataPreload(t.Context(), filepath.Join(alternate, "repo"), 0) {
		t.Fatal("alternate source selected for full inventory")
	}
}

// These tests exercise the complete import engine and its published snapshots.
// Lowering only the inventory activation threshold keeps fixtures small.
func TestPreloadedImportPreservesLazySnapshots(t *testing.T) {
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			source := t.TempDir()
			command(t, source, "init", "-q", "--object-format="+format)
			write(t, source, "file", []byte("old\n"))
			write(t, source, "sub/script", []byte("script\n"))
			first := commit(t, source)
			write(t, source, "file", []byte("new\n"))
			second := commit(t, source)
			local, e := store.NewLocal(t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			backend := &countedStore{Store: local}
			tmp := t.TempDir()
			if _, e = importWithMetadataThreshold(t.Context(), backend, ImportOptions{Repo: source, TempDir: tmp, CompressionWorkers: 4}, 0); e != nil {
				t.Fatal(e)
			}
			r, _ := New(backend, 64<<10)
			for _, rev := range []string{first, second} {
				snap, e := r.Open(t.Context(), rev)
				if e != nil {
					t.Fatal(e)
				}
				backend.reset()
				entries, e := snap.ReadDir(t.Context(), snap.Tree, "", 100)
				if e != nil || len(entries) != 2 || backend.packGets != 0 || backend.gets > 10 {
					t.Fatalf("lazy root: entries=%d gets=%d payload=%d error=%v", len(entries), backend.gets, backend.packGets, e)
				}
				entry, e := snap.Resolve(t.Context(), "file")
				if e != nil {
					t.Fatal(e)
				}
				got := make([]byte, 4)
				n, e := snap.ReadAt(t.Context(), entry.OID, got, 0)
				want := []byte("old\n")
				if rev == second {
					want = []byte("new\n")
				}
				if e != nil || n != len(want) || !bytes.Equal(got, want) {
					t.Fatalf("file %s: %q %v", rev, got, e)
				}
				graphParity(t, r, snap, source, "rev-list", "--parents", rev)
				checkObjectViewParity(t, t.Context(), r, snap, source, "", []string{"ls-tree", "-rl", rev})
			}
			if files, e := os.ReadDir(tmp); e != nil || len(files) != 0 {
				t.Fatalf("scratch remains: %v %v", files, e)
			}
		})
	}
}

func TestPreloadedImportIgnoresCorruptUnreachableObjects(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q")
	write(t, source, "file", []byte("reachable\n"))
	head := commit(t, source)
	corrupt := filepath.Join(source, ".git", "objects", "ab", strings.Repeat("c", 38))
	if e := os.MkdirAll(filepath.Dir(corrupt), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(corrupt, []byte("invalid loose object"), 0600); e != nil {
		t.Fatal(e)
	}
	command(t, source, "count-objects", "-v")
	probe := exec.Command("git", "-C", source, "cat-file", "--batch-all-objects", "--batch-check")
	output, scanErr := probe.CombinedOutput()
	if scanErr == nil && !bytes.Contains(output, []byte("missing")) {
		t.Fatalf("fixture did not report an unreadable object: %q", output)
	}
	local, e := store.NewLocal(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	tmp := t.TempDir()
	if _, e = importWithMetadataThreshold(t.Context(), local, ImportOptions{Repo: source, TempDir: tmp, CompressionWorkers: 4}, 0); e != nil {
		t.Fatalf("unreachable corruption must not prevent import: %v", e)
	}
	r, _ := New(local, 64<<10)
	snap, e := r.Open(t.Context(), head)
	if e != nil {
		t.Fatal(e)
	}
	checkObjectViewParity(t, t.Context(), r, snap, source, "", []string{"ls-tree", "-r", head})
	if files, e := os.ReadDir(tmp); e != nil || len(files) != 0 {
		t.Fatalf("scratch remains: %v %v", files, e)
	}
}
