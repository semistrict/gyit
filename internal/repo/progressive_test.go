package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gyit/internal/store"
)

func TestProgressiveExistingFixture(t *testing.T) {
	source := os.Getenv("GYIT_PROGRESSIVE_SOURCE")
	if source == "" {
		t.Skip("set GYIT_PROGRESSIVE_SOURCE to import an existing packed fixture")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	root := t.TempDir()
	backend, err := store.NewLocal(filepath.Join(root, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	disk, err := store.NewDiskCache(nil, filepath.Join(root, "cache"), "progressive-test", 256<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	p, err := NewProgressive(ctx, backend, disk, root)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err = p.ImportPacks(ctx, source); err != nil {
		t.Fatal(err)
	}
	t.Logf("pack publication %s", time.Since(start))
	sha, err := exec.Command("git", "--git-dir="+source, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	id := strings.TrimSpace(string(sha))
	start = time.Now()
	if err = p.PrepareSnapshot(ctx, id); err != nil {
		t.Fatal(err)
	}
	t.Logf("metadata publication %s", time.Since(start))
	s, err := p.Open(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	start = time.Now()
	var visit func(string) error
	visit = func(tree string) error {
		after := ""
		for {
			entries, err := s.ReadDir(ctx, tree, after, 128)
			if err != nil {
				return err
			}
			if len(entries) == 0 {
				break
			}
			for _, e := range entries {
				count++
				if e.Mode == 0040000 {
					if err = visit(e.OID); err != nil {
						return err
					}
				}
			}
			after = entries[len(entries)-1].Name
		}
		return nil
	}
	if err = visit(s.Tree); err != nil {
		t.Fatal(err)
	}
	t.Logf("scan %d entries %s", count, time.Since(start))
	for _, name := range []string{"README", "Makefile", "kernel/sched/core.c", "include/linux/sched.h"} {
		e, err := s.Resolve(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		want, err := exec.Command("git", "--git-dir="+source, "show", id+":"+name).Output()
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, e.Size)
		if _, err = s.ReadAt(ctx, e.OID, got, 0); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("mismatch %s", name)
		}
	}
}

type progressiveCountStore struct {
	mu sync.Mutex
	store.Store
	bytes    int
	puts     int
	conflict bool
}

func (s *progressiveCountStore) Put(ctx context.Context, key string, b []byte, cond string) error {
	if key == "HEAD" && s.conflict {
		return store.ErrConflict
	}
	s.mu.Lock()
	s.bytes += len(b)
	s.puts++
	s.mu.Unlock()
	return s.Store.Put(ctx, key, b, cond)
}
func TestProgressiveIncrementalAndCAS(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	os.Mkdir(source, 0700)
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", source}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "-b", "main")
	for i := 0; i < 160; i++ {
		dir := filepath.Join(source, fmt.Sprintf("dir%03d", i))
		os.Mkdir(dir, 0700)
		os.WriteFile(filepath.Join(dir, "file"), bytes.Repeat([]byte(fmt.Sprintf("content %d\n", i)), 1000), 0600)
	}
	os.Symlink("dir000/file", filepath.Join(source, "link"))
	os.WriteFile(filepath.Join(source, "empty"), nil, 0600)
	git("add", ".")
	git("commit", "-qm", "initial")
	old := git("rev-parse", "HEAD")
	git("repack", "-ad")
	local, err := store.NewLocal(filepath.Join(root, "store"))
	if err != nil {
		t.Fatal(err)
	}
	backend := &progressiveCountStore{Store: local}
	p, err := NewProgressive(t.Context(), backend, nil, root)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.ImportPacks(t.Context(), filepath.Join(source, ".git")); err != nil {
		t.Fatal(err)
	}
	if err = p.PrepareSnapshot(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	before, err := p.Open(t.Context(), old)
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(source, "dir000/file"), []byte("changed\n"), 0600)
	git("commit", "-qam", "one file")
	newID := git("rev-parse", "HEAD")
	git("repack", "-d")
	if err = p.ImportPacks(t.Context(), filepath.Join(source, ".git")); err != nil {
		t.Fatal(err)
	}
	backend.bytes = 0
	backend.puts = 0
	if err = p.PrepareSnapshot(t.Context(), newID); err != nil {
		t.Fatal(err)
	}
	if backend.bytes > 256<<10 {
		t.Fatalf("one-file metadata update wrote %d bytes", backend.bytes)
	}
	t.Logf("one-file metadata: %d bytes, %d writes", backend.bytes, backend.puts)
	after, err := p.Open(t.Context(), newID)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []*Snapshot{before, after} {
		e, err := s.Resolve(t.Context(), "dir000/file")
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, e.Size)
		if _, err = s.ReadAt(t.Context(), e.OID, got, 0); err != nil {
			t.Fatal(err)
		}
		want, err := exec.Command("git", "-C", source, "show", s.SHA+":dir000/file").Output()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatal("pinned content changed")
		}
	}
	// A failed CAS must not install a new local root.
	oldRoot := p.index().root
	backend.conflict = true
	if err = p.SetHistoryError(t.Context(), newID, "failed publication test"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("CAS result: %v", err)
	}
	if p.index().root != oldRoot {
		t.Fatal("failed publication changed reader root")
	}
	reopened, err := NewProgressive(t.Context(), local, nil, root)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.index().root != oldRoot {
		t.Fatal("failed CAS changed durable root")
	}
}
