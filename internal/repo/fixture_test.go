package repo

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"gyit/internal/store"
)

func command(t *testing.T, dir string, args ...string) string {
	t.Helper()
	c := exec.Command("git", append([]string{"-C", dir}, args...)...)
	c.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
	b, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, b)
	}
	return strings.TrimSpace(string(b))
}
func write(t *testing.T, dir, name string, b []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), b, 0644); err != nil {
		t.Fatal(err)
	}
}
func commit(t *testing.T, dir string) string {
	t.Helper()
	command(t, dir, "add", ".")
	command(t, dir, "commit", "-qm", "fixture")
	return command(t, dir, "rev-parse", "HEAD")
}

type countedStore struct {
	store.Store
	mu                    sync.Mutex
	gets, packGets, bytes int
	failHead              bool
}

func (s *countedStore) Get(ctx context.Context, k string, o, n int64) ([]byte, string, error) {
	b, v, e := s.Store.Get(ctx, k, o, n)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	s.bytes += len(b)
	if strings.HasPrefix(k, "packs/") {
		s.packGets++
	}
	return b, v, e
}
func (s *countedStore) Put(ctx context.Context, k string, b []byte, c string) error {
	if k == "HEAD" && s.failHead {
		return errors.New("injected publication failure")
	}
	return s.Store.Put(ctx, k, b, c)
}
func (s *countedStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets = 0
	s.packGets = 0
	s.bytes = 0
}
