package githubfs

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGlobalCacheBudgetPreservesRepositoryStore(t *testing.T) {
	opts, first, second := fixture(t)
	opts.CacheBytes = 16 << 10
	root := filepath.Dir(opts.DataDir)
	source := filepath.Join(root, "source")
	// A second repository with enough distinct decoded blobs to force eviction.
	for i := range 24 {
		body := strings.Repeat(fmt.Sprintf("file %02d\n", i), 1024)
		if err := os.WriteFile(filepath.Join(source, fmt.Sprintf("file-%02d", i)), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	git := func(args ...string) string {
		t.Helper()
		c := exec.Command("git", append([]string{"-C", source}, args...)...)
		c.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
		out, err := c.CombinedOutput()
		if err != nil {
			t.Fatalf("git: %v: %s", err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("add", ".")
	git("commit", "-qm", "cache fixture")
	third := git("rev-parse", "HEAD")
	git("clone", "--bare", "--quiet", source, filepath.Join(root, "remotes", "acme", "other.git"))
	paths := []string{"acme/project@" + first, "acme/project@" + second, "acme/other@" + third}
	f, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	for _, p := range paths {
		if _, err := f.Lookup(t.Context(), p); err != nil {
			t.Fatal(err)
		}
		waitReady(t, f, p)
		waitHistory(t, f, p)
	}
	before := storeContents(t, opts.DataDir)
	for round := range 2 {
		for i := range 24 {
			path := fmt.Sprintf("%s/file-%02d", paths[2], i)
			buf := make([]byte, 8192)
			n, err := f.Read(t.Context(), path, buf, 0)
			want := strings.Repeat(fmt.Sprintf("file %02d\n", i), 1024)
			if err != nil || string(buf[:n]) != want {
				t.Fatalf("round %d read %s: %v", round, path, err)
			}
			if used := cacheAllocation(t, opts.CacheDir); used > opts.CacheBytes {
				t.Fatalf("combined cache %d exceeds %d", used, opts.CacheBytes)
			}
		}
		for i, p := range paths[:2] {
			want := []string{"first revision\n", "second revision\n"}[i]
			if got := read(t, f, p+"/dir/hello"); got != want {
				t.Fatalf("revision read: %q", got)
			}
		}
	}
	if got := storeContents(t, opts.DataDir); !maps.Equal(got, before) {
		t.Fatalf("cache activity modified durable repository data: before %d objects, after %d", len(before), len(got))
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	// Cache deletion and zero cache capacity must not lose imported repositories.
	if err := os.RemoveAll(opts.CacheDir); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(root, "remotes")); err != nil {
		t.Fatal(err)
	}
	opts.CacheBytes = 0
	again, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	for _, p := range paths {
		if _, err := again.Lookup(t.Context(), p); err != nil {
			t.Fatal(err)
		}
		waitReady(t, again, p)
		if got := read(t, again, p+"/dir/hello"); got == "" {
			t.Fatal("durable file lost with cache")
		}
	}
	if used := cacheAllocation(t, opts.CacheDir); used != 0 {
		t.Fatalf("disabled cache uses %d bytes", used)
	}
	if got := storeContents(t, opts.DataDir); !maps.Equal(got, before) {
		t.Fatal("durable repository data changed after cache removal")
	}
}

func storeContents(t *testing.T, root string) map[string][32]byte {
	t.Helper()
	result := map[string][32]byte{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if strings.HasSuffix(entry.Name(), ".git") {
				return filepath.SkipDir
			}
			return nil
		}
		// Acquisition state and staging databases are not published objects.
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) < 4 || parts[0] != progressiveDirectory || parts[2] != "objects" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		result[strings.TrimPrefix(path, root)] = sha256.Sum256(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func cacheAllocation(t *testing.T, root string) int64 {
	t.Helper()
	var total int64
	// Eviction runs concurrently; an entry removed mid-walk occupies no space.
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() == "lock" {
			return nil
		}
		info, err := entry.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		total += (info.Size() + 4095) / 4096 * 4096
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return total
}
