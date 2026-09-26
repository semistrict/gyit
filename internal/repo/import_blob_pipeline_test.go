package repo

import (
	"bytes"
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gyit/internal/store"
)

// Enough small blobs to activate bulk import, interleaved with histories that
// cross the chunk boundary. Git supplies all object IDs and expected output.
func blobPipelineFixture(t testing.TB, format string) string {
	t.Helper()
	source := t.TempDir()
	if out, err := git(t.Context(), source, "init", "--bare", "-q", "-b", "main", "--object-format="+format).CombinedOutput(); err != nil {
		t.Fatalf("init: %v %s", err, out)
	}
	var input bytes.Buffer
	big := make([]byte, ChunkSize+127)
	rand.New(rand.NewSource(4871)).Read(big)
	for revision := 0; revision < 3; revision++ {
		fmt.Fprintf(&input, "commit refs/heads/main\ncommitter Test <test@example.test> %d +0000\ndata 7\nfixture\n", 1700000000+revision)
		if revision == 0 {
			for i := 0; i < 1100; i++ {
				content := fmt.Sprintf("contents %04d\n", i)
				fmt.Fprintf(&input, "M 100644 inline file-%04d\ndata %d\n%s", i, len(content), content)
			}
			input.WriteString("M 100644 inline empty\ndata 0\n\n")
		}
		big[revision*100] ^= 0x71
		fmt.Fprintf(&input, "M 100644 inline large\ndata %d\n", len(big))
		input.Write(big)
		input.WriteByte('\n')
		input.WriteByte('\n')
	}
	cmd := git(t.Context(), source, "fast-import", "--quiet")
	cmd.Stdin = &input
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fixture: %v %s", err, out)
	}
	return source
}

type parallelBlobStore struct {
	store.Store
	mu              sync.Mutex
	active, arrived int
	second          chan struct{}
	failure         error
}

func (s *parallelBlobStore) Put(ctx context.Context, key string, data []byte, condition string) error {
	if !strings.HasPrefix(key, "packs/") {
		return s.Store.Put(ctx, key, data, condition)
	}
	s.mu.Lock()
	s.arrived++
	ticket := s.arrived
	s.active++
	if ticket == 2 {
		close(s.second)
	}
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.active--; s.mu.Unlock() }()
	select {
	case <-s.second:
	case <-ctx.Done():
		return ctx.Err()
	}
	if s.failure != nil {
		if ticket == 1 {
			return s.failure
		}
		<-ctx.Done()
		return ctx.Err()
	}
	return s.Store.Put(ctx, key, data, condition)
}
func TestImportParallelBlobSources(t *testing.T) {
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			source := blobPipelineFixture(t, format)
			local, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			storage := &parallelBlobStore{Store: local, second: make(chan struct{})}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			scratch := t.TempDir()
			stats, err := Import(ctx, storage, ImportOptions{Repo: source, DeltaDepth: 1, TempDir: scratch, CompressionWorkers: 4})
			if err != nil {
				t.Fatal(err)
			}
			if stats.Blobs != 1104 || stats.Chunks != 1106 {
				t.Fatalf("unexpected counts: %+v", stats)
			}
			r, _ := New(local, 32<<20)
			for _, rev := range []string{"HEAD", "HEAD~1", "HEAD~2"} {
				sha := command(t, source, "rev-parse", rev)
				snap, err := r.OpenRevision(t.Context(), sha, "")
				if err != nil {
					t.Fatal(err)
				}
				checkObjectViewParity(t, t.Context(), r, snap, source, "", []string{"ls-tree", "-r", "-l", sha})
				for _, path := range []string{"large", "empty", "file-0000", "file-1099"} {
					checkObjectViewParity(t, t.Context(), r, snap, source, "", []string{"cat-file", "-p", sha + ":" + path})
				}
			}
			again, err := Import(t.Context(), local, ImportOptions{Repo: source, DeltaDepth: 1, TempDir: scratch})
			if err != nil || again.Objects != 0 {
				t.Fatalf("unchanged import: %+v %v", again, err)
			}
		})
	}
}
func TestImportBlobFailureJoinsWriters(t *testing.T) {
	source := blobPipelineFixture(t, "sha1")
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("blob upload failed")
	storage := &parallelBlobStore{Store: local, second: make(chan struct{}), failure: injected}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := Import(ctx, storage, ImportOptions{Repo: source, DeltaDepth: 1, CompressionWorkers: 4}); !errors.Is(err, injected) {
		t.Fatalf("lost writer error: %v", err)
	}
	storage.mu.Lock()
	active := storage.active
	storage.mu.Unlock()
	if active != 0 {
		t.Fatalf("import returned with %d active writers", active)
	}
	if _, _, err := local.Get(t.Context(), "HEAD", 0, -1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("failed import published: %v", err)
	}
}

func TestImportParallelBlobCancellation(t *testing.T) {
	source := blobPipelineFixture(t, "sha1")
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err = Import(ctx, local, ImportOptions{Repo: source, DeltaDepth: 1, TempDir: scratch, CompressionWorkers: 4, Progress: func(stats Stats) {
		if stats.Objects >= 100 {
			cancel()
		}
	}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if _, _, err := local.Get(t.Context(), "HEAD", 0, -1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("canceled import published: %v", err)
	}
	if files, err := os.ReadDir(scratch); err != nil || len(files) != 0 {
		t.Fatalf("staging cleanup: %v %v", files, err)
	}
}
func TestImportParallelBlobVerifiesSource(t *testing.T) {
	source := blobPipelineFixture(t, "sha1")
	create := git(t.Context(), source, "hash-object", "-w", "--stdin")
	create.Stdin = strings.NewReader("before\n")
	out, err := create.Output()
	if err != nil {
		t.Fatal(err)
	}
	oid := strings.TrimSpace(string(out))
	head := command(t, source, "rev-parse", "HEAD")
	appendCommit := git(t.Context(), source, "fast-import", "--quiet")
	appendCommit.Stdin = strings.NewReader("commit refs/heads/main\ncommitter Test <test@example.test> 1700000004 +0000\ndata 7\nfixture\nfrom " + head + "\nM 100644 " + oid + " verify\n\n")
	if out, err := appendCommit.CombinedOutput(); err != nil {
		t.Fatalf("append fixture: %v %s", err, out)
	}
	var corrupt bytes.Buffer
	z := zlib.NewWriter(&corrupt)
	z.Write([]byte("blob 7\x00after!\n"))
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	var mutationErr error
	_, err = Import(t.Context(), local, ImportOptions{Repo: source, DeltaDepth: 1, CompressionWorkers: 4, Progress: func(stats Stats) {
		if stats.Phase == "objects" {
			once.Do(func() {
				path := filepath.Join(source, "objects", oid[:2], oid[2:])
				mutationErr = os.Chmod(path, 0600)
				if mutationErr == nil {
					mutationErr = os.WriteFile(path, corrupt.Bytes(), 0600)
				}
			})
		}
	}})
	if mutationErr != nil {
		t.Fatal(mutationErr)
	}
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("source integrity: %v", err)
	}
	if _, _, err := local.Get(t.Context(), "HEAD", 0, -1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("corrupt source published: %v", err)
	}
}
func BenchmarkImportBlobPipeline(b *testing.B) {
	source := blobPipelineFixture(b, "sha1")
	for _, workers := range []int{1, 4} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			root := b.TempDir()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				local, err := store.NewLocal(filepath.Join(root, fmt.Sprint(i)))
				if err != nil {
					b.Fatal(err)
				}
				if _, err := Import(b.Context(), local, ImportOptions{Repo: source, DeltaDepth: 1, CompressionWorkers: workers}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
