package repo

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"gyit/internal/store"
	"golang.org/x/sys/unix"
)

// TestMediumRepository reuses an ignored, complete bare clone. Each invocation
// creates a new object store so a previous import cannot hide importer failures.
// First run: GYIT_MEDIUM_TEST=1 GYIT_CLONE_REPO=owner/repository go test ...
// Later runs need only GYIT_MEDIUM_TEST=1; the cached source is never fetched.
func TestMediumRepository(t *testing.T) {
	if os.Getenv("GYIT_MEDIUM_TEST") != "1" {
		t.Skip("set GYIT_MEDIUM_TEST=1; first run also requires GYIT_CLONE_REPO=owner/repository")
	}
	ctx := t.Context()
	if deadline, ok := t.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline.Add(-15*time.Second))
		defer cancel()
	}
	source := mediumSource(t, ctx)
	sha := mediumGit(t, ctx, source, "rev-parse", "HEAD")
	commits, err := strconv.ParseInt(mediumGit(t, ctx, source, "rev-list", "--all", "--count"), 10, 64)
	if err != nil || commits < 2 {
		t.Fatalf("fixture must have at least two commits: count=%d err=%v", commits, err)
	}
	objectDir := filepath.Join(t.TempDir(), "objects")
	backend, err := store.NewLocal(objectDir)
	if err != nil {
		t.Fatal(err)
	}
	maxDuration := 5 * time.Minute
	if text := os.Getenv("GYIT_IMPORT_MAX_DURATION"); text != "" {
		maxDuration, err = time.ParseDuration(text)
		if err != nil || maxDuration <= 0 {
			t.Fatal("GYIT_IMPORT_MAX_DURATION must be a positive duration")
		}
	}
	workers := min(4, runtime.GOMAXPROCS(0))
	if text := os.Getenv("GYIT_IMPORT_WORKERS"); text != "" {
		if text == "numcpu" {
			workers = runtime.NumCPU()
		} else {
			workers, err = strconv.Atoi(text)
			if err != nil || workers < 1 {
				t.Fatal("GYIT_IMPORT_WORKERS must be positive or numcpu")
			}
		}
	}
	depth, candidates := 1, 4
	if value := os.Getenv("GYIT_DELTA_DEPTH"); value != "" {
		depth, err = strconv.Atoi(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	if value := os.Getenv("GYIT_DELTA_CANDIDATES"); value != "" {
		candidates, err = strconv.Atoi(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("compression: %s; workers=%d depth=%d candidates=%d", compressorName, workers, depth, candidates)
	started, lastProgress := time.Now(), time.Now()
	lastPhase := ""
	stats, err := Import(ctx, backend, ImportOptions{Repo: source, DisableDeltas: os.Getenv("GYIT_NO_DELTAS") == "1", CompressionWorkers: workers, DeltaDepth: depth, DeltaCandidates: candidates, Progress: func(s Stats) {
		if s.Phase != lastPhase || time.Since(lastProgress) >= 15*time.Second {
			t.Logf("import progress: elapsed=%s phase=%s objects=%d raw_MiB=%d uploaded_MiB=%d", time.Since(started).Round(time.Millisecond), s.Phase, s.Objects, s.Bytes>>20, s.UploadedBytes>>20)
			lastPhase = s.Phase
			lastProgress = time.Now()
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > maxDuration {
		t.Errorf("import exceeded %s target: %s", maxDuration, elapsed)
	}
	if stats.Objects < commits || stats.Blobs == 0 {
		t.Fatalf("incomplete import: %+v; source commits=%d", stats, commits)
	}
	t.Logf("import: %s; commits=%d objects=%d blobs=%d raw_bytes=%d pack_bytes=%d", time.Since(started), commits, stats.Objects, stats.Blobs, stats.Bytes, stats.UploadedBytes)
	t.Logf("chunks=%d deltas=%d max_depth=%d", stats.Chunks, stats.DeltaChunks, stats.MaxDepth)
	if stats.MaxDepth > depth {
		t.Fatal("import exceeded configured delta depth")
	}
	measureStoreSize(t, objectDir)

	measured := &countedStore{Store: backend}
	r, err := New(measured, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	root := mediumTree(t, ctx, source, sha, false)
	started = time.Now()
	snapshot, err := r.Open(ctx, sha)
	if err != nil {
		t.Fatal(err)
	}
	after, seen := "", 0
	for {
		entries, err := snapshot.ReadDir(ctx, snapshot.Tree, after, 128)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			expected, ok := root[entry.Name]
			if !ok || entry.OID != expected.OID || entry.Mode != expected.Mode {
				t.Fatalf("root entry mismatch: %q", entry.Name)
			}
			if entry.Name <= after {
				t.Fatal("directory names out of order")
			}
			after = entry.Name
			seen++
		}
		if len(entries) < 128 {
			break
		}
	}
	if seen != len(root) {
		t.Fatalf("root listing has %d entries; Git has %d", seen, len(root))
	}
	// Bounds are on I/O, not wall-clock timing, so slow CI does not cause flakes.
	if measured.packGets != 0 || measured.gets > 12+(len(root)/128)*4 || measured.bytes > 2<<20 {
		t.Fatalf("root listing fetched excessive data: GETs=%d bytes=%d pack_GETs=%d", measured.gets, measured.bytes, measured.packGets)
	}
	t.Logf("cold open + root listing: %s; entries=%d GETs=%d bytes=%d pack_GETs=%d", time.Since(started), seen, measured.gets, measured.bytes, measured.packGets)

	// Verify every HEAD path's metadata, then stream a deterministic sample of
	// blobs and compare their Git object hashes, including nested file paths.
	files := mediumTree(t, ctx, source, sha, true)
	checked := 0
	format := mediumGit(t, ctx, source, "rev-parse", "--show-object-format")
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	// Git tree order differs around directories; sort explicitly for stable samples.
	slices.Sort(paths)
	stride := max(1, len(paths)/16)
	for i, path := range paths {
		expected := files[path]
		entry, err := snapshot.Resolve(ctx, path)
		if err != nil {
			t.Fatalf("resolve %q: %v", path, err)
		}
		if entry.OID != expected.OID || entry.Mode != expected.Mode {
			t.Fatalf("metadata mismatch: %q", path)
		}
		if i%stride == 0 && entry.Mode != 0160000 {
			mediumCheckBlob(t, ctx, snapshot, entry, format)
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("no file contents were verified")
	}
	measureMediumReads(t, ctx, backend, snapshot, paths, files)
	t.Logf("verified %d file paths and %d complete blob hashes", len(files), checked)

	// Reopen a historical commit with a cold cache. This validates that the
	// importer retained history and that checkout opening does not download it.
	older := mediumGit(t, ctx, source, "rev-parse", "HEAD~1")
	r, err = New(measured, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	measured.reset()
	old, err := r.Open(ctx, older)
	if err != nil {
		t.Fatal(err)
	}
	if old.Tree != mediumGit(t, ctx, source, "rev-parse", older+"^{tree}") {
		t.Fatal("historical checkout has the wrong tree")
	}
	if measured.packGets != 0 || measured.gets > 8 || measured.bytes > 1<<20 {
		t.Fatal("historical checkout downloaded excessive data")
	}

	started = time.Now()
	repeated, err := Import(ctx, backend, ImportOptions{Repo: source})
	if err != nil {
		t.Fatal(err)
	}
	if repeated.Objects != 0 || repeated.UploadedBytes != 0 || repeated.Generation != stats.Generation {
		t.Fatalf("unchanged reimport did extra work or changed generation: %+v", repeated)
	}
	t.Logf("unchanged reimport: %s; no new objects or packs", time.Since(started))
}

func mediumSource(t *testing.T, ctx context.Context) string {
	t.Helper()
	// go test runs in the package directory; the cache belongs to this checkout.
	cache, err := filepath.Abs(filepath.Join("..", "..", ".testdata"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cache, 0755); err != nil {
		t.Fatal(err)
	}
	lock, err := os.OpenFile(filepath.Join(cache, "medium-repo.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	for {
		err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if err != unix.EWOULDBLOCK {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	source := filepath.Join(cache, "medium-repo.git")
	if _, err := os.Stat(source); os.IsNotExist(err) {
		target := os.Getenv("GYIT_CLONE_REPO")
		if !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*/[A-Za-z0-9][A-Za-z0-9_.-]*$`).MatchString(target) {
			t.Fatal("first run requires GYIT_CLONE_REPO=owner/repository")
		}
		// Rename only after a successful clone. A failed download cannot become
		// a cached fixture, and concurrent test runs cannot race its creation.
		staging, err := os.MkdirTemp(cache, ".clone-*")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(staging)
		clone := filepath.Join(staging, "repo.git")
		t.Log("cloning complete repository into ignored fixture cache")
		mediumRun(t, ctx, "gh", "repo", "clone", target, clone, "--", "--bare", "--quiet")
		if err := os.Rename(clone, source); err != nil {
			t.Fatal(err)
		}
	} else if err != nil {
		t.Fatal(err)
	} else {
		t.Log("reusing existing fixture clone without fetching")
	}
	if mediumGit(t, ctx, source, "rev-parse", "--is-bare-repository") != "true" || mediumGit(t, ctx, source, "rev-parse", "--is-shallow-repository") != "false" {
		t.Fatal("cached fixture must be a complete bare repository")
	}
	// Changing the requested repository must not silently test the old fixture.
	if requested := os.Getenv("GYIT_CLONE_REPO"); requested != "" {
		remote := mediumGit(t, ctx, source, "remote", "get-url", "origin")
		remote = strings.TrimSuffix(remote, ".git")
		if !strings.HasSuffix(strings.ToLower(remote), "github.com/"+strings.ToLower(requested)) && !strings.HasSuffix(strings.ToLower(remote), "github.com:"+strings.ToLower(requested)) {
			t.Fatal("cached clone is for another repository; move it aside before selecting a different fixture")
		}
	}
	return source
}

func mediumRun(t *testing.T, ctx context.Context, executable string, args ...string) []byte {
	t.Helper()
	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Env = append(os.Environ(), "GH_PROMPT_DISABLED=1", "GIT_TERMINAL_PROMPT=0", "GIT_NO_LAZY_FETCH=1", "GIT_NO_REPLACE_OBJECTS=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 5 * time.Second
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("fixture command failed: %v: %s", err, stderr.String())
	}
	return out
}

func mediumGit(t *testing.T, ctx context.Context, source string, args ...string) string {
	t.Helper()
	return strings.TrimSpace(string(mediumRun(t, ctx, "git", append([]string{"-C", source}, args...)...)))
}

func mediumTree(t *testing.T, ctx context.Context, source, sha string, recursive bool) map[string]Entry {
	t.Helper()
	args := []string{"-C", source, "ls-tree", "-z"}
	if recursive {
		args = append(args, "-r")
	}
	args = append(args, sha)
	data := mediumRun(t, ctx, "git", args...)
	entries := make(map[string]Entry)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if i := bytes.IndexByte(data, 0); i >= 0 {
			return i + 1, data[:i], nil
		}
		if atEOF && len(data) > 0 {
			return 0, nil, io.ErrUnexpectedEOF
		}
		return 0, nil, nil
	})
	for scanner.Scan() {
		fields, path, ok := strings.Cut(scanner.Text(), "\t")
		parts := strings.Fields(fields)
		if !ok || len(parts) != 3 {
			t.Fatal("invalid git ls-tree output")
		}
		mode, err := strconv.ParseUint(parts[0], 8, 32)
		if err != nil {
			t.Fatal(err)
		}
		entries[path] = Entry{Name: path, Mode: uint32(mode), OID: parts[2]}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return entries
}

func mediumCheckBlob(t *testing.T, ctx context.Context, snapshot *Snapshot, entry Entry, format string) {
	t.Helper()
	var h hash.Hash = sha1.New()
	if format == "sha256" {
		h = sha256.New()
	}
	fmt.Fprintf(h, "blob %d\x00", entry.Size)
	buf := make([]byte, ChunkSize)
	for off := int64(0); off < entry.Size; {
		want := min(int64(len(buf)), entry.Size-off)
		n, err := snapshot.ReadAt(ctx, entry.OID, buf[:want], off)
		if err != nil || int64(n) != want {
			t.Fatalf("blob read %s: n=%d err=%v", entry.OID, n, err)
		}
		h.Write(buf[:n])
		off += int64(n)
	}
	if hex.EncodeToString(h.Sum(nil)) != entry.OID {
		t.Fatalf("blob content or size differs from Git: %s", entry.OID)
	}
}

// TestDeltaMatrix compares fresh destinations against the same cached history.
// Runs sequentially to keep import timings free of competing benchmark work.
func TestDeltaMatrix(t *testing.T) {
	if os.Getenv("GYIT_DELTA_MATRIX") != "1" {
		t.Skip("set GYIT_DELTA_MATRIX=1 to measure base selection and depths")
	}
	t.Setenv("GYIT_MEDIUM_TEST", "1")
	t.Setenv("GYIT_NO_DELTAS", "0")
	for _, opt := range []struct{ depth, candidates int }{{1, 1}, {1, 4}, {2, 4}, {4, 4}, {8, 4}} {
		t.Run(fmt.Sprintf("depth_%d_candidates_%d", opt.depth, opt.candidates), func(t *testing.T) {
			t.Setenv("GYIT_DELTA_DEPTH", strconv.Itoa(opt.depth))
			t.Setenv("GYIT_DELTA_CANDIDATES", strconv.Itoa(opt.candidates))
			TestMediumRepository(t)
		})
	}
}
