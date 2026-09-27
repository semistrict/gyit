package githubfs

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

func fixture(t *testing.T) (Options, string, string) {
	t.Helper()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) string {
		t.Helper()
		c := exec.Command("git", append([]string{"-C", source}, args...)...)
		c.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
		out, err := c.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "-b", "main")
	os.Mkdir(filepath.Join(source, "dir"), 0700)
	os.WriteFile(filepath.Join(source, "dir", "hello"), []byte("first revision\n"), 0600)
	os.WriteFile(filepath.Join(source, "NOTICE"), []byte("real repository notice\n"), 0600)
	git("add", ".")
	git("commit", "-qm", "first")
	first := git("rev-parse", "HEAD")
	git("checkout", "-qb", "feature/login")
	os.WriteFile(filepath.Join(source, "dir", "hello"), []byte("second revision\n"), 0600)
	git("commit", "-qam", "second")
	second := git("rev-parse", "HEAD")
	git("checkout", "-q", "main")
	remote := filepath.Join(root, "remotes", "acme")
	os.MkdirAll(remote, 0700)
	git("clone", "--bare", "--quiet", source, filepath.Join(remote, "project.git"))
	return Options{DataDir: filepath.Join(root, "data"), CacheDir: filepath.Join(root, "cache"), CacheBytes: 16 << 20, RemoteBase: "file://" + filepath.Join(root, "remotes")}, first, second
}
func openFixture(t *testing.T) (*FS, Options, string, string) {
	t.Helper()
	opts, a, b := fixture(t)
	f, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f, opts, a, b
}
func waitReady(t *testing.T, f *FS, path string) {
	t.Helper()
	if _, err := f.ReadDir(t.Context(), path, "", 128); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if f.Generation(path) > 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	b := make([]byte, 4096)
	n, _ := f.Read(t.Context(), path+"/NOTICE", b, 0)
	t.Fatalf("setup did not finish: %s", b[:n])
}
func read(t *testing.T, f *FS, path string) string {
	t.Helper()
	b := make([]byte, 4096)
	n, err := f.Read(t.Context(), path, b, 0)
	if err != nil {
		t.Fatal(err)
	}
	return string(b[:n])
}
func TestNoticeThenAtomicSnapshot(t *testing.T) {
	f, _, _, _ := openFixture(t)
	// Occupy setup slots to deterministically inspect the queued state.
	f.workers <- struct{}{}
	f.workers <- struct{}{}
	started := time.Now()
	entries, err := f.ReadDir(t.Context(), "acme/project", "", 128)
	if elapsed := time.Since(started); elapsed < 10*time.Second || elapsed > 12*time.Second {
		t.Fatalf("NOTICE deadline: %s", elapsed)
	}
	if err != nil || len(entries) != 1 || entries[0].Name != "NOTICE" {
		t.Fatalf("initial directory %v %v", entries, err)
	}
	if notice := read(t, f, "acme/project/NOTICE"); !strings.Contains(notice, "Queued") {
		t.Fatalf("notice: %s", notice)
	}
	if _, err = f.Lookup(t.Context(), "acme/project/dir/hello"); err == nil {
		t.Fatal("partial tree visible before setup")
	}
	if _, err = f.Lookup(t.Context(), "acme/project/.git/HEAD"); err == nil {
		t.Fatal("partial Git history visible before setup")
	}
	<-f.workers
	<-f.workers
	waitReady(t, f, "acme/project")
	if got := read(t, f, "acme/project/dir/hello"); got != "first revision\n" {
		t.Fatalf("file %q", got)
	}
	if got := read(t, f, "acme/project/NOTICE"); got != "real repository notice\n" {
		t.Fatalf("NOTICE was not replaced by real file: %q", got)
	}
	entries, err = f.ReadDir(t.Context(), "acme/project", "", 128)
	if err != nil || len(entries) != 3 {
		t.Fatalf("ready directory %v %v", entries, err)
	}
}

func TestNoticeReadShowsCurrentProgressAndElapsedTime(t *testing.T) {
	f, _, _, _ := openFixture(t)
	f.workers <- struct{}{}
	f.workers <- struct{}{}
	defer func() { <-f.workers; <-f.workers }()
	j, err := f.ensureJob(Target{Owner: "acme", Repository: "project"}, "acme/project")
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	j.noticeAfter = time.Now()
	f.mu.Unlock()
	first := read(t, f, "acme/project/NOTICE")
	if !strings.Contains(first, "Elapsed: ") {
		t.Fatalf("NOTICE lacks elapsed setup time: %q", first)
	}
	j.progress("Fetching history: 25%")
	time.Sleep(time.Second)
	second := read(t, f, "acme/project/NOTICE")
	if len(first) != len(second) {
		t.Fatalf("NOTICE changed size from %d to %d; mounted readers may show stale bytes", len(first), len(second))
	}
	if !strings.Contains(second, "Fetching history: 25%") || strings.Contains(second, "Queued for background setup") {
		t.Fatalf("NOTICE did not update progress: %q", second)
	}
	if !strings.Contains(second, "Elapsed: 00:00:01") {
		t.Fatalf("NOTICE did not update elapsed time: %q", second)
	}
	j.progress(strings.Repeat("fetching…", 200))
	long := read(t, f, "acme/project/NOTICE")
	if len(long) != len(first) || !utf8.ValidString(long) || !strings.Contains(long, "Elapsed: ") {
		t.Fatalf("long progress produced an invalid NOTICE: %q", long)
	}
	e, err := f.Lookup(t.Context(), "acme/project/NOTICE")
	if err != nil || e.Size != int64(len(long)) {
		t.Fatalf("NOTICE size = %d, %v; want %d", e.Size, err, len(long))
	}
}
func TestRevisionPathsAndReuse(t *testing.T) {
	f, opts, first, second := openFixture(t)
	paths := []string{"acme/project@" + first, "acme/project@feature%2Flogin", "acme/project@" + second}
	var wg sync.WaitGroup
	for _, p := range paths {
		wg.Go(func() {
			if _, err := f.Lookup(t.Context(), p); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	for i, p := range paths {
		waitReady(t, f, p)
		want := "second revision\n"
		if i == 0 {
			want = "first revision\n"
		}
		if got := read(t, f, p+"/dir/hello"); got != want {
			t.Fatalf("%s: %q", p, got)
		}
	}
	f.Close()
	// Explicit commits remain usable from prepared data without their remote.
	os.RemoveAll(strings.TrimPrefix(opts.RemoteBase, "file://"))
	again, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	again.Lookup(t.Context(), paths[0])
	waitReady(t, again, paths[0])
	if read(t, again, paths[0]+"/dir/hello") != "first revision\n" {
		t.Fatal("prepared snapshot changed")
	}
}
func TestSetupFailureRemainsNotice(t *testing.T) {
	f, _, _, _ := openFixture(t)
	path := "acme/missing"
	f.ReadDir(t.Context(), path, "", 128)
	_, target, _ := parse(path)
	f.mu.Lock()
	j := f.jobs[target.Key()]
	f.mu.Unlock()
	select {
	case <-j.done:
	case <-time.After(5 * time.Second):
		t.Fatal("failed setup did not finish")
	}
	entries, err := f.ReadDir(t.Context(), path, "", 128)
	if err != nil || len(entries) != 1 || entries[0].Name != "NOTICE" {
		t.Fatalf("failure directory %v %v", entries, err)
	}
	if got := read(t, f, path+"/NOTICE"); !strings.Contains(got, "Setup failed") {
		t.Fatal(got)
	}
	if f.Generation(path) != 1 {
		t.Fatal("failed setup published a snapshot")
	}
}
func TestRevisionValidation(t *testing.T) {
	for _, path := range []string{"../repo", "org/repo@", "org/repo@%zz", "org/repo@%00", "org/repo@-bad", "org/repo/../file"} {
		if _, _, err := parse(path); err == nil {
			t.Errorf("accepted %q", path)
		}
	}
	_, target, err := parse("Org/Repo@feature%2FLogin/file")
	if err != nil || target.Revision != "feature/Login" || target.Repository != "repo" {
		t.Fatalf("parse: %+v %v", target, err)
	}
}
func TestRootDoesNotStartSetup(t *testing.T) {
	f, _, _, _ := openFixture(t)
	entries, err := f.ReadDir(context.Background(), "", "", 128)
	if err != nil || len(entries) != 0 || len(f.jobs) != 0 {
		t.Fatalf("root %v %v", entries, err)
	}
}

func TestQuickSetupReturnsFilesWithoutPlaceholder(t *testing.T) {
	f, _, _, _ := openFixture(t)
	started := time.Now()
	entries, err := f.ReadDir(t.Context(), "acme/project", "", 128)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(started) >= 10*time.Second {
		t.Fatal("small local import exceeded synchronous window")
	}
	if len(entries) != 3 || f.Generation("acme/project") != 2 {
		t.Fatalf("not fully published: %v", entries)
	}
	if got := read(t, f, "acme/project/NOTICE"); got != "real repository notice\n" {
		t.Fatalf("placeholder exposed: %q", got)
	}
}

func TestSetupWaitHonorsCancellation(t *testing.T) {
	f, _, _, _ := openFixture(t)
	f.workers <- struct{}{}
	f.workers <- struct{}{}
	defer func() { <-f.workers; <-f.workers }()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := f.ReadDir(ctx, "acme/project", "", 128); err != context.DeadlineExceeded {
		t.Fatalf("read: %v", err)
	}
}
