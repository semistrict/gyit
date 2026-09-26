package githubfs

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gyit/internal/repo"
	"gyit/internal/store"
)

func (f *FS) git(ctx context.Context, dir string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	env := []string{}
	for _, v := range os.Environ() {
		key, _, _ := strings.Cut(v, "=")
		if !strings.HasPrefix(key, "GIT_") {
			env = append(env, v)
		}
	}
	cmd.Env = append(env, "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_NO_REPLACE_OBJECTS=1")
	if f.opts.Token != "" && f.opts.RemoteBase == "https://github.com" {
		auth := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + f.opts.Token))
		cmd.Env = append(cmd.Env, "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=http.https://github.com/.extraheader", "GIT_CONFIG_VALUE_0=Authorization: Basic "+auth)
	}
	cmd.WaitDelay = 5 * time.Second
	return cmd
}
func fullSHA(s string) bool {
	if len(s) != 40 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}
func (f *FS) resolve(ctx context.Context, remote string, t Target) (string, error) {
	if fullSHA(t.Revision) {
		return strings.ToLower(t.Revision), nil
	}
	refs := []string{"HEAD"}
	if t.Revision != "" {
		refs = []string{"refs/heads/" + t.Revision, "refs/tags/" + t.Revision + "^{}", "refs/tags/" + t.Revision}
	}
	args := append([]string{"ls-remote", "--exit-code", remote}, refs...)
	out, err := f.git(ctx, f.opts.DataDir, args...).Output()
	if err != nil {
		var failure *exec.ExitError
		if errors.As(err, &failure) {
			return "", fmt.Errorf("resolve revision: %w: %s", err, f.redact(strings.TrimSpace(string(failure.Stderr))))
		}
		return "", fmt.Errorf("resolve revision: %w", err)
	}
	found := map[string]string{}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fullSHA(fields[0]) {
			found[fields[1]] = fields[0]
		}
	}
	for _, ref := range refs {
		if sha := found[ref]; sha != "" {
			return sha, nil
		}
	}
	return "", fmt.Errorf("revision not found")
}

type progressWriter struct {
	mu     sync.Mutex
	update func(string)
	last   time.Time
}

func (w *progressWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if time.Since(w.last) > 250*time.Millisecond {
		line := strings.TrimSpace(strings.ReplaceAll(string(b), "\r", "\n"))
		if len(line) > 1500 {
			line = line[len(line)-1500:]
		}
		w.update("Fetching complete repository history through Git…\n" + line)
		w.last = time.Now()
	}
	return len(b), nil
}
func (f *FS) open(ctx context.Context, path string) (*repo.Repository, *repo.Snapshot, error) {
	s, err := store.NewLocal(path)
	if err != nil {
		return nil, nil, err
	}
	r, err := repo.NewSharedDisk(s, f.cache)
	if err != nil {
		return nil, nil, err
	}
	snapshot, err := r.OpenRevision(ctx, "HEAD", "")
	if err != nil {
		r.Close()
		return nil, nil, err
	}
	return r, snapshot, nil
}
func (f *FS) prepare(ctx context.Context, t Target, progress func(string)) (*repo.Repository, *repo.Snapshot, error) {
	remote := strings.TrimRight(f.opts.RemoteBase, "/") + "/" + t.Owner + "/" + t.Repository + ".git"
	progress("Resolving revision…")
	sha, err := f.resolve(ctx, remote, t)
	if err != nil {
		return nil, nil, err
	}
	final := filepath.Join(f.opts.DataDir, "repositories-v1", storeID(t, sha))
	if _, err = os.Stat(filepath.Join(final, "HEAD")); err == nil {
		progress("Opening prepared snapshot…")
		return f.open(ctx, final)
	}
	staging, err := os.MkdirTemp(f.opts.DataDir, ".setup-")
	if err != nil {
		return nil, nil, err
	}
	defer os.RemoveAll(staging)
	source := filepath.Join(staging, "source.git")
	if err = os.Mkdir(source, 0700); err != nil {
		return nil, nil, err
	}
	if out, err := f.git(ctx, source, "init", "--bare", "--quiet").CombinedOutput(); err != nil {
		return nil, nil, fmt.Errorf("initialize snapshot: %w: %s", err, out)
	}
	cmd := f.git(ctx, source, "fetch", "--tags", "--force", "--progress", remote, sha, "+refs/heads/*:refs/heads/*")
	cmd.Stderr = &progressWriter{update: progress}
	if err = cmd.Run(); err != nil {
		return nil, nil, fmt.Errorf("fetch snapshot: %w", err)
	}
	if out, err := f.git(ctx, source, "rev-parse", "FETCH_HEAD").Output(); err != nil || !bytes.Equal(bytes.TrimSpace(out), []byte(sha)) {
		return nil, nil, fmt.Errorf("fetched revision did not match requested commit")
	}
	// Never publish an incomplete graph, including when the source is shallow.
	if out, err := f.git(ctx, source, "rev-parse", "--is-shallow-repository").Output(); err != nil || strings.TrimSpace(string(out)) != "false" {
		return nil, nil, fmt.Errorf("complete repository history is required")
	}
	// Detach HEAD at the requested commit while retaining every fetched branch.
	if out, err := f.git(ctx, source, "update-ref", "--no-deref", "HEAD", sha).CombinedOutput(); err != nil {
		return nil, nil, fmt.Errorf("pin revision: %w: %s", err, out)
	}
	progress("Preparing local repository data…")
	build := filepath.Join(staging, "store")
	backend, err := store.NewLocal(build)
	if err != nil {
		return nil, nil, err
	}
	_, err = repo.Import(ctx, backend, repo.ImportOptions{Repo: source, TempDir: staging, Progress: func(s repo.Stats) {
		progress(fmt.Sprintf("Preparing local repository data…\n%d objects, %d MiB processed, %d MiB stored", s.Objects, s.Bytes>>20, s.UploadedBytes>>20))
	}})
	if err != nil {
		return nil, nil, fmt.Errorf("prepare snapshot: %w", err)
	}
	if err = os.MkdirAll(filepath.Dir(final), 0700); err != nil {
		return nil, nil, err
	}
	// Other aliases of the same revision can finish concurrently. Reuse an
	// already-published immutable snapshot and discard this staging directory.
	if err = os.Rename(build, final); err != nil {
		if _, statErr := os.Stat(filepath.Join(final, "HEAD")); statErr != nil {
			return nil, nil, err
		}
	}
	return f.open(ctx, final)
}

func (f *FS) redact(s string) string {
	if token := f.opts.Token; token != "" {
		s = strings.ReplaceAll(s, token, "[redacted]")
		s = strings.ReplaceAll(s, base64.StdEncoding.EncodeToString([]byte("x-access-token:"+token)), "[redacted]")
	}
	return s
}
