package githubfs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

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
			if failure.ExitCode() == 2 {
				return "", fmt.Errorf("revision %q: %w", t.Revision, store.ErrNotFound)
			}
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
func (f *FS) redact(s string) string {
	if token := f.opts.Token; token != "" {
		s = strings.ReplaceAll(s, token, "[redacted]")
		s = strings.ReplaceAll(s, base64.StdEncoding.EncodeToString([]byte("x-access-token:"+token)), "[redacted]")
	}
	return s
}
