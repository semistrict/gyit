package githubfs

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
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

// Refresh references only when mounting/updating, never to disambiguate a log path.
func (f *FS) resolve(ctx context.Context, p *progressiveRepository, t Target) (string, error) {
	if fullSHA(t.Revision) {
		return strings.ToLower(t.Revision), nil
	}
	file, err := os.CreateTemp(f.opts.DataDir, "advertised-refs-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	cmd := f.git(ctx, f.opts.DataDir, "ls-remote", "--symref", p.remote, "HEAD", "refs/heads/*", "refs/tags/*")
	var stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = file, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("resolve revision: %w: %s", err, f.redact(strings.TrimSpace(stderr.String())))
	}
	if _, err := file.Seek(0, 0); err != nil {
		return "", err
	}
	return p.reader.ImportAdvertisedReferences(ctx, file, t.Revision)
}

func (f *FS) redact(s string) string {
	if token := f.opts.Token; token != "" {
		s = strings.ReplaceAll(s, token, "[redacted]")
		s = strings.ReplaceAll(s, base64.StdEncoding.EncodeToString([]byte("x-access-token:"+token)), "[redacted]")
	}
	return s
}
