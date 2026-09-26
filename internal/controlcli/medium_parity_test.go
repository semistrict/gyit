package controlcli

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gyit/internal/control"
	"gyit/internal/repo"
	"gyit/internal/store"
)

// GYIT_MEDIUM_PARITY_TEST=1 go test ./internal/controlcli -run TestMediumCommandParity
// Uses the existing ignored source and imported store; never reclones or fetches.
// Every comparison is byte-for-byte: no sorting, whitespace normalization,
// omitted fields, or exemptions for ambiguous blank-line attribution.
func TestMediumCommandParity(t *testing.T) {
	if os.Getenv("GYIT_MEDIUM_PARITY_TEST") != "1" {
		t.Skip("set GYIT_MEDIUM_PARITY_TEST=1 with the imported medium fixture")
	}
	source, err := filepath.Abs("../../.testdata/medium-repo.git")
	if err != nil {
		t.Fatal(err)
	}
	storeDir := os.Getenv("GYIT_PARITY_STORE")
	if storeDir == "" {
		storeDir = "../../.testdata/lima-store"
	}
	local, err := store.NewLocal(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	repository, _ := repo.New(local, 32<<20)
	selected, err := repository.OpenRevision(t.Context(), "main", "")
	if err != nil {
		t.Fatal(err)
	}
	controller := control.New(repository, selected)
	dir, err := os.MkdirTemp("", "gyit-parity-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "s")
	server, err := control.ListenStream(t.Context(), socket, controller.Serve)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	binary := filepath.Join(dir, "gyit")
	build := exec.Command("go", "build", "-o", binary, "../../cmd/gyit")
	if b, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, b)
	}
	env := append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "LC_ALL=C", "TZ=UTC", "TERM=dumb", "GYIT_PAGER=cat", "GIT_PAGER=cat")
	native := func(args ...string) []byte {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "git", append([]string{"-C", source, "-c", "core.abbrev=7", "-c", "color.ui=false", "-c", "log.decorate=false"}, args...)...)
		cmd.Env = env
		b, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	// Native worktree-dependent queries use a disposable checkout of the existing clone.
	worktree := filepath.Join(dir, "native")
	add := exec.CommandContext(t.Context(), "git", "-C", source, "worktree", "add", "--detach", worktree, selected.SHA)
	add.Env = env
	if b, err := add.CombinedOutput(); err != nil {
		t.Fatalf("native checkout: %v %s", err, b)
	}
	defer func() {
		c := exec.Command("git", "-C", source, "worktree", "remove", "--force", worktree)
		c.Env = env
		if err := c.Run(); err != nil {
			t.Error(err)
		}
	}()
	originalNative := native
	native = func(args ...string) []byte {
		if args[0] != "ls-files" && args[0] != "grep" {
			return originalNative(args...)
		}
		c := exec.CommandContext(t.Context(), "git", append([]string{"-C", worktree, "-c", "core.abbrev=7", "-c", "color.ui=false"}, args...)...)
		c.Env = env
		b, err := c.Output()
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	before := strings.TrimSpace(string(native("log", "-1", "--format=%H", selected.SHA, "--", "README.md"))) + "^"
	cases := []struct {
		name string
		args []string
	}{
		{"show", []string{"show", selected.SHA}},
		{"show-blob", []string{"show", selected.SHA + ":README.md"}},
		{"show-names", []string{"show", "--name-only", selected.SHA}},
		{"show-first-parent", []string{"show", "--first-parent", selected.SHA}},
		{"ls-tree", []string{"ls-tree", "HEAD"}},
		{"ls-tree-recursive", []string{"ls-tree", "-rl", "HEAD"}},
		{"ls-files", []string{"ls-files", "-s"}},
		{"cat-file-blob", []string{"cat-file", "-p", "HEAD:README.md"}},
		{"cat-file-type", []string{"cat-file", "-t", "HEAD"}},
		{"cat-file-size", []string{"cat-file", "-s", "HEAD"}},
		{"grep-blob-selector", []string{"grep", "-n", "-F", "the", "HEAD:README.md"}},
		{"grep-tree-selector", []string{"grep", "-n", "-F", "the", "HEAD:docs"}},
		{"grep-file", []string{"grep", "-n", "the", "--", "README.md"}},
		{"grep-repository", []string{"grep", "-l", "the"}},
		{"branch", []string{"branch", "-a"}},
		{"tag", []string{"tag"}},
		{"show-ref", []string{"show-ref"}},
		{"rev-parse", []string{"rev-parse", "HEAD"}},
		{"rev-list", []string{"rev-list", "-n100", "HEAD"}},
		{"rev-list-count", []string{"rev-list", "--count", "HEAD"}},
		{"merge-base", []string{"merge-base", "HEAD", "HEAD~100"}},
		{"shortlog", []string{"shortlog", "-sne", "HEAD"}},
		{"log", []string{"log", "-n5", selected.SHA}},
		{"log-oneline", []string{"log", "--oneline", "-n100", selected.SHA}},
		{"log-first-parent", []string{"log", "--first-parent", "-n100", selected.SHA}},
		{"log-file", []string{"log", "-n20", selected.SHA, "--", "README.md"}},
		{"log-file-first-parent", []string{"log", "--first-parent", "-n20", selected.SHA, "--", "README.md"}},
		{"log-directory", []string{"log", "--oneline", "-n20", selected.SHA, "--", "docs"}},
		{"log-follow", []string{"log", "--follow", "-n20", selected.SHA, "--", "README.md"}},
		{"log-glob", []string{"log", "--oneline", "-n20", selected.SHA, "--", ":(glob)docs/**/*.md"}},
		{"log-exclude", []string{"log", "--oneline", "-n20", selected.SHA, "--", "docs", ":(exclude)docs/getting-started.md"}},
		{"diff-names", []string{"diff", "--no-renames", "--name-only", selected.SHA + "~100", selected.SHA}},
		{"diff-status", []string{"diff", "--no-renames", "--name-status", selected.SHA + "~100", selected.SHA}},
		{"diff-patch", []string{"diff", "--no-renames", before, selected.SHA, "--", "README.md"}},
		{"blame", []string{"blame", "-L1,40", selected.SHA, "--", "README.md"}},
		{"blame-first-parent", []string{"blame", "--first-parent", "-L1,40", selected.SHA, "--", "README.md"}},
		{"blame-older", []string{"blame", "-L1,20", selected.SHA + "~100", "--", "README.md"}},
		{"annotate", []string{"annotate", "-L1,40", selected.SHA, "--", "README.md"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), binary, append([]string{tc.args[0], "--socket", socket}, tc.args[1:]...)...)
			cmd.Env = env
			started := time.Now()
			got, err := cmd.Output()
			elapsed := time.Since(started)
			t.Logf("%s took %s", tc.name, elapsed)
			if elapsed > time.Second {
				t.Logf("SLOW (>1s): %s %s", tc.name, elapsed)
			}
			if err != nil {
				t.Fatal(err)
			}
			want := native(tc.args...)
			if !bytes.Equal(got, want) {
				// Files aid diagnosis without flooding logs with fixture content.
				artifacts := filepath.Join("../../.build/parity", tc.name)
				if err := os.MkdirAll(artifacts, 0700); err != nil {
					t.Fatal(err)
				}
				_ = os.WriteFile(filepath.Join(artifacts, "gyit.txt"), got, 0600)
				_ = os.WriteFile(filepath.Join(artifacts, "git.txt"), want, 0600)
				offset := 0
				for offset < len(got) && offset < len(want) && got[offset] == want[offset] {
					offset++
				}
				t.Fatalf("command output differs at byte %d: got %d bytes, want %d; see .build/parity/%s", offset, len(got), len(want), tc.name)
			}
		})
	}
	t.Run("status-after-switch", func(t *testing.T) {
		worktree := filepath.Join(dir, "checkout")
		add := exec.CommandContext(t.Context(), "git", "-c", "core.logAllRefUpdates=true", "-C", source, "worktree", "add", "--detach", worktree, selected.SHA+"~1")
		add.Env = env
		if err := add.Run(); err != nil {
			t.Fatal(err)
		}
		defer func() {
			cmd := exec.Command("git", "-C", source, "worktree", "remove", "--force", worktree)
			cmd.Env = env
			if err := cmd.Run(); err != nil {
				t.Error(err)
			}
		}()
		for _, revision := range []string{selected.SHA, selected.SHA + "~1"} {
			checkout := exec.CommandContext(t.Context(), "git", "-c", "core.logAllRefUpdates=true", "-C", worktree, "checkout", "--detach", revision)
			checkout.Env = env
			if err := checkout.Run(); err != nil {
				t.Fatal(err)
			}
			change := exec.CommandContext(t.Context(), binary, "switch", "--socket", socket, revision)
			change.Env = env
			if b, err := change.CombinedOutput(); err != nil {
				t.Fatalf("switch: %v %s", err, b)
			}
			for _, flags := range [][]string{{}, {"--short"}, {"-sb"}, {"--porcelain=1", "--branch"}, {"--porcelain=2", "--branch"}, {"-z"}} {
				args := append([]string{"-C", worktree, "-c", "core.abbrev=7", "status"}, flags...)
				cmd := exec.CommandContext(t.Context(), "git", args...)
				cmd.Env = env
				want, err := cmd.Output()
				if err != nil {
					t.Fatal(err)
				}
				args = append([]string{"status", "--socket", socket}, flags...)
				cmd = exec.CommandContext(t.Context(), binary, args...)
				cmd.Env = env
				got, err := cmd.Output()
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, want) {
					os.WriteFile("../../.build/status-gyit.txt", got, 0600)
					os.WriteFile("../../.build/status-git.txt", want, 0600)
					t.Fatalf("status %v differs after switch %s", flags, revision)
				}
			}
		}
	})

}
