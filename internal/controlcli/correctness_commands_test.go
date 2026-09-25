package controlcli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"gat/internal/control"
	"gat/internal/repo"
	"gat/internal/store"
)

const correctnessCommandLimit = 60 * time.Second
const correctnessOutputLimit = 4 << 20

type correctnessCommandResult struct {
	Seconds        float64 `json:"seconds"`
	Over1s         bool    `json:"over_1s"`
	Exit           int     `json:"exit"`
	Error          string  `json:"error,omitempty"`
	TimedOut       bool    `json:"timed_out"`
	OutputLimited  bool    `json:"output_limited"`
	StdoutBytes    int     `json:"stdout_bytes"`
	StderrBytes    int     `json:"stderr_bytes"`
	StdoutSHA256   string  `json:"stdout_sha256"`
	stdout, stderr []byte
}

type correctnessCommandRow struct {
	Snapshot        string                   `json:"snapshot"`
	SHA             string                   `json:"sha"`
	Name            string                   `json:"name"`
	Args            []string                 `json:"args"`
	GitArgs         []string                 `json:"git_args"`
	ExpectedExit    int                      `json:"expected_exit"`
	Gat             correctnessCommandResult `json:"gat"`
	Git             correctnessCommandResult `json:"git"`
	Equal           bool                     `json:"equal"`
	FirstDifference int                      `json:"first_difference"`
	Artifacts       string                   `json:"artifacts"`
}

type correctnessCommandSetup struct {
	Name    string  `json:"name"`
	Seconds float64 `json:"seconds"`
	Over1s  bool    `json:"over_1s"`
}

type correctnessCommandsReport struct {
	Source                    string                    `json:"source"`
	Store                     string                    `json:"store"`
	GitBinary                 string                    `json:"git_binary"`
	GitVersion                string                    `json:"git_version"`
	CacheBytes                int                       `json:"cache_bytes"`
	PerCommandSeconds         int                       `json:"per_command_seconds"`
	OutputLimit               int                       `json:"output_limit_bytes"`
	StoreHeadBefore           string                    `json:"store_head_before_sha256"`
	StoreHeadAfter            string                    `json:"store_head_after_sha256"`
	SourceRefsSHA256          string                    `json:"source_refs_sha256"`
	SourceHEAD                string                    `json:"source_head"`
	Setups                    []correctnessCommandSetup `json:"setups"`
	Cases                     []correctnessCommandRow   `json:"cases"`
	ExpectedCases             int                       `json:"expected_cases"`
	Completed                 bool                      `json:"completed"`
	Limitations               []string                  `json:"limitations"`
	Correctness               bool                      `json:"correctness"`
	TemporaryDirectoryRemoved bool                      `json:"temporary_directory_removed"`
}

type correctnessCommandCase struct {
	name string
	args []string
	exit int
}

// This test imports nothing and never edits the source repository or store.
// It calls the normal CLI dispatcher through a real local protobuf server.
// A mismatch is always a failure, including output order and whitespace.
func TestCorrectnessLinuxCommandParity(t *testing.T) {
	if os.Getenv("GAT_RUN_CORRECTNESS_COMMANDS") != "1" {
		t.Skip("set GAT_RUN_CORRECTNESS_COMMANDS=1 after a complete store is published")
	}
	source := correctnessRequiredDirectory(t, "GAT_CORRECTNESS_SOURCE")
	storeDir := correctnessRequiredDirectory(t, "GAT_CORRECTNESS_STORE")
	reportPath := os.Getenv("GAT_CORRECTNESS_COMMAND_REPORT")
	if !filepath.IsAbs(reportPath) {
		t.Fatal("GAT_CORRECTNESS_COMMAND_REPORT must be an absolute new file")
	}
	if _, err := os.Lstat(reportPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("report already exists or cannot be inspected: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(reportPath), 0700); err != nil {
		t.Fatal(err)
	}
	artifacts := reportPath + ".d"
	if err := os.Mkdir(artifacts, 0700); err != nil {
		t.Fatal(err)
	}
	headPath := filepath.Join(storeDir, "HEAD")
	head, err := os.ReadFile(headPath)
	if err != nil {
		t.Fatalf("existing publication required: %v", err)
	}
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	git, err = filepath.Abs(git)
	if err != nil {
		t.Fatal(err)
	}
	report := correctnessCommandsReport{
		Source: source, Store: storeDir, GitBinary: git, CacheBytes: 32 << 20,
		PerCommandSeconds: int(correctnessCommandLimit / time.Second), OutputLimit: correctnessOutputLimit,
		StoreHeadBefore: correctnessDigest(head),
		ExpectedCases:   len(correctnessCommandCases(true)) + 2*len(correctnessCommandCases(false)),
		Limitations: []string{
			"This is bounded command correctness coverage, not exhaustive Git compatibility or a performance qualification.",
			"All rows use the CLI dispatcher and real protobuf socket. Terminal paging, shell exit rendering, socket discovery, and FUSE behavior require the separate executable/mount checks.",
			"Exact stdout and exit status are compared. Stderr is preserved for diagnosis but Git's diagnostic wording is not compared.",
			"Git uses a private index with all tracked paths marked skip-worktree: clean immutable status and tracked-file queries do not require a full checkout. Only tracked .mailmap and Makefile are materialized.",
			"Commands are sequential and reuse one bounded repository cache within each snapshot. Timings are observational, not cold-cache benchmarks.",
			"Known unsupported syntax is outside the passing case set: arbitrary log --format; diff rename detection and three-dot merge-base syntax; blame -w/-M/-C; raw commit/tag cat-file bodies. These remain compatibility gaps, not successful negative tests.",
			"No mutating Git commands, untracked/modified worktree semantics, arbitrary user configuration, network object store, or concurrent publication is covered here.",
		},
	}
	// Own the report path before execution; checkpoints survive a later hard
	// watchdog without presenting an incomplete run as successful.
	reportFile, err := os.OpenFile(reportPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := reportFile.Close(); err != nil {
		t.Fatal(err)
	}
	writeReport := func(t *testing.T) {
		t.Helper()
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			t.Error(err)
			return
		}
		staged := reportPath + ".writing"
		if err := os.WriteFile(staged, append(data, '\n'), 0600); err != nil {
			t.Error(err)
			return
		}
		if err := os.Rename(staged, reportPath); err != nil {
			t.Error(err)
		}
	}
	writeReport(t)
	var owned string
	defer func() {
		after, readErr := os.ReadFile(headPath)
		if readErr != nil {
			t.Errorf("read publication after checks: %v", readErr)
		} else {
			report.StoreHeadAfter = correctnessDigest(after)
			if !bytes.Equal(head, after) {
				t.Error("command checks changed the published HEAD")
			}
		}
		if owned != "" {
			if err := os.RemoveAll(owned); err != nil {
				t.Errorf("remove private oracle: %v", err)
			}
			_, err := os.Lstat(owned)
			report.TemporaryDirectoryRemoved = errors.Is(err, os.ErrNotExist)
			if !report.TemporaryDirectoryRemoved {
				t.Errorf("private oracle remains: %s (%v)", owned, err)
			}
		}
		report.Correctness = !t.Failed() && report.Completed && len(report.Cases) == report.ExpectedCases
		writeReport(t)
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 240*time.Second)
	defer cancel()
	env := correctnessGitEnvironment()
	runGit := func(dir string, input []byte, args ...string) correctnessCommandResult {
		return correctnessRunGit(ctx, git, env, dir, input, args...)
	}
	mustGit := func(t *testing.T, dir string, input []byte, args ...string) []byte {
		t.Helper()
		r := correctnessRunGitLimit(ctx, git, env, dir, input, 32<<20, args...)
		if r.Exit != 0 || r.TimedOut || r.OutputLimited {
			t.Fatalf("Git setup %v: exit=%d error=%s stderr=%s", args, r.Exit, r.Error, r.stderr)
		}
		return r.stdout
	}
	report.GitVersion = strings.TrimSpace(string(mustGit(t, source, nil, "--version")))
	report.SourceHEAD = strings.TrimSpace(string(mustGit(t, source, nil, "rev-parse", "--verify", "HEAD^{commit}")))
	refs := mustGit(t, source, nil, "for-each-ref", "--format=%(objectname) %(refname) %(symref)")
	report.SourceRefsSHA256 = correctnessDigest(refs)
	if err := os.WriteFile(filepath.Join(artifacts, "source-refs.txt"), refs, 0600); err != nil {
		t.Fatal(err)
	}
	objects := strings.TrimSpace(string(mustGit(t, source, nil, "rev-parse", "--path-format=absolute", "--git-path", "objects")))
	if !filepath.IsAbs(objects) || strings.ContainsAny(objects, "\n\r") {
		t.Fatal("invalid source object directory")
	}
	owned, err = os.MkdirTemp("", "gat-command-correctness-")
	if err != nil {
		t.Fatal(err)
	}
	oracle := filepath.Join(owned, "oracle")
	start := time.Now()
	mustGit(t, owned, nil, "init", "-q", oracle)
	if err := os.MkdirAll(filepath.Join(oracle, ".git", "objects", "info"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oracle, ".git", "objects", "info", "alternates"), []byte(objects+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var update bytes.Buffer
	type symbolic struct{ name, target string }
	var symrefs []symbolic
	for _, line := range strings.Split(strings.TrimSpace(string(refs)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || len(fields) > 3 || len(fields[0]) != 40 || !strings.HasPrefix(fields[1], "refs/") {
			t.Fatalf("malformed source reference %q", line)
		}
		fmt.Fprintf(&update, "update %s %s\n", fields[1], fields[0])
		if len(fields) == 3 {
			symrefs = append(symrefs, symbolic{fields[1], fields[2]})
		}
	}
	mustGit(t, oracle, update.Bytes(), "update-ref", "--stdin")
	for _, ref := range symrefs {
		mustGit(t, oracle, nil, "symbolic-ref", ref.name, ref.target)
	}
	if copied := mustGit(t, oracle, nil, "for-each-ref", "--format=%(objectname) %(refname) %(symref)"); !bytes.Equal(copied, refs) {
		t.Fatal("private oracle refs differ from independent source refs")
	}
	correctnessRecordSetup(&report, "private-reference-oracle", start)

	local, err := store.NewLocal(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, revision := range []string{"HEAD", "v4.4", "v2.6.24"} {
		if ctx.Err() != nil {
			t.Error("command suite deadline exceeded; coverage is incomplete")
			break
		}
		sha := strings.TrimSpace(string(mustGit(t, source, nil, "rev-parse", "--verify", revision+"^{commit}")))
		if len(sha) != 40 {
			t.Fatalf("bad snapshot SHA for %s: %q", revision, sha)
		}
		t.Run(revision, func(t *testing.T) {
			start := time.Now()
			// A real checkout reflog entry lets native status describe the same
			// detached identity without writing thousands of worktree files.
			mustGit(t, oracle, nil, "-c", "core.logAllRefUpdates=true", "update-ref", "--no-deref", "--create-reflog", "-m", "checkout: moving from HEAD to "+sha, "HEAD", sha)
			// Each native snapshot gets a fresh owned index. Remove only the
			// two files this oracle materializes, before the next read-tree.
			for _, name := range []string{".mailmap", "Makefile", ".git/index"} {
				if err := os.Remove(filepath.Join(oracle, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
					t.Fatal(err)
				}
			}
			mustGit(t, oracle, nil, "read-tree", "--reset", sha)
			tracked := mustGit(t, oracle, nil, "ls-files", "-z")
			mustGit(t, oracle, tracked, "update-index", "-z", "--skip-worktree", "--stdin")
			// The private index is authoritative for tracked paths; a mailmap
			// needs a worktree file because shortlog normally reads that file.
			mailmapPath := filepath.Join(oracle, ".mailmap")
			if err := os.Remove(mailmapPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			mailmap := runGit(oracle, nil, "cat-file", "-p", "HEAD:.mailmap")
			if mailmap.Exit == 0 && !mailmap.OutputLimited && !mailmap.TimedOut {
				if err := os.WriteFile(mailmapPath, mailmap.stdout, 0600); err != nil {
					t.Fatal(err)
				}
			} else if mailmap.Exit != 128 || mailmap.OutputLimited || mailmap.TimedOut {
				t.Fatalf("native mailmap check: %+v", mailmap)
			}
			// Native Git infers a positional path from the worktree, even when
			// every index entry is skip-worktree. Keep this small exact file.
			makefile := mustGit(t, oracle, nil, "cat-file", "-p", "HEAD:Makefile")
			if err := os.WriteFile(filepath.Join(oracle, "Makefile"), makefile, 0600); err != nil {
				t.Fatal(err)
			}
			correctnessRecordSetup(&report, revision+"/native-index", start)
			repository, err := repo.New(local, 32<<20)
			if err != nil {
				t.Fatal(err)
			}
			start = time.Now()
			selected, err := repository.OpenRevision(ctx, sha, "")
			correctnessRecordSetup(&report, revision+"/open", start)
			if err != nil {
				t.Fatal(err)
			}
			if selected.SHA != sha {
				t.Fatal("opened revision differs from independent source")
			}
			controller := control.New(repository, selected)
			socket := filepath.Join(owned, "s")
			server, err := control.ListenStream(t.Context(), socket, controller.Serve)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			for _, tc := range correctnessCommandCases(revision == "HEAD") {
				if ctx.Err() != nil {
					t.Error("command suite deadline exceeded; coverage is incomplete")
					break
				}
				t.Run(tc.name, func(t *testing.T) {
					row := correctnessCommandRow{Snapshot: revision, SHA: sha, Name: tc.name, Args: tc.args, GitArgs: tc.args, ExpectedExit: tc.exit}
					row.Git = runGit(oracle, nil, tc.args...)
					row.Gat = correctnessRunGat(ctx, socket, tc.args)
					row.FirstDifference = correctnessFirstDifference(row.Gat.stdout, row.Git.stdout)
					row.Equal = row.Gat.Exit == tc.exit && row.Git.Exit == tc.exit && row.FirstDifference == -1 && !row.Gat.TimedOut && !row.Git.TimedOut && !row.Gat.OutputLimited && !row.Git.OutputLimited
					row.Artifacts = filepath.Join(artifacts, revision, tc.name)
					if err := os.MkdirAll(row.Artifacts, 0700); err != nil {
						t.Fatal(err)
					}
					for name, data := range map[string][]byte{"gat.stdout": row.Gat.stdout, "git.stdout": row.Git.stdout, "gat.stderr": row.Gat.stderr, "git.stderr": row.Git.stderr} {
						if err := os.WriteFile(filepath.Join(row.Artifacts, name), data, 0600); err != nil {
							t.Fatal(err)
						}
					}
					report.Cases = append(report.Cases, row)
					writeReport(t)
					if row.Gat.Over1s || row.Git.Over1s {
						t.Logf("OVER 1s %s: gat %.6fs, Git %.6fs", tc.name, row.Gat.Seconds, row.Git.Seconds)
					}
					if !row.Equal {
						t.Errorf("parity failure: gat exit=%d, Git exit=%d, expected=%d; first differing byte=%d; gat error=%q; Git error=%q; artifacts %s", row.Gat.Exit, row.Git.Exit, tc.exit, row.FirstDifference, row.Gat.Error, row.Git.Error, row.Artifacts)
					}
				})
			}
		})
	}
	// Verify that even source reference metadata was only read.
	if after := mustGit(t, source, nil, "for-each-ref", "--format=%(objectname) %(refname) %(symref)"); !bytes.Equal(after, refs) {
		t.Error("source reference census changed during command verification")
	}
	if after := strings.TrimSpace(string(mustGit(t, source, nil, "rev-parse", "--verify", "HEAD^{commit}"))); after != report.SourceHEAD {
		t.Error("source HEAD changed during command verification")
	}
	report.Completed = len(report.Cases) == report.ExpectedCases
	if !report.Completed {
		t.Errorf("incomplete coverage: %d of %d cases", len(report.Cases), report.ExpectedCases)
	}
}

func correctnessCommandCases(full bool) []correctnessCommandCase {
	cases := []correctnessCommandCase{
		{"status", []string{"status"}, 0},
		{"status-short", []string{"status", "--short"}, 0},
		{"status-branch", []string{"status", "-sb"}, 0},
		{"status-porcelain2", []string{"status", "--porcelain=2", "--branch"}, 0},
		{"log", []string{"log", "-n5", "HEAD"}, 0},
		{"log-file", []string{"log", "-n5", "HEAD", "--", "Makefile"}, 0},
		{"log-follow", []string{"log", "--follow", "-n5", "HEAD", "--", "Makefile"}, 0},
		{"show-commit", []string{"show", "--no-patch", "HEAD"}, 0},
		{"show-blob", []string{"show", "HEAD:README"}, 0},
		{"diff-patch", []string{"diff", "--no-renames", "HEAD~1", "HEAD", "--", "Makefile"}, 0},
		{"diff-empty", []string{"diff", "--no-renames", "HEAD", "HEAD", "--", "Makefile"}, 0},
		{"blame", []string{"blame", "-L1,10", "HEAD", "--", "Makefile"}, 0},
		{"annotate", []string{"annotate", "-L1,10", "HEAD", "--", "Makefile"}, 0},
		{"ls-tree-root", []string{"ls-tree", "-l", "HEAD"}, 0},
		{"cat-file-blob", []string{"cat-file", "-p", "HEAD:Makefile"}, 0},
		{"rev-parse", []string{"rev-parse", "HEAD", "HEAD~1", "HEAD^{tree}"}, 0},
		{"rev-list-parents", []string{"rev-list", "--parents", "--max-count=20", "HEAD"}, 0},
		{"merge-base", []string{"merge-base", "HEAD", "HEAD~8"}, 0},
	}
	if !full {
		return cases
	}
	return append(cases,
		correctnessCommandCase{"status-porcelain1", []string{"status", "--porcelain=1", "--branch"}, 0},
		correctnessCommandCase{"status-zero", []string{"status", "-z"}, 0},
		correctnessCommandCase{"status-porcelain2-zero", []string{"status", "--porcelain=2", "--branch", "-z"}, 0},
		correctnessCommandCase{"log-oneline", []string{"log", "--oneline", "-n20", "HEAD"}, 0},
		correctnessCommandCase{"log-first-parent", []string{"log", "--first-parent", "-n20", "HEAD"}, 0},
		correctnessCommandCase{"log-positional-file", []string{"log", "-n5", "Makefile"}, 0},
		correctnessCommandCase{"log-directory", []string{"log", "--oneline", "-n5", "HEAD", "--", "include/linux"}, 0},
		correctnessCommandCase{"log-glob", []string{"log", "--oneline", "-n5", "HEAD", "--", ":(glob)include/linux/compiler*.h"}, 0},
		correctnessCommandCase{"log-exclude", []string{"log", "--oneline", "-n5", "HEAD", "--", "include/linux", ":(exclude)include/linux/compiler.h"}, 0},
		correctnessCommandCase{"log-zero", []string{"log", "-n0", "HEAD"}, 0},
		correctnessCommandCase{"show-names", []string{"show", "--first-parent", "--name-only", "HEAD", "--", "Makefile", "include/linux"}, 0},
		correctnessCommandCase{"show-previous-blob", []string{"show", "HEAD~1:Makefile"}, 0},
		correctnessCommandCase{"show-tree", []string{"show", "HEAD:include/linux"}, 0},
		correctnessCommandCase{"diff-names", []string{"diff", "--no-renames", "--name-only", "HEAD~8", "HEAD", "--", "Makefile", "include/linux"}, 0},
		correctnessCommandCase{"diff-status", []string{"diff", "--no-renames", "--name-status", "HEAD~8", "HEAD", "--", "Makefile", "include/linux"}, 0},
		correctnessCommandCase{"diff-unified-zero", []string{"diff", "--no-renames", "-U0", "HEAD~8", "HEAD", "--", "Makefile"}, 0},
		correctnessCommandCase{"diff-release-patch", []string{"diff", "--no-renames", "v2.6.24", "v4.4", "--", "Makefile"}, 0},
		correctnessCommandCase{"blame-first-parent", []string{"blame", "--first-parent", "-L1,10", "HEAD", "--", "Makefile"}, 0},
		correctnessCommandCase{"blame-porcelain", []string{"blame", "--line-porcelain", "-L1,5", "HEAD", "--", "Makefile"}, 0},
		correctnessCommandCase{"ls-tree-zero", []string{"ls-tree", "-lz", "HEAD:include/linux"}, 0},
		correctnessCommandCase{"ls-tree-subtree", []string{"ls-tree", "-rl", "HEAD:arch/x86/include/asm"}, 0},
		correctnessCommandCase{"ls-files-stage", []string{"ls-files", "-s", "--", "Makefile", "README"}, 0},
		correctnessCommandCase{"ls-files-zero", []string{"ls-files", "-z", "--", "include/linux/compiler.h"}, 0},
		correctnessCommandCase{"cat-file-type", []string{"cat-file", "-t", "HEAD"}, 0},
		correctnessCommandCase{"cat-file-size", []string{"cat-file", "-s", "HEAD"}, 0},
		correctnessCommandCase{"cat-file-tree-pretty", []string{"cat-file", "-p", "HEAD:include/linux"}, 0},
		correctnessCommandCase{"cat-file-tree-raw", []string{"cat-file", "tree", "HEAD^{tree}"}, 0},
		correctnessCommandCase{"cat-file-exists", []string{"cat-file", "-e", "HEAD:Makefile"}, 0},
		correctnessCommandCase{"grep-blob", []string{"grep", "-n", "-F", "VERSION", "HEAD:Makefile"}, 0},
		correctnessCommandCase{"grep-no-match", []string{"grep", "-q", "-F", "gat-correctness-impossible-token-812bd45e", "HEAD:Makefile"}, 1},
		correctnessCommandCase{"branch", []string{"branch", "-a"}, 0},
		correctnessCommandCase{"branch-current-detached", []string{"branch", "--show-current"}, 0},
		correctnessCommandCase{"tag-all", []string{"tag", "--list"}, 0},
		correctnessCommandCase{"show-ref-all", []string{"show-ref"}, 0},
		correctnessCommandCase{"tag-pattern", []string{"tag", "--list", "v4.*"}, 0},
		correctnessCommandCase{"show-ref-heads", []string{"show-ref", "--heads"}, 0},
		correctnessCommandCase{"show-ref-verify-tag", []string{"show-ref", "--verify", "refs/tags/v4.4"}, 0},
		correctnessCommandCase{"rev-parse-short", []string{"rev-parse", "--short", "HEAD"}, 0},
		correctnessCommandCase{"rev-list-first-parent", []string{"rev-list", "--first-parent", "--max-count=20", "HEAD"}, 0},
		correctnessCommandCase{"rev-list-range", []string{"rev-list", "--count", "HEAD~8..HEAD"}, 0},
		correctnessCommandCase{"rev-list-zero", []string{"rev-list", "--max-count=0", "HEAD"}, 0},
		correctnessCommandCase{"merge-base-is-ancestor", []string{"merge-base", "--is-ancestor", "HEAD~1", "HEAD"}, 0},
		correctnessCommandCase{"merge-base-not-ancestor", []string{"merge-base", "--is-ancestor", "HEAD", "HEAD~1"}, 1},
		correctnessCommandCase{"shortlog", []string{"shortlog", "-sne", "--max-count=20", "HEAD"}, 0},
	)
}

func correctnessRequiredDirectory(t *testing.T, name string) string {
	t.Helper()
	p := os.Getenv(name)
	if !filepath.IsAbs(p) {
		t.Fatalf("%s must name an existing absolute directory", name)
	}
	p, err := filepath.EvalSymlinks(p)
	if err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(p)
	if err != nil || !st.IsDir() {
		t.Fatalf("%s is not a directory: %v", name, err)
	}
	return p
}

func correctnessGitEnvironment() []string {
	var env []string
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		if strings.HasPrefix(key, "GIT_") || key == "LC_ALL" || key == "TZ" || key == "TERM" || key == "GAT_PAGER" {
			continue
		}
		env = append(env, value)
	}
	return append(env, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_PAGER=cat", "GIT_TERMINAL_PROMPT=0", "GIT_OPTIONAL_LOCKS=0", "LC_ALL=C", "TZ=UTC", "TERM=dumb", "GAT_PAGER=cat")
}

type correctnessCappedBuffer struct {
	bytes.Buffer
	limit   int
	limited bool
}

var errCorrectnessOutputLimit = errors.New("command output exceeds fixed cap")

func (b *correctnessCappedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		n, _ := b.Buffer.Write(p[:max(0, b.limit-b.Len())])
		b.limited = true
		return n, errCorrectnessOutputLimit
	}
	return b.Buffer.Write(p)
}

func correctnessRunGit(parent context.Context, binary string, env []string, dir string, input []byte, args ...string) correctnessCommandResult {
	return correctnessRunGitLimit(parent, binary, env, dir, input, correctnessOutputLimit, args...)
}

func correctnessRunGitLimit(parent context.Context, binary string, env []string, dir string, input []byte, outputLimit int, args ...string) correctnessCommandResult {
	ctx, cancel := context.WithTimeout(parent, correctnessCommandLimit)
	defer cancel()
	base := []string{"-C", dir, "-c", "core.abbrev=7", "-c", "color.ui=false", "-c", "log.decorate=false", "-c", "core.quotePath=true", "-c", "core.fsmonitor=false", "-c", "user.name=Command Oracle", "-c", "user.email=command-oracle@invalid"}
	cmd := exec.CommandContext(ctx, binary, append(base, args...)...)
	cmd.Env = env
	cmd.Stdin = bytes.NewReader(input)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = 2 * time.Second
	stdout := correctnessCappedBuffer{limit: outputLimit}
	stderr := correctnessCappedBuffer{limit: 256 << 10}
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	start := time.Now()
	err := cmd.Run()
	r := correctnessCommandResult{Seconds: time.Since(start).Seconds(), stdout: stdout.Bytes(), stderr: stderr.Bytes(), TimedOut: ctx.Err() != nil, OutputLimited: stdout.limited || stderr.limited}
	if err != nil {
		r.Exit, r.Error = -1, err.Error()
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			r.Exit = exit.ExitCode()
		}
	}
	correctnessFinishResult(&r)
	return r
}

func correctnessRunGat(parent context.Context, socket string, args []string) correctnessCommandResult {
	ctx, cancel := context.WithTimeout(parent, correctnessCommandLimit)
	defer cancel()
	stdout := correctnessCappedBuffer{limit: correctnessOutputLimit}
	stderr := correctnessCappedBuffer{limit: 256 << 10}
	forwarded := append([]string{args[0], "--socket", socket, "--timeout", "60s"}, args[1:]...)
	start := time.Now()
	err := Run(ctx, forwarded, &stdout, &stderr)
	r := correctnessCommandResult{Seconds: time.Since(start).Seconds(), stdout: stdout.Bytes(), stderr: stderr.Bytes(), TimedOut: ctx.Err() != nil, OutputLimited: stdout.limited || stderr.limited}
	if err != nil {
		r.Exit, r.Error = 1, err.Error()
		if !errors.Is(err, repo.ErrViewNoMatch) {
			_, _ = fmt.Fprintf(&stderr, "gat: %v\n", err)
			r.stderr = stderr.Bytes()
		}
	}
	correctnessFinishResult(&r)
	return r
}

func correctnessFinishResult(r *correctnessCommandResult) {
	r.Over1s = r.Seconds > 1
	r.StdoutBytes, r.StderrBytes = len(r.stdout), len(r.stderr)
	r.StdoutSHA256 = correctnessDigest(r.stdout)
}
func correctnessDigest(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func correctnessFirstDifference(a, b []byte) int {
	for i := 0; i < min(len(a), len(b)); i++ {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return min(len(a), len(b))
	}
	return -1
}
func correctnessRecordSetup(report *correctnessCommandsReport, name string, start time.Time) {
	seconds := time.Since(start).Seconds()
	report.Setups = append(report.Setups, correctnessCommandSetup{Name: name, Seconds: seconds, Over1s: seconds > 1})
}

var _ io.Writer = (*correctnessCappedBuffer)(nil)
