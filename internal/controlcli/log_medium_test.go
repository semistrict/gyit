package controlcli

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"gat/internal/control"
	"gat/internal/repo"
	"gat/internal/store"
)

// Uses the existing ignored clone and imported demo store; never clones/imports.
func TestMediumLogMatchesGit(t *testing.T) {
	if os.Getenv("GAT_MEDIUM_LOG_TEST") != "1" {
		t.Skip("set GAT_MEDIUM_LOG_TEST=1 with the existing medium fixture and demo store")
	}
	source, err := filepath.Abs("../../.testdata/medium-repo.git")
	if err != nil {
		t.Fatal(err)
	}
	local, err := store.NewLocal("../../.testdata/lima-store")
	if err != nil {
		t.Fatal(err)
	}
	repository, err := repo.New(local, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := repository.OpenRevision(context.Background(), "main", "")
	if err != nil {
		t.Fatal(err)
	}
	controller := control.New(repository, selected)
	dir, err := os.MkdirTemp("", "gat-log-medium-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "control.sock")
	server, err := control.ListenStream(context.Background(), socket, controller.Serve)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	for _, flags := range [][]string{{"--oneline", "-n", "20"}, {"--oneline", "-n", "100"}, {"--first-parent", "-n", "100"}, {"-n", "5"}} {
		args := append([]string{"-C", source, "log", "--no-decorate", "--no-color", "--abbrev=7"}, flags...)
		args = append(args, selected.SHA, "--")
		cmd := exec.Command("git", args...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "LC_ALL=C")
		want, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		var stdout, stderr bytes.Buffer
		if err := Run(context.Background(), append([]string{"log", "--socket", socket}, flags...), &stdout, &stderr); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(stdout.Bytes(), want) {
			a, b := stdout.Bytes(), want
			pos := 0
			for pos < len(a) && pos < len(b) && a[pos] == b[pos] {
				pos++
			}
			t.Fatalf("medium log %v differs at byte %d (got %d bytes, want %d)", flags, pos, len(a), len(b))
		}
	}
}

func TestMediumHistoryAgainstGit(t *testing.T) {
	if os.Getenv("GAT_MEDIUM_HISTORY_TEST") != "1" {
		t.Skip("set GAT_MEDIUM_HISTORY_TEST=1 with the existing medium fixture and demo store")
	}
	source, err := filepath.Abs("../../.testdata/medium-repo.git")
	if err != nil {
		t.Fatal(err)
	}
	local, err := store.NewLocal("../../.testdata/lima-store")
	if err != nil {
		t.Fatal(err)
	}
	repository, err := repo.New(local, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := repository.OpenRevision(context.Background(), "main", "")
	if err != nil {
		t.Fatal(err)
	}
	controller := control.New(repository, selected)
	dir, err := os.MkdirTemp("", "gat-log-medium-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "control.sock")
	server, err := control.ListenStream(context.Background(), socket, controller.Serve)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", source}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "LC_ALL=C")
		b, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	readmeBefore := strings.TrimSpace(git("log", "-1", "--format=%H", selected.SHA, "--", "README.md")) + "^"
	cases := [][]string{
		{"diff", "--name-status", selected.SHA + "~100", selected.SHA},
		{"diff", readmeBefore, selected.SHA, "--", "README.md"},
		{"blame", "--line-porcelain", "-L1,40", selected.SHA, "--", "README.md"},
	}
	for _, args := range cases {
		started := time.Now()
		var out, stderr bytes.Buffer
		if err := Run(context.Background(), append([]string{args[0], "--socket", socket}, args[1:]...), &out, &stderr); err != nil {
			t.Fatal(args[0], err)
		}
		gitArgs := args
		if args[0] == "diff" {
			gitArgs = append([]string{"diff", "--no-renames"}, args[1:]...)
		}
		want, got := git(gitArgs...), out.String()
		if args[0] == "blame" {
			a, b := strings.Split(strings.TrimSuffix(blameIdentity(got), "\n"), "\n"), strings.Split(strings.TrimSuffix(blameIdentity(want), "\n"), "\n")
			if len(a) != len(b) || len(a)%7 != 0 {
				t.Fatal("blame lost lines")
			}
			versions := map[string][]string{}
			for i := 0; i < len(a); i += 7 {
				header := strings.Fields(a[i])
				if len(header) != 3 {
					t.Fatal("invalid attribution header")
				}
				sha := header[0]
				original, err := strconv.Atoi(header[1])
				if err != nil {
					t.Fatal(err)
				}
				data, ok := versions[sha]
				if !ok {
					data = strings.Split(git("show", sha+":README.md"), "\n")
					versions[sha] = data
				}
				if original < 1 || original > len(data) || "\t"+data[original-1] != a[i+6] {
					t.Fatal("attributed line does not exist at the reported original location")
				}
				if a[i+6] != b[i+6] {
					t.Fatal("blame content differs from Git")
				}
				if strings.Join(a[i:i+7], "\n") != strings.Join(b[i:i+7], "\n") {
					t.Fatalf("line %d differs from native attribution", i/7+1)
				}
			}
			t.Log("validated exact attribution including blank lines")
		} else if args[1] == "--name-status" {
			// Names stream in directory traversal order, not flattened order.
			a, b := strings.Split(got, "\n"), strings.Split(want, "\n")
			sort.Strings(a)
			sort.Strings(b)
			if strings.Join(a, "\n") != strings.Join(b, "\n") {
				t.Fatal("changed paths/status differ from Git")
			}
		} else {
			// Patch validity and resulting bytes matter, not Git's hunk labels
			// or which equally minimal layout it chooses for repeated lines.
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte(git("show", args[1]+":README.md")), 0600); err != nil {
				t.Fatal(err)
			}
			apply := exec.Command("git", "apply")
			apply.Dir = dir
			apply.Stdin = strings.NewReader(got)
			if _, err := apply.CombinedOutput(); err != nil {
				t.Fatal("medium patch did not apply", err)
			}
			result, err := os.ReadFile(filepath.Join(dir, "README.md"))
			if err != nil {
				t.Fatal(err)
			}
			if string(result) != git("show", selected.SHA+":README.md") {
				t.Fatal("medium patch did not reconstruct target")
			}
		}
		t.Logf("%s validated in %s (%d output bytes)", args[0], time.Since(started).Round(time.Millisecond), out.Len())
	}
}
