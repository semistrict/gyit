package controlcli

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gat/internal/control"
	pb "gat/internal/gen/gat/control/v1"
	"gat/internal/repo"
	"gat/internal/store"
)

func TestLogMatchesNativeGit(t *testing.T) {
	source := t.TempDir()
	date := "1000000000 +0530"
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", source}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test", "GIT_AUTHOR_DATE="+date, "GIT_COMMITTER_DATE="+date, "LC_ALL=C")
		data, err := cmd.Output()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, err.(*exec.ExitError).Stderr)
		}
		return string(data)
	}
	commit := func(name, message string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(source, name), []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
		git("add", ".")
		git("commit", "-qm", message)
	}
	git("init", "-q", "-b", "main")
	commit("root", "Root subject\ncontinued subject\n\nBody line\n\ttabbed body")
	git("checkout", "-qb", "topic")
	date = "1000000020 -0700"
	commit("topic", "Topic subject")
	git("checkout", "-q", "main")
	date = "1000000030 +0000"
	commit("main", "Main subject")
	date = "1000000040 +0000"
	git("merge", "--no-ff", "-qm", "Merge subject", "topic")
	date = "1000000050 +0000"
	if err := os.MkdirAll(filepath.Join(source, "sub"), 0755); err != nil {
		t.Fatal(err)
	}
	commit("sub/file", "Nested file")
	date = "1000000060 +0000"
	if err := os.Chmod(filepath.Join(source, "root"), 0755); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "Mode change")
	date = "1000000070 +0000"
	git("rm", "-q", "root")
	git("commit", "-qm", "Delete root")
	storage, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := repo.Import(ctx, storage, repo.ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	repository, _ := repo.New(storage, 1<<20)
	selected, err := repository.OpenRevision(ctx, "main", "")
	if err != nil {
		t.Fatal(err)
	}
	controller := control.New(repository, selected)
	dir, err := os.MkdirTemp("", "gat-log-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "control.sock")
	server, err := control.ListenStream(ctx, socket, controller.Serve)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	for _, flags := range [][]string{{}, {"--oneline"}, {"-n", "2"}, {"--oneline", "-n3", "topic", "--"}, {"--first-parent"}, {"--first-parent", "--oneline"}, {"--max-count=1", "main~1"}, {"-n", "0"}, {"sub/file"}, {"--", "root"}, {"--", "sub"}, {"--", "topic"}, {"main", "--", "root"}, {"--first-parent", "--", "topic"}, {"--", "missing"}, {"--", "root", "topic"}, {"--", ":(glob)**/file"}} {
		gitArgs := append([]string{"log", "--no-decorate", "--no-color", "--abbrev=7"}, flags...)
		// These arguments are shared verbatim, including the revision/path separator.
		want := git(gitArgs...)
		var stdout, stderr bytes.Buffer
		args := append([]string{"log", "--socket", socket}, flags...)
		if err := Run(ctx, args, &stdout, &stderr); err != nil {
			t.Fatal(args, err)
		}
		if stdout.String() != want {
			t.Fatalf("log %v:\n got %q\nwant %q", flags, stdout.String(), want)
		}
		if controller.Current() != selected {
			t.Fatal("log changed the mounted checkout")
		}
	}
	for _, args := range [][]string{{"-n", "-1"}, {"-n", "1001"}, {"--timeout", "0s"}, {"main", "topic"}, {"--unknown"}, {"missing-branch"}, {"topic"}, {"main"}, {"root"}} {
		var stdout, stderr bytes.Buffer
		if err := Run(ctx, append([]string{"log", "--socket", socket}, args...), &stdout, &stderr); err == nil {
			t.Fatal("accepted invalid log", args)
		}
		if stdout.Len() != 0 {
			t.Fatal("invalid request printed commits", stdout.String())
		}
	}
	client := control.Client{Endpoint: socket}
	if _, err := client.Switch(ctx, "topic"); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := Run(ctx, []string{"log", "--oneline", "-1", "--socket", socket}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Topic subject") {
		t.Fatal("log did not follow mounted commit", stdout.String())
	}
	var shas []string
	if err := client.Log(ctx, "", 20, false, func(entry *pb.LogEntry) error {
		shas = append(shas, entry.Sha)
		if len(shas) == 1 {
			_, err := client.Switch(ctx, "main")
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	wantIDs := strings.Fields(git("log", "--format=%H", "topic", "--"))
	if strings.Join(shas, " ") != strings.Join(wantIDs, " ") {
		t.Fatal("log mixed checkouts during switch", shas, wantIDs)
	}

}
