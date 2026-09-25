package controlcli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"gat/internal/control"
	pb "gat/internal/gen/gat/control/v1"
	"gat/internal/repo"
	"gat/internal/store"
)

func TestHistoryCommandsAgainstGit(t *testing.T) {
	source := t.TempDir()
	ctx := context.Background()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", source}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Test Author", "GIT_AUTHOR_EMAIL=a@example.test", "GIT_COMMITTER_NAME=Other Committer", "GIT_COMMITTER_EMAIL=c@example.test", "GIT_AUTHOR_DATE=1000000000 +0530", "GIT_COMMITTER_DATE=1000001234 -0730", "LC_ALL=C")
		data, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, data)
		}
		return string(data)
	}
	write := func(p, text string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(source, p)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(source, p), []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	commit := func(message string) { git("add", "."); git("commit", "-qm", message) }
	git("init", "-q", "-b", "main")
	write("file", "one\ntwo\nthree\nfour\nfive\nsix\nseven\neight\nnine\nten\n")
	write("deleted", "gone\n")
	write("mode", "exec\n")
	write("binary", "a\x00b")
	write("no-newline", "original")
	write("replacement", "was file\n")
	write("unchanged/tree/file", "do not load me\n")
	if err := os.Symlink("old", filepath.Join(source, "link")); err != nil {
		t.Fatal(err)
	}
	commit("root")
	root := strings.TrimSpace(git("rev-parse", "HEAD"))
	git("checkout", "-qb", "topic")
	write("file", "one\ntwo\nthree\nfour\nfive\nsix\nseven\neight\nnine\ntopic\n")
	commit("topic")
	git("checkout", "-q", "main")
	write("file", "main\none\ntwo\nthree\nfour\nfive\nsix\nseven\neight\nnine\nten\n")
	commit("main change")
	git("merge", "--no-ff", "--no-commit", "topic")
	merged, err := os.ReadFile(filepath.Join(source, "file"))
	if err != nil {
		t.Fatal(err)
	}
	write("file", string(merged)+"merge-only\n")
	commit("merge")
	write("added", "fresh\n")
	write("space name", "space\n")
	write("tab\tname", "tab\n")
	write("ümlaut", "utf8\n")
	write("empty", "")
	write("binary", "a\x00c")
	write("no-newline", "changed")
	if err := os.Remove(filepath.Join(source, "deleted")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(source, "replacement")); err != nil {
		t.Fatal(err)
	}
	write("replacement/child", "now directory\n")
	if err := os.Chmod(filepath.Join(source, "mode"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(source, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("new", filepath.Join(source, "link")); err != nil {
		t.Fatal(err)
	}
	commit("other changes")
	local, _ := store.NewLocal(t.TempDir())
	if _, err := repo.Import(ctx, local, repo.ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	repository, _ := repo.New(local, 1<<20)
	selected, err := repository.OpenRevision(ctx, "main", "")
	if err != nil {
		t.Fatal(err)
	}
	controller := control.New(repository, selected)
	dir, err := os.MkdirTemp("", "gat-history-")
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
	run := func(args ...string) string {
		t.Helper()
		var out, stderr bytes.Buffer
		args = append([]string{args[0], "--socket", socket}, args[1:]...)
		if err := Run(ctx, args, &out, &stderr); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		return out.String()
	}
	for _, flags := range [][]string{{}, {"--name-only"}, {"--name-status"}, {"-U0"}, {"-U1"}, {"--", "file"}, {"--", "unchanged"}, {"--", "replacement/child"}} {
		args := append([]string{"diff", root, "main"}, flags...)
		want := git(append([]string{"diff", "--no-ext-diff", "--no-textconv", "--no-color", "--no-renames", "--no-indent-heuristic", "--abbrev=7", root, "main"}, flags...)...)
		got := run(args...)
		if stripHunkLabels(got) != stripHunkLabels(want) {
			t.Errorf("diff %v:\n got %s\nwant %s", flags, got, want)
		}
	}
	if got := run("diff"); got != "" {
		t.Fatal("clean mount diff", got)
	}
	if got, want := run("diff", root, "--name-only"), run("diff", root+"..HEAD", "--name-only"); got != want {
		t.Fatal("one revision/range", got, want)
	}
	for _, revision := range []string{"main", "topic", root} {
		for _, first := range []bool{false, true} {
			flags := []string{}
			if first {
				flags = append(flags, "--first-parent")
			}
			args := append([]string{"blame", "--line-porcelain"}, flags...)
			args = append(args, revision, "--", "file")
			want := git(args...)
			got := run(args...)
			if !bytes.Equal([]byte(got), []byte(want)) {
				t.Fatalf("blame %s first=%v:\n%s\nwant\n%s", revision, first, got, want)
			}
		}
	}
	got := run("blame", "--line-porcelain", "-L2,4", "--", "file")
	want := git("blame", "--line-porcelain", "-L2,4", "--", "file")
	if got != want {
		t.Fatal("line range", got, want)
	}
	for _, name := range []string{"added", "tab\tname", "no-newline"} {
		args := []string{"blame", "--line-porcelain", "--", name}
		if got, want := run(args...), git(args...); got != want {
			t.Fatalf("exact porcelain %q:\n%s\nwant\n%s", name, got, want)
		}
	}
	if got := run("annotate", "-L1,1", "file"); got != git("annotate", "-L1,1", "file") {
		t.Fatal("annotate", got)
	}
	if got := run("blame", "empty"); got != "" {
		t.Fatal("empty blame", got)
	}
	for _, args := range [][]string{{"blame", "binary"}, {"blame", "-L0,2", "file"}, {"blame", "-L2,100", "file"}, {"blame", "missing"}, {"diff", "--name-only", "--name-status"}, {"diff", "-U101"}, {"diff", "main...topic"}, {"diff", root, "--", "../outside"}} {
		var out bytes.Buffer
		if err := Run(ctx, append([]string{args[0], "--socket", socket}, args[1:]...), &out, &out); err == nil {
			t.Fatal("accepted invalid input", args)
		}
	}
	client := control.Client{Socket: socket}
	var lines []string
	err = client.Blame(ctx, &pb.BlameRequest{Path: []byte("file")}, func(e *pb.BlameLine) error {
		lines = append(lines, string(e.Content))
		if len(lines) == 1 {
			_, err := client.Switch(ctx, "topic")
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(lines, "") != git("show", "main:file") {
		t.Fatal("blame mixed switched snapshots")
	}

	// Apply the generated text patch using native Git and compare the resulting
	// tree to the imported target (binary content is verified separately above).
	patch := run("diff", root, "main", "--", "added", "empty", "file", "deleted", "mode", "link", "no-newline", "replacement", "space name", "tab\tname", "ümlaut")
	git("checkout", "-q", "--detach", root)
	apply := exec.Command("git", "-C", source, "apply", "--index")
	apply.Stdin = strings.NewReader(patch)
	if data, err := apply.CombinedOutput(); err != nil {
		t.Fatalf("generated patch does not apply: %v: %s", err, data)
	}
	if remaining := git("diff", "--cached", "--name-only", "main", "--", "added", "empty", "file", "deleted", "mode", "link", "no-newline", "replacement", "space name", "tab\tname", "ümlaut"); remaining != "" {
		t.Fatal("patch did not reconstruct target tree", remaining)
	}
}

// Compare provenance and content, ignoring Git's grouping/extra committer fields.
func blameIdentity(text string) string {
	var out strings.Builder
	for _, line := range strings.Split(text, "\n") {
		f := strings.Fields(line)
		if (len(f) == 3 || len(f) == 4) && (len(f[0]) == 40 || len(f[0]) == 64) {
			if _, err := strconv.Atoi(f[1]); err == nil {
				fmt.Fprintf(&out, "%s %s %s\n", f[0], f[1], f[2])
				continue
			}
		}
		if strings.HasPrefix(line, "\t") || strings.HasPrefix(line, "author ") || strings.HasPrefix(line, "author-mail ") || strings.HasPrefix(line, "author-time ") || strings.HasPrefix(line, "author-tz ") || strings.HasPrefix(line, "filename ") {
			out.WriteString(line)
			out.WriteByte('\n')
		}
	}
	return out.String()
}

// Function labels are optional decoration, outside the patch's edit semantics.
func stripHunkLabels(patch string) string {
	lines := strings.Split(patch, "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "@@ ") {
			if p := strings.Index(line[3:], " @@"); p >= 0 {
				lines[i] = line[:p+6]
			}
		}
	}
	return strings.Join(lines, "\n")
}
