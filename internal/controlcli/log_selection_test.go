package controlcli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gat/internal/control"
	"gat/internal/repo"
	"gat/internal/store"
)

type logFixture struct {
	t              *testing.T
	source, socket string
	tick           int
}

func (h *logFixture) git(args ...string) []byte {
	h.t.Helper()
	cmd := exec.Command("git", append([]string{"-C", h.source, "-c", "core.abbrev=7", "-c", "log.decorate=false", "-c", "color.ui=false"}, args...)...)
	date := fmt.Sprintf("%d +0000", 1700000000+h.tick)
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test", "GIT_AUTHOR_DATE="+date, "GIT_COMMITTER_DATE="+date, "LC_ALL=C")
	b, err := cmd.CombinedOutput()
	if err != nil {
		h.t.Fatalf("git %v: %v %s", args, err, b)
	}
	return b
}
func (h *logFixture) write(name, data string) {
	h.t.Helper()
	p := filepath.Join(h.source, name)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(data), 0644); err != nil {
		h.t.Fatal(err)
	}
}
func (h *logFixture) commit(message string) {
	h.t.Helper()
	h.tick++
	h.git("add", ".")
	h.git("commit", "-qm", message)
}
func newLogFixture(t *testing.T) *logFixture {
	h := &logFixture{t: t, source: t.TempDir()}
	h.git("init", "-qb", "main")
	// The target mount has case-sensitive Linux semantics.
	h.git("config", "core.ignorecase", "false")
	return h
}
func (h *logFixture) serve() {
	h.t.Helper()
	local, err := store.NewLocal(h.t.TempDir())
	if err != nil {
		h.t.Fatal(err)
	}
	if _, err := repo.Import(h.t.Context(), local, repo.ImportOptions{Repo: h.source}); err != nil {
		h.t.Fatal(err)
	}
	r, _ := repo.New(local, 32<<20)
	s, err := r.OpenRevision(h.t.Context(), "main", "")
	if err != nil {
		h.t.Fatal(err)
	}
	dir, err := os.MkdirTemp("", "gat-history-")
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { os.RemoveAll(dir) })
	h.socket = filepath.Join(dir, "s")
	c := control.New(r, s)
	server, err := control.ListenStream(h.t.Context(), h.socket, c.Serve)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { server.Close() })
}
func (h *logFixture) compare(args ...string) {
	h.t.Helper()
	want := h.git(append([]string{"log"}, args...)...)
	var out, stderr bytes.Buffer
	if err := Run(context.Background(), append([]string{"log", "--socket", h.socket}, args...), &out, &stderr); err != nil {
		h.t.Fatalf("gat log %v: %v %s", args, err, &stderr)
	}
	if !bytes.Equal(out.Bytes(), want) {
		h.t.Fatalf("log %v:\n got %q\nwant %q", args, out.Bytes(), want)
	}
}
func TestLogPathspecsMatchGit(t *testing.T) {
	h := newLogFixture(t)
	for _, name := range []string{"root.go", "root.txt", "src/a.go", "src/deep/b.go", "src/UPPER.GO", "docs/a.txt", "star*.txt", "colon:name", "src/x1.go"} {
		h.write(name, name+"\n")
		h.commit(name)
	}
	h.write(".gitattributes", "*.go chosen\nroot.go -marked\nroot.txt !marked\n*.txt kind=text\n[attr]chosen marked kind=code\n")
	h.write("src/.gitattributes", "a.go kind=special\n")
	h.commit("attribute rules")
	h.serve()
	cases := [][]string{
		{"*.go"}, {":(glob)nothing-here*"}, {"no-such*.extension"}, {"--", "*.go"}, {"--", "src/*.go"}, {"--", ":(glob)src/*.go"}, {"--", ":(glob)**/*.go"},
		{"--", ":(glob)src/**/b.go"}, {"--", ":(icase)*.go"}, {"--", ":(literal)star*.txt"},
		{"--", ":(top)root.go"}, {"--", ":/root.go"}, {"--", ".", ":!src"}, {"--", ":^src"},
		{"--", "*.go", ":(exclude)src/a.go"}, {"--", ":(glob)src/[ax][[:digit:]].go"},
		{"--", ":(glob)src/[!z]?.go"}, {"--", ":(glob)src/**"}, {"--", ":"},
		{"--", ":(glob)s*"}, {"--", ":(glob)src/*"}, {"--", "s*"},
		{"--", "src/"}, {"--", "root.go/"}, {"--", "src/?*.go"},
		{"--", "star\\*.txt"}, {"--", ":(glob)src/[[:alpha:]]*.go"},
		{"--", ":(glob)**/[ab].go"}, {"--", ":(glob,icase)**/*.go"},
		{"--", ":(glob)src[/]a.go"},
		{"--", ":(attr:marked)*.go"}, {"--", ":(attr:-marked)*.go"}, {"--", ":(attr:!marked)*"},
		{"--", ":(attr:kind=special)*"}, {"--", ":(attr:marked kind=code)*"},
		{"--", ".", ":(exclude,attr:kind=text)*"}, {"--", ":(top,literal)colon:name"},
		{"--oneline", "HEAD~2", "--", ":(attr:marked)*.go"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) { copy := *h; copy.t = t; copy.compare(args...) })
	}
	for _, args := range [][]string{{"--follow"}, {"--follow", "--", "root.go", "src/a.go"}, {"--follow", "--", ":(glob)*.go"}, {"--", ":(glob,literal)*"}, {"--", ":(unknown)file"}, {"--", ":", "root.go"}} {
		var out, stderr bytes.Buffer
		err := Run(t.Context(), append([]string{"log", "--socket", h.socket}, args...), &out, &stderr)
		if err == nil || out.Len() != 0 {
			t.Fatalf("invalid log %v emitted output or succeeded: %v", args, err)
		}
	}

}
func TestLogFollowMatchesGit(t *testing.T) {
	h := newLogFixture(t)
	var lines strings.Builder
	for i := 0; i < 40; i++ {
		fmt.Fprintf(&lines, "original line %02d is here\n", i)
	}
	h.write("old/file.txt", lines.String())
	h.commit("original")
	h.write("old/file.txt", lines.String()+"extra line\n")
	h.commit("edit original")
	if err := os.MkdirAll(filepath.Join(h.source, "new"), 0755); err != nil {
		t.Fatal(err)
	}
	h.git("mv", "old/file.txt", "new/file.txt")
	h.commit("exact rename")
	h.git("mv", "new/file.txt", "new/renamed.txt")
	h.write("new/renamed.txt", strings.ReplaceAll(lines.String(), "line 03", "edited 03"))
	h.commit("rename with edits")
	h.write("unrelated", "other\n")
	h.commit("unrelated")
	h.serve()
	for _, args := range [][]string{{"--follow", "new/renamed.txt"}, {"--follow", "--oneline", "--", "new/renamed.txt"}, {"--follow", "-n2", "--", "new/renamed.txt"}, {"--follow", "--first-parent", "--", "new/renamed.txt"}, {"--follow", "--", ":(top,literal)new/renamed.txt"}} {
		h.compare(args...)
	}
}

func TestLogFollowEdgeCasesMatchGit(t *testing.T) {
	for _, kind := range []string{"copy", "empty", "binary", "crlf", "ambiguous", "symlink", "unrelated"} {
		t.Run(kind, func(t *testing.T) {
			h := newLogFixture(t)
			content := strings.Repeat("a reasonably long unchanged line\n", 50)
			switch kind {
			case "empty":
				content = ""
			case "binary":
				content = string(bytes.Repeat([]byte{0, 1, 2, 3, 4, 5, 6, 7}, 2000))
			case "crlf":
				content = strings.Repeat("unchanged line\r\n", 6000)
			}
			if kind == "symlink" {
				if err := os.Symlink("target", filepath.Join(h.source, "a")); err != nil {
					t.Fatal(err)
				}
			} else {
				h.write("a", content)
			}
			h.commit("source a")
			if kind == "ambiguous" {
				h.write("b", content)
				h.commit("source b")
			}
			if kind == "copy" {
				h.write("z", content)
			} else {
				h.git("mv", "a", "z")
			}
			if kind == "ambiguous" {
				h.git("rm", "b")
			}
			if kind == "binary" || kind == "crlf" {
				h.write("z", content+"extra data\n")
			}
			if kind == "unrelated" {
				h.write("z", strings.Repeat("entirely different content\n", 60))
			}
			h.commit("destination")
			h.serve()
			h.compare("--follow", "--", "z")
		})
	}
}

func TestLogFollowMergeMatchesGit(t *testing.T) {
	h := newLogFixture(t)
	h.write("old", "initial\n")
	h.commit("root")
	h.git("checkout", "-qb", "topic")
	h.git("mv", "old", "new")
	h.commit("topic rename")
	h.git("checkout", "-q", "main")
	h.write("other", "unrelated\n")
	h.commit("main edit")
	h.tick++
	h.git("merge", "--no-ff", "-qm", "merge topic", "topic")
	h.write("new", "initial\nmore\n")
	h.commit("edit after merge")
	h.serve()
	h.compare("--follow", "--", "new")
	h.compare("--follow", "--first-parent", "--", "new")
}

func TestLogFollowResolvedMergeMatchesGit(t *testing.T) {
	h := newLogFixture(t)
	content := strings.Repeat("common original line\n", 30)
	h.write("old", content)
	h.commit("root")
	h.git("checkout", "-qb", "topic")
	h.git("mv", "old", "new")
	h.write("new", content+"topic\n")
	h.commit("topic rename")
	h.git("checkout", "-q", "main")
	h.git("mv", "old", "new")
	h.write("new", content+"main\n")
	h.commit("main rename")
	merge := exec.Command("git", "-C", h.source, "-c", "user.name=Test", "-c", "user.email=test@example.test", "merge", "--no-ff", "--no-commit", "topic")
	if err := merge.Run(); err == nil {
		t.Fatal("fixture should conflict")
	}
	h.write("new", content+"resolved\n")
	h.commit("resolve")
	h.serve()
	h.compare("--follow", "--", "new")
	h.compare("--follow", "--first-parent", "--", "new")
}

func TestLogFollowRecreatedNameMatchesGit(t *testing.T) {
	h := newLogFixture(t)
	h.write("file", "old unrelated contents\n")
	h.commit("old identity")
	h.git("rm", "file")
	h.commit("delete old identity")
	h.write("file", "new identity with different contents\n")
	h.commit("new identity")
	h.serve()
	h.compare("--follow", "--", "file")
}
