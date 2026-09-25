package controlcli

import (
	"bytes"
	"errors"
	"testing"

	"gat/internal/repo"
)

func TestShowNativeParity(t *testing.T) {
	h := newLogFixture(t)
	h.write("README.md", "first\nsecond\n")
	h.write("dir/space file", "old\n")
	h.write("a/child", "nested\n")
	h.write("a.c", "flat\n")
	h.commit("Initial\n\nBody\twith tabs")
	h.write("README.md", "first\nchanged\n")
	h.write("dir/space file", "new\n")
	h.write("binary", "a\x00b")
	h.commit("Update files")
	h.serve()
	for _, args := range [][]string{{}, {"HEAD"}, {"HEAD^"}, {"--oneline"}, {"-s"}, {"--name-only"}, {"--name-status"}, {"-U0"}, {"HEAD", "--", "README.md"}, {"HEAD", "--", ":(glob)dir/*"}, {"HEAD:README.md"}, {"HEAD:dir"}, {"HEAD^{tree}"}, {"HEAD:README.md", "HEAD:README.md"}, {"HEAD^", "HEAD"}, {"--oneline", "HEAD^", "HEAD"}} {
		t.Run("show "+joinArgs(args), func(t *testing.T) {
			want := h.git(append([]string{"show"}, args...)...)
			var out, stderr bytes.Buffer
			if err := Run(t.Context(), append([]string{"show", "--socket", h.socket}, args...), &out, &stderr); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(out.Bytes(), want) {
				t.Fatalf("got %q\nwant %q", out.Bytes(), want)
			}
		})
	}
}

func joinArgs(args []string) string {
	var s string
	for _, a := range args {
		s += a + " "
	}
	return s
}

func TestViewCommandsThroughSocket(t *testing.T) {
	h := newLogFixture(t)
	h.write("file", "needle\n--socket\n--timeout\n--\n")
	h.commit("Initial")
	h.git("tag", "v1")
	h.serve()
	cases := [][]string{{"ls-tree", "HEAD"}, {"ls-files", "-s"}, {"cat-file", "-p", "HEAD:file"}, {"grep", "-n", "needle"}, {"grep", "-e", "--socket"}, {"grep", "-e", "--timeout"}, {"grep", "-e", "--"}, {"grep", "-e", "--", "HEAD"}, {"grep", "-e", "--", "HEAD:file"}, {"branch"}, {"tag"}, {"show-ref"}, {"rev-parse", "HEAD"}, {"rev-list", "HEAD"}, {"merge-base", "HEAD", "HEAD"}, {"shortlog", "-sne", "HEAD"}}
	for _, args := range cases {
		t.Run(joinArgs(args), func(t *testing.T) {
			want := h.git(args...)
			var out, stderr bytes.Buffer
			err := Run(t.Context(), append([]string{args[0], "--socket", h.socket}, args[1:]...), &out, &stderr)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(out.Bytes(), want) {
				t.Fatalf("got %q\nwant %q", out.Bytes(), want)
			}
		})
	}
	for _, args := range [][]string{{"grep", "absent"}, {"rev-parse", "--verify", "--quiet", "absent"}, {"show-ref", "--quiet", "--verify", "refs/heads/absent"}} {
		var out, stderr bytes.Buffer
		err := Run(t.Context(), append([]string{args[0], "--socket", h.socket}, args[1:]...), &out, &stderr)
		if !errors.Is(err, repo.ErrViewNoMatch) {
			t.Fatalf("%v: expected quiet negative result, got %v", args, err)
		}
	}
}
