package repo

import (
	"bytes"
	"strings"
	"testing"

	"gat/internal/store"
)

func TestShowPathSelectionMatchesGit(t *testing.T) {
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", "/dev/null")
	source := t.TempDir()
	command(t, source, "init", "-q", "-b", "main")
	write(t, source, "Makefile", []byte("unchanged\n"))
	write(t, source, "include/linux/header.h", []byte("unchanged\n"))
	root := commit(t, source)
	command(t, source, "checkout", "-qb", "topic")
	write(t, source, "topic-file", []byte("topic\n"))
	commit(t, source)
	command(t, source, "checkout", "-q", "main")
	write(t, source, "main-file", []byte("main\n"))
	main := commit(t, source)
	command(t, source, "merge", "-q", "--no-ff", "topic", "-m", "merge separate files")
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Import(t.Context(), backend, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	r, err := New(backend, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	current, err := r.OpenRevision(t.Context(), "main", "")
	if err != nil {
		t.Fatal(err)
	}
	cases := [][]string{
		{"--first-parent", "--name-only", "HEAD", "--", "Makefile", "include/linux"},
		{"--first-parent", "--name-only", "HEAD", "--", "missing"},
		{"--first-parent", "--name-only", "HEAD", "--", "topic-file"},
		{"--first-parent", "--name-status", "HEAD", "--", "topic-file"},
		{"--first-parent", "HEAD", "--", "topic-file"},
		{"--first-parent", "--no-patch", "HEAD", "--", "topic-file"},
		{"--first-parent", "--no-patch", "HEAD", "--", "Makefile"},
		{"--no-patch", "HEAD", "--", "topic-file"},
		{"--no-patch", "HEAD", "--", "main-file", "topic-file"},
		{"--name-only", "HEAD", "--", "topic-file"},
		{"--name-only", "HEAD", "--", "main-file", "topic-file"},
		{"--name-only", main, "--", "Makefile"},
		{"--name-status", main, "--", "missing"},
		{main, "--", "missing"},
		{"--no-patch", main, "--", "missing"},
		{"--no-patch", main, "--", "main-file"},
		{"--name-only", root, "--", "Makefile"},
		{"--name-only", root, "--", "missing"},
		{"--first-parent", "--name-only", "HEAD", main, "--", "main-file"},
		{"--first-parent", "--name-only", main, "HEAD", "--", "main-file"},
		{"--first-parent", "--oneline", "--name-only", "HEAD", main, "--", "main-file"},
		{"--first-parent", "--name-only", "HEAD", "--", ":(glob)topic-*"},
		{"--first-parent", "--name-only", "HEAD", "--", ":(exclude)topic-file"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			want, status := graphGit(t, source, append([]string{"show"}, args...)...)
			if status != 0 {
				t.Fatalf("native show failed: %d", status)
			}
			var got bytes.Buffer
			if err := r.ViewShow(t.Context(), current, ViewOptions{Args: args}, &got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got.Bytes(), want) {
				t.Fatalf("show path selection differs:\n got %q\nwant %q", got.Bytes(), want)
			}
		})
	}
}
