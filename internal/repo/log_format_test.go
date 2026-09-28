package repo

import (
	"bytes"
	"testing"

	"gyit/internal/store"
)

func TestLogMediumWhitespaceMatchesGit(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	write(t, dir, "file", []byte("test"))
	command(t, dir, "add", ".")
	command(t, dir, "commit", "-q", "--cleanup=verbatim", "-m", "subject \t\n\nbody with spaces \t\n\tindented\ttext \r\n")
	backend, _ := store.NewLocal(t.TempDir())
	if _, err := Import(t.Context(), backend, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	s, err := p.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err = s.LogWithOptions(t.Context(), LogOptions{Count: 1, Paths: []string{"file"}, FullCommitIDs: true}, func(e LogEntry) error { return WriteLogEntry(&out, e, false) })
	if err != nil {
		t.Fatal(err)
	}
	want := command(t, dir, "log", "-1", "--format=medium", "--no-decorate") + "\n"
	if out.String() != want {
		t.Fatalf("got %q; want %q", out.String(), want)
	}
}
