package repo

import (
	"strings"
	"testing"

	"gyit/internal/store"
)

func TestBlameOldCommitterMetadataRequiresReimport(t *testing.T) {
	ctx := t.Context()
	source := t.TempDir()
	command(t, source, "init", "-q", "-b", "main")
	write(t, source, "file", []byte("line\n"))
	sha := commit(t, source)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Import(ctx, backend, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	r, err := New(backend, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	s, err := r.Open(ctx, sha)
	if err != nil {
		t.Fatal(err)
	}
	var info commitInfo
	if err = s.idx.get(ctx, "c/"+sha, &info); err != nil {
		t.Fatal(err)
	}
	info.Committer, info.CommitterOffset, info.CommitterTruncated, info.HasCommitter = nil, 0, false, false
	value, err := marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	changes := &changes{key: []byte("c/" + sha), value: value, read: func() ([]byte, []byte, error) { return nil, nil, nil }}
	s.idx.root, err = s.idx.updateChanges(ctx, changes)
	if err != nil {
		t.Fatal(err)
	}
	emitted := 0
	emit := func(BlameLine) error { emitted++; return nil }
	err = s.Blame(ctx, BlameOptions{Path: "file", IncludeCommitter: true}, emit)
	if err == nil || !strings.Contains(err.Error(), "re-import") || emitted != 0 {
		t.Fatal("old metadata produced invented porcelain fields", err, emitted)
	}
	if err = s.Blame(ctx, BlameOptions{Path: "file"}, emit); err != nil || emitted != 1 {
		t.Fatal("legacy ordinary blame should remain available", err, emitted)
	}
}
