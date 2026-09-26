package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gyit/internal/store"
	bolt "go.etcd.io/bbolt"
)

func refViewGit(t *testing.T, source string, args ...string) []byte {
	t.Helper()
	c := exec.Command("git", append([]string{"-C", source, "-c", "color.ui=false", "-c", "core.abbrev=7"}, args...)...)
	c.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "LC_ALL=C")
	b, err := c.Output()
	if err != nil {
		t.Fatalf("native command %v: %v", args, err)
	}
	return b
}

func TestReferenceViewsMatchGit(t *testing.T) {
	ctx := context.Background()
	source := t.TempDir()
	command(t, source, "init", "-q", "-b", "main")
	write(t, source, "file", []byte("one\n"))
	first := commit(t, source)
	command(t, source, "branch", "feature/a")
	command(t, source, "tag", "v1")
	command(t, source, "tag", "-am", "release", "release")
	command(t, source, "update-ref", "refs/remotes/origin/main", first)
	command(t, source, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main")
	write(t, source, "file", []byte("two\n"))
	commit(t, source)
	command(t, source, "tag", "v2")
	local, _ := store.NewLocal(t.TempDir())
	if _, err := Import(ctx, local, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	r, _ := New(local, 1<<20)
	current, err := r.OpenRevision(ctx, "main", "")
	if err != nil {
		t.Fatal(err)
	}
	cases := [][]string{
		{"branch"}, {"branch", "-a"}, {"branch", "-r"}, {"branch", "-v"}, {"branch", "-av"}, {"branch", "--show-current"}, {"branch", "--list", "feature/*"}, {"branch", "--list", "*"}, {"branch", "--format=%(refname) %(objectname)"},
		{"tag"}, {"tag", "--list", "v*"}, {"tag", "--list", "missing*"}, {"tag", "--format=%(refname:short) %(objectname)"},
		{"show-ref"}, {"show-ref", "--head"}, {"show-ref", "--heads"}, {"show-ref", "--tags"}, {"show-ref", "--verify", "refs/heads/main"}, {"show-ref", "--hash=8", "main"}, {"show-ref", "--abbrev=8"}, {"show-ref", "-d"},
		{"rev-parse", "HEAD"}, {"rev-parse", "HEAD~1"}, {"rev-parse", "main", "v1"}, {"rev-parse", "release"}, {"rev-parse", "release^{}"}, {"rev-parse", "HEAD^{tree}"}, {"rev-parse", "HEAD:file"}, {"rev-parse", "--verify", "HEAD"}, {"rev-parse", "--short=8", "HEAD"}, {"rev-parse", "--symbolic-full-name", "HEAD"}, {"rev-parse", "--abbrev-ref", "HEAD"}, {"rev-parse", "--symbolic-full-name", "v1"}, {"rev-parse", "--all"}, {"rev-parse", "--branches"}, {"rev-parse", "--tags"}, {"rev-parse", "--remotes"}, {"rev-parse", "--show-prefix"}, {"rev-parse", "--show-cdup"}, {"rev-parse", "--is-inside-work-tree"}, {"rev-parse", "--show-object-format"}, {"rev-parse", "origin/HEAD"}, {"rev-parse", "--symbolic-full-name", "origin/HEAD"}, {"rev-parse", command(t, source, "rev-parse", "release")}, {"rev-parse", command(t, source, "rev-parse", "HEAD:file")},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out bytes.Buffer
			if err := r.ViewRefs(ctx, current, ViewOptions{Command: args[0], Args: args[1:]}, &out); err != nil {
				t.Fatal(err)
			}
			want := refViewGit(t, source, args...)
			if !bytes.Equal(out.Bytes(), want) {
				t.Fatalf("got %q\nwant %q", out.Bytes(), want)
			}
		})
	}
	for _, args := range [][]string{{"branch", "new"}, {"branch", "-d", "main"}, {"branch", "-m", "renamed"}, {"tag", "new"}, {"tag", "-d", "v1"}, {"tag", "-a", "new"}, {"show-ref", "--verify", "main"}, {"rev-parse", "--verify", "HEAD", "main"}} {
		if err := r.ViewRefs(ctx, current, ViewOptions{Command: args[0], Args: args[1:]}, io.Discard); err == nil {
			t.Errorf("accepted invalid or mutating command %v", args)
		}
	}
}

func TestReferenceViewsPinnedHeadAndFreshRefs(t *testing.T) {
	ctx := context.Background()
	source := t.TempDir()
	command(t, source, "init", "-q", "-b", "main")
	write(t, source, "file", []byte("old"))
	old := commit(t, source)
	local, _ := store.NewLocal(t.TempDir())
	if _, err := Import(ctx, local, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	r, _ := New(local, 1<<20)
	current, err := r.OpenRevision(ctx, "main", "")
	if err != nil {
		t.Fatal(err)
	}
	write(t, source, "file", []byte("new"))
	next := commit(t, source)
	if _, err := Import(ctx, local, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := r.ViewRefs(ctx, current, ViewOptions{Command: "rev-parse", Args: []string{"HEAD", "main"}}, &out); err != nil {
		t.Fatal(err)
	}
	if out.String() != old+"\n"+next+"\n" {
		t.Fatalf("mixed mounted HEAD and source ref: %q", out.String())
	}
}

func TestReferenceMetadataUpgradeAndQuietNoMatch(t *testing.T) {
	ctx := t.Context()
	source := t.TempDir()
	command(t, source, "init", "-q", "-b", "main")
	write(t, source, "file", []byte("contents"))
	commit(t, source)
	command(t, source, "tag", "-am", "release", "release")
	local, _ := store.NewLocal(t.TempDir())
	if _, err := Import(ctx, local, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	m, token, err := readHead(ctx, local)
	if err != nil {
		t.Fatal(err)
	}
	idx := &index{store: local, cache: newCache(1 << 20), root: m.Refs}
	refs, err := readViewRefs(ctx, idx)
	if err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(filepath.Join(t.TempDir(), "old.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("refs"))
		if err != nil {
			return err
		}
		for _, ref := range refs {
			data, err := marshal(reference{Commit: ref.Commit})
			if err != nil {
				return err
			}
			if err := bucket.Put([]byte("r/"+ref.name), data); err != nil {
				return err
			}
			if ref.ObjectID != "" && ref.ObjectID != ref.Commit {
				if err := bucket.Put([]byte("a/"+ref.ObjectID), data); err != nil {
					return err
				}
			}
		}
		hash := sha256.New()
		if err := bucket.ForEach(func(k, v []byte) error {
			for _, value := range [][]byte{k, v} {
				var size [8]byte
				binary.BigEndian.PutUint64(size[:], uint64(len(value)))
				hash.Write(size[:])
				hash.Write(value)
			}
			return nil
		}); err != nil {
			return err
		}
		m.RefsHash = fmt.Sprintf("%x", hash.Sum(nil))
		builder := &index{store: local, cache: newCache(1 << 20)}
		m.Refs, err = builder.update(ctx, bucket)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Put(ctx, "HEAD", data, token); err != nil {
		t.Fatal(err)
	}
	r, _ := New(local, 1<<20)
	current, err := r.OpenRevision(ctx, "main", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ViewRefs(ctx, current, ViewOptions{Command: "rev-parse", Args: []string{"release"}}, io.Discard); err == nil || !strings.Contains(err.Error(), "re-import") {
		t.Fatalf("old metadata should require upgrade: %v", err)
	}
	upgraded, err := Import(ctx, local, ImportOptions{Repo: source})
	if err != nil {
		t.Fatal(err)
	}
	if upgraded.Objects != 0 {
		t.Fatal("metadata upgrade reread objects")
	}
	var out bytes.Buffer
	if err := r.ViewRefs(ctx, current, ViewOptions{Command: "show-ref", Args: []string{"-d"}}, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), refViewGit(t, source, "show-ref", "-d")) {
		t.Fatal("upgraded reference identities do not match")
	}
	repeated, err := Import(ctx, local, ImportOptions{Repo: source})
	if err != nil || repeated.Generation != upgraded.Generation {
		t.Fatalf("repeated upgrade changed generation: %v", err)
	}
	out.Reset()
	err = r.ViewRefs(ctx, current, ViewOptions{Command: "show-ref", Args: []string{"--quiet", "--verify", "refs/heads/missing"}}, &out)
	if !errors.Is(err, ErrViewNoMatch) || out.Len() != 0 {
		t.Fatalf("quiet missing ref: %v, output %q", err, out.String())
	}
}

func TestReferenceViewsMediumParity(t *testing.T) {
	if os.Getenv("GYIT_MEDIUM_PARITY_TEST") != "1" {
		t.Skip("set GYIT_MEDIUM_PARITY_TEST=1 with the imported medium fixture")
	}
	dir := os.Getenv("GYIT_PARITY_STORE")
	if dir == "" {
		dir = "../../.testdata/lima-store"
	}
	local, err := store.NewLocal(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"branch"}, {"branch", "-a"}, {"branch", "-av"},
		{"tag"}, {"tag", "--list", "rust-v0.*"},
		{"show-ref"}, {"show-ref", "-d"}, {"show-ref", "--verify", "refs/heads/main"},
		{"rev-parse", "HEAD"}, {"rev-parse", "--short", "HEAD"}, {"rev-parse", "HEAD~10"},
		{"rev-parse", "--symbolic-full-name", "HEAD"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			r, _ := New(local, 32<<20)
			current, err := r.OpenRevision(t.Context(), "main", "")
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			start := time.Now()
			err = r.ViewRefs(t.Context(), current, ViewOptions{Command: args[0], Args: args[1:]}, &out)
			elapsed := time.Since(start)
			t.Logf("elapsed=%s", elapsed)
			if elapsed > time.Second {
				t.Logf("SLOW: %s took %s (>1s)", strings.Join(args, " "), elapsed)
			}
			if err != nil {
				t.Fatal(err)
			}
			want := refViewGit(t, "../../.testdata/medium-repo.git", args...)
			if !bytes.Equal(out.Bytes(), want) {
				t.Fatalf("output mismatch (%d vs %d bytes)", out.Len(), len(want))
			}
		})
	}
}

// No clone, network access or fixture mutation. Use -benchtime=3x for repeated
// cold-cache measurements; each iteration starts with a fresh bounded cache.
func BenchmarkReferenceViewsMedium(b *testing.B) {
	dir := os.Getenv("GYIT_BENCH_STORE")
	if dir == "" {
		dir = "../../.testdata/lima-store"
	}
	if _, err := os.Stat(filepath.Join(dir, "HEAD")); err != nil {
		b.Skip("import the medium fixture or set GYIT_BENCH_STORE")
	}
	for _, args := range [][]string{{"branch", "-a"}, {"branch", "-av"}, {"tag"}, {"show-ref"}, {"rev-parse", "--short", "HEAD"}} {
		b.Run(strings.Join(args, "_"), func(b *testing.B) {
			local, err := store.NewLocal(dir)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				r, _ := New(local, 32<<20)
				current, err := r.OpenRevision(context.Background(), "main", "")
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				start := time.Now()
				err = r.ViewRefs(context.Background(), current, ViewOptions{Command: args[0], Args: args[1:]}, io.Discard)
				elapsed := time.Since(start)
				if err != nil {
					b.Fatal(err)
				}
				if elapsed > time.Second {
					b.Logf("SLOW: %s took %s (>1s)", strings.Join(args, " "), elapsed)
				}
			}
		})
	}
}
