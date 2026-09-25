package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	readerv1 "gat/internal/gen/verification/reader/v1"
	"gat/internal/store"
	"google.golang.org/protobuf/proto"
)

const correctnessMaxOracleBytes = 192 << 20
const correctnessMaxBlobBytes = 32 << 20

type correctnessLimitedWriter struct {
	out       io.Writer
	remaining int64
}

func (w *correctnessLimitedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, fmt.Errorf("Git oracle output exceeds bound")
	}
	n, err := w.out.Write(p)
	w.remaining -= int64(n)
	return n, err
}
func readerOracleGit(ctx context.Context, source string, out io.Writer, limit int64, args ...string) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", source}, args...)...)
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "GIT_") {
			cmd.Env = append(cmd.Env, e)
		}
	}
	cmd.Env = append(cmd.Env, "GIT_NO_REPLACE_OBJECTS=1", "GIT_NO_LAZY_FETCH=1", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "LC_ALL=C")
	cmd.WaitDelay = time.Second
	var stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &correctnessLimitedWriter{out, limit}, &correctnessLimitedWriter{&stderr, 64 << 10}
	started := time.Now()
	err := cmd.Run()
	if elapsed := time.Since(started); elapsed > time.Second {
		fmt.Fprintf(os.Stderr, "over 1 s: Git %v %.6f s\n", args, elapsed.Seconds())
	}
	if err != nil {
		return fmt.Errorf("Git %v: %w: %s", args, err, stderr.String())
	}
	return nil
}
func readerOracleGitBytes(t *testing.T, ctx context.Context, source string, limit int64, args ...string) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := readerOracleGit(ctx, source, &b, limit, args...); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func readerOracleGitText(t *testing.T, ctx context.Context, source string, args ...string) string {
	t.Helper()
	return strings.TrimSpace(string(readerOracleGitBytes(t, ctx, source, 4<<20, args...)))
}
func correctnessParseTree(raw []byte) ([]*readerv1.Entry, error) {
	var entries []*readerv1.Entry
	for len(raw) > 0 {
		line, rest, found := bytes.Cut(raw, []byte{0})
		if !found {
			return nil, fmt.Errorf("unterminated Git tree row")
		}
		raw = rest
		header, name, found := bytes.Cut(line, []byte{'\t'})
		if !found || len(name) == 0 {
			return nil, fmt.Errorf("invalid Git tree row")
		}
		fields := bytes.Fields(header)
		if len(fields) != 4 {
			return nil, fmt.Errorf("invalid Git tree fields")
		}
		mode, err := strconv.ParseUint(string(fields[0]), 8, 32)
		if err != nil {
			return nil, err
		}
		size := int64(0)
		if string(fields[1]) == "blob" {
			size, err = strconv.ParseInt(string(fields[3]), 10, 64)
			if err != nil || size < 0 {
				return nil, fmt.Errorf("invalid Git blob size")
			}
		} else if string(fields[1]) != "tree" && string(fields[1]) != "commit" {
			return nil, fmt.Errorf("unexpected Git tree kind")
		}
		if _, err = hex.DecodeString(string(fields[2])); err != nil || len(fields[2]) != 40 {
			return nil, fmt.Errorf("invalid Git tree OID")
		}
		entries = append(entries, &readerv1.Entry{Name: string(name), Oid: string(fields[2]), Mode: uint32(mode), Size: size})
		if len(entries) > 200000 {
			return nil, fmt.Errorf("Git tree entry bound exceeded")
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	for i := 1; i < len(entries); i++ {
		if entries[i-1].Name == entries[i].Name {
			return nil, fmt.Errorf("duplicate Git tree entry")
		}
	}
	return entries, nil
}
func correctnessTree(t *testing.T, ctx context.Context, source, oid string, recursive bool) []*readerv1.Entry {
	t.Helper()
	args := []string{"ls-tree", "-l", "-z"}
	if recursive {
		args = append(args, "-r")
	}
	args = append(args, oid)
	b := readerOracleGitBytes(t, ctx, source, 32<<20, args...)
	entries, err := correctnessParseTree(b)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func correctnessPrepareOracle(t *testing.T, ctx context.Context, source, dir string) *readerv1.Oracle {
	t.Helper()
	rev := os.Getenv("GAT_CORRECTNESS_REVISION")
	if rev == "" {
		rev = "HEAD"
	}
	tip := readerOracleGitText(t, ctx, source, "rev-parse", "--verify", rev+"^{commit}")
	o := &readerv1.Oracle{Tip: tip}
	gap := func(message string) { o.Gaps = append(o.Gaps, message); t.Log("oracle gap:", message) }
	seenRev := map[string]bool{}
	addRevision := func(sha, expr string) {
		if seenRev[sha] {
			return
		}
		seenRev[sha] = true
		o.Revisions = append(o.Revisions, &readerv1.Revision{Sha: sha, Tree: readerOracleGitText(t, ctx, source, "rev-parse", sha+"^{tree}"), Expression: expr})
	}
	addRevision(tip, tip)
	parents := strings.Fields(readerOracleGitText(t, ctx, source, "rev-list", "--max-count=2", "--first-parent", tip))
	if len(parents) > 1 {
		addRevision(parents[1], "HEAD~1")
	} else {
		gap("selected tip has no first parent")
	}
	head := correctnessTree(t, ctx, source, tip, true)
	byPath := map[string]*readerv1.Entry{}
	var files []*readerv1.Entry
	for _, e := range head {
		byPath[e.Name] = e
		if e.Mode != 0160000 && e.Size <= correctnessMaxBlobBytes {
			files = append(files, e)
		} else if e.Mode != 0160000 {
			gap(fmt.Sprintf("HEAD blob %s exceeds the 32 MiB single-body oracle limit", e.Name))
		}
	}
	chosen := map[string]bool{}
	var selected []*readerv1.Entry
	choose := func(e *readerv1.Entry) {
		if e != nil && !chosen[e.Name] && e.Mode != 0160000 && e.Size <= correctnessMaxBlobBytes {
			chosen[e.Name] = true
			selected = append(selected, e)
		}
	}
	for _, name := range []string{"README", "Makefile", "init/main.c", "Cargo.toml", "README.md"} {
		choose(byPath[name])
	}
	for i := 0; i < 48 && len(files) > 0; i++ {
		choose(files[i*(len(files)-1)/47])
	}
	largest := append([]*readerv1.Entry(nil), files...)
	sort.Slice(largest, func(i, j int) bool {
		if largest[i].Size != largest[j].Size {
			return largest[i].Size > largest[j].Size
		}
		return largest[i].Name < largest[j].Name
	})
	for i := 0; i < min(12, len(largest)); i++ {
		choose(largest[i])
	}
	for _, mode := range []uint32{0120000, 0100755} {
		for _, e := range files {
			if e.Mode == mode {
				choose(e)
				break
			}
		}
	}
	for _, e := range files {
		if e.Size == 0 {
			choose(e)
			break
		}
	}
	var oracleBytes int64
	materialized := map[string][]byte{}
	addBlob := func(sha string, e *readerv1.Entry, label string) {
		if e.Size > correctnessMaxBlobBytes {
			t.Fatalf("selected blob exceeds bound: %s", e.Name)
		}
		digest, exists := materialized[e.Oid]
		if !exists {
			if oracleBytes+e.Size > correctnessMaxOracleBytes {
				t.Fatalf("oracle body budget exceeded (%d bytes)", oracleBytes+e.Size)
			}
			f, err := os.OpenFile(filepath.Join(dir, e.Oid), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			h := sha256.New()
			err = readerOracleGit(ctx, source, io.MultiWriter(f, h), e.Size, "cat-file", "blob", e.Oid)
			closeErr := f.Close()
			if err != nil {
				t.Fatal(err)
			}
			if closeErr != nil {
				t.Fatal(closeErr)
			}
			st, err := os.Stat(filepath.Join(dir, e.Oid))
			if err != nil || st.Size() != e.Size {
				t.Fatalf("Git body size mismatch: %v", err)
			}
			digest = h.Sum(nil)
			materialized[e.Oid] = digest
			oracleBytes += e.Size
		}
		o.Blobs = append(o.Blobs, &readerv1.Blob{Oid: e.Oid, Size: e.Size, Path: e.Name, Revision: sha, Label: label, Sha256: digest})
	}
	for _, e := range selected {
		addBlob(tip, e, "head")
	}
	// The canonical empty blob is also read by OID when no checked-out path
	// exposes it. This is valid for the selected all-local object population.
	empty := "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
	var emptySize bytes.Buffer
	if err := readerOracleGit(ctx, source, &emptySize, 128, "cat-file", "-s", empty); err == nil && strings.TrimSpace(emptySize.String()) == "0" {
		addBlob(tip, &readerv1.Entry{Oid: empty}, "empty-object")
	} else {
		gap("canonical empty blob is unavailable from the source oracle")
	}
	historyPath := "init/main.c"
	if byPath[historyPath] == nil && len(selected) > 0 {
		historyPath = selected[0].Name
	}
	historical := strings.Fields(readerOracleGitText(t, ctx, source, "log", "-16", "--format=%H", tip, "--", historyPath))
	if len(historical) < 16 {
		gap(fmt.Sprintf("only %d related file revisions exist for %s", len(historical), historyPath))
	}
	for _, sha := range historical {
		addRevision(sha, sha)
		entries, err := correctnessParseTree(readerOracleGitBytes(t, ctx, source, 1<<20, "ls-tree", "-l", "-z", sha, "--", historyPath))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 1 && entries[0].Size <= correctnessMaxBlobBytes {
			addBlob(sha, entries[0], "related-history")
		} else {
			gap("historical file is absent or above the oracle limit: " + sha + ":" + historyPath)
		}
	}
	dirs := []string{"", "include/linux", "arch/x86/include/asm", "drivers", "tools"}
	for i := 0; i < min(4, len(largest)); i++ {
		if name := path.Dir(largest[i].Name); name != "." {
			dirs = append(dirs, name)
		}
	}
	seenDir := map[string]bool{}
	addDir := func(sha, name string) {
		k := sha + ":" + name
		if seenDir[k] {
			return
		}
		seenDir[k] = true
		expr := sha + "^{tree}"
		if name != "" {
			expr = sha + ":" + name
		}
		var oid bytes.Buffer
		if err := readerOracleGit(ctx, source, &oid, 128, "rev-parse", "--verify", expr); err != nil {
			gap("directory is unavailable from the source oracle: " + expr + ": " + err.Error())
			return
		}
		id := strings.TrimSpace(oid.String())
		var kind bytes.Buffer
		if err := readerOracleGit(ctx, source, &kind, 128, "cat-file", "-t", id); err != nil {
			t.Fatal(err)
		}
		if strings.TrimSpace(kind.String()) != "tree" {
			gap("selected directory oracle names a non-tree: " + expr)
			return
		}
		o.Directories = append(o.Directories, &readerv1.Directory{Oid: id, Path: name, Revision: sha, Entries: correctnessTree(t, ctx, source, id, false)})
	}
	for _, name := range dirs {
		addDir(tip, name)
	}
	if len(o.Revisions) > 1 {
		for _, r := range []*readerv1.Revision{o.Revisions[1], o.Revisions[len(o.Revisions)-1]} {
			addDir(r.Sha, "")
			if name := path.Dir(historyPath); name != "." {
				addDir(r.Sha, name)
			}
		}
	}
	// Choose compiled representation by stored metadata only. Its expected
	// entries still come entirely from Git, and the child reads the tree through
	// public ReadDir/Lookup without assuming it belongs to the selected checkout.
	backend, err := store.NewLocal(os.Getenv("GAT_CORRECTNESS_STORE"))
	if err != nil {
		t.Fatal(err)
	}
	m, _, err := readHead(ctx, backend)
	if err != nil {
		t.Fatal(err)
	}
	idx := &index{store: backend, cache: newCache(1 << 20), root: m.Root}
	after, inspected, compiled := "", 0, 0
	for inspected < 50000 && compiled < 2 {
		rows, err := idx.scan(ctx, "o/", after, 128)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			after = row.Key
			inspected++
			var obj object
			if err := unmarshal(row.Value, &obj); err != nil {
				t.Fatal(err)
			}
			if obj.Kind != "tree" || !strings.HasPrefix(obj.Directory.Pack, "index/dirs-") {
				continue
			}
			oid := strings.TrimPrefix(row.Key, "o/")
			if readerOracleGitText(t, ctx, source, "cat-file", "-t", oid) != "tree" {
				t.Fatal("stored compiled identity is not a Git tree", oid)
			}
			o.Directories = append(o.Directories, &readerv1.Directory{Oid: oid, Revision: tip, Path: "catalog-compiled:" + oid, Entries: correctnessTree(t, ctx, source, oid, false), ByOid: true})
			compiled++
			if compiled == 2 {
				break
			}
		}
	}
	if compiled == 0 {
		gap(fmt.Sprintf("no compiled directory found in the first %d stored identities", inspected))
	}
	t.Logf("selected %d compiled directories from %d persisted identity rows", compiled, inspected)
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > 8<<20 {
		t.Fatal("oracle metadata bound exceeded")
	}
	if err = os.WriteFile(filepath.Join(dir, "oracle.pb"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("prepared independent oracle: %d revisions, %d blob cases, %d directories, %d unique body bytes", len(o.Revisions), len(o.Blobs), len(o.Directories), oracleBytes)
	return o
}
