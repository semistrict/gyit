//go:build !js

package repo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gyit/internal/store"
)

func TestHistoryTreeEntry(t *testing.T) {
	for _, mode := range []string{"100644", "100755", "100664", "100600", "100744", "100655", "40775", "120777", "160000", strings.Repeat("0", 5000) + "100644"} {
		for _, name := range []string{"a", "raw-\xff", strings.Repeat("x", 255)} {
			raw := append([]byte(mode+" "+name+"\x00"), bytes.Repeat([]byte{127}, 20)...)
			want, err := readNativeTreeEntry(bufio.NewReader(bytes.NewReader(raw)))
			if err != nil {
				t.Fatal(err)
			}
			data := raw
			got, err := takeHistoryTreeEntry(&data)
			if err != nil || len(data) != 0 || string(got.name) != want.Name || hex.EncodeToString(got.oid) != want.OID || got.mode != want.Mode {
				t.Fatalf("%s %q: got %+v remaining=%d err=%v want %+v", mode, name, got, len(data), err, want)
			}
			if _, err := takeHistoryTreeEntry(&data); err != io.EOF {
				t.Fatalf("end of tree: %v", err)
			}
		}
	}
	for _, text := range []string{" ", "8 a\x00", "40000100644 a\x00", "100644", "100644 \x00", "100644 .\x00", "100644 ..\x00", "100644 a/b\x00", "100644 " + strings.Repeat("a", 256) + "\x00", "000000 a\x00"} {
		raw := append([]byte(text), make([]byte, 20)...)
		if _, err := takeHistoryTreeEntry(&raw); err == nil {
			t.Fatalf("accepted malformed tree entry %q", text)
		}
	}
	raw := append([]byte("100644 a\x00"), make([]byte, 20)...)
	for length := 1; length < len(raw); length++ {
		prefix := raw[:length]
		if _, err := takeHistoryTreeEntry(&prefix); err == nil {
			t.Fatalf("accepted truncated entry of length %d", length)
		}
	}
}

func TestHistoryTreeNameOrder(t *testing.T) {
	for _, a := range []string{"a", "a.c", "a0", "aa", "a\xff", "b"} {
		for _, b := range []string{"a", "a.c", "a0", "aa", "a\xff", "b"} {
			for _, ma := range []uint32{0100644, 0040000} {
				for _, mb := range []uint32{0100644, 0040000} {
					ka, kb := a, b
					if ma == 0040000 {
						ka += "/"
					}
					if mb == 0040000 {
						kb += "/"
					}
					got := compareHistoryTreeEntries(historyTreeEntry{name: []byte(a), mode: ma}, historyTreeEntry{name: []byte(b), mode: mb})
					want := strings.Compare(ka, kb)
					if (got < 0) != (want < 0) || (got > 0) != (want > 0) {
						t.Fatalf("%q/%o vs %q/%o: %d want sign %d", a, ma, b, mb, got, want)
					}
				}
			}
		}
	}
}

func TestHistoryEqualRunStopsAtCompleteEntry(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%v", indexed), func(t *testing.T) {
			ctx := context.WithValue(t.Context(), historySourceKey{}, &historySource{indexedByGit: indexed})
			checkHistoryEqualRunBoundaries(t, ctx)
		})
	}
}
func checkHistoryEqualRunBoundaries(t *testing.T, ctx context.Context) {
	var raw []byte
	var starts, oids []int
	for i := range 20 {
		starts = append(starts, len(raw))
		name := fmt.Sprintf("file-%02d", i)
		if i == 7 {
			name = strings.Repeat("long-name", 28)
		}
		raw = append(raw, []byte("100644 "+name+"\x00")...)
		oids = append(oids, len(raw))
		// Embedded NULs inside an OID are not entry boundaries.
		raw = append(raw, make([]byte, 20)...)
	}
	for i, off := range oids {
		for j := range 20 {
			other := bytes.Clone(raw)
			other[off+j] = 1
			left, right := raw, other
			if err := skipEqualHistoryEntries(ctx, &left, &right); err != nil {
				t.Fatal(err)
			}
			want := starts[i]
			if off+j < 64 {
				want = 0
			}
			if !bytes.Equal(left, raw[want:]) || !bytes.Equal(right, other[want:]) {
				t.Fatalf("skipped changed entry %d at OID byte %d", i, j)
			}
		}
	}
	for _, end := range starts {
		left, right := raw, raw[:end]
		if err := skipEqualHistoryEntries(ctx, &left, &right); err != nil {
			t.Fatal(err)
		}
		want := end
		if end < 64 {
			want = 0
		}
		if !bytes.Equal(left, raw[want:]) || !bytes.Equal(right, raw[want:end]) {
			t.Fatalf("unequal tree lengths advanced incorrectly at %d", end)
		}
	}
}

func TestHistoryTreeDeltaGitParity(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for _, name := range []string{"a", "a.c", "dir/nested", "gone", "mode", "sp ace"} {
		write(t, dir, name, []byte(name))
	}
	// Long unchanged runs surround sparse edits. Their byte prefixes include
	// names, terminators, and binary object IDs, none of which can be mistaken
	// for a complete tree-entry boundary when skipping an equal run.
	for i := range 512 {
		name := fmt.Sprintf("stable-%03d", i)
		write(t, dir, name, []byte(name))
	}
	commit(t, dir)
	before := command(t, dir, "rev-parse", "HEAD^{tree}")
	for _, name := range []string{"a", "gone"} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"a/new", "dir/nested", "new/deep/file"} {
		write(t, dir, name, []byte("changed"))
	}
	for _, i := range []int{0, 65, 256, 511} {
		write(t, dir, fmt.Sprintf("stable-%03d", i), []byte("changed"))
	}
	if err := os.Chmod(filepath.Join(dir, "mode"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a/new", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	commit(t, dir)
	after := command(t, dir, "rev-parse", "HEAD^{tree}")
	command(t, dir, "repack", "-ad")
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	const empty = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
	for _, pair := range [][2]string{{before, after}, {after, before}, {empty, after}, {after, empty}, {after, after}} {
		t.Run(fmt.Sprintf("%.7s-%.7s", pair[0], pair[1]), func(t *testing.T) {
			want := map[string]bool{}
			for _, name := range strings.Split(command(t, dir, "diff-tree", "-r", "--no-renames", "--name-only", "-z", pair[0], pair[1]), "\x00") {
				if name == "" {
					continue
				}
				want[""] = true
				for {
					want[name] = true
					cut := strings.LastIndexByte(name, '/')
					if cut < 0 {
						break
					}
					name = name[:cut]
				}
			}
			got := map[string]bool{}
			if err := p.historyTreeDelta(t.Context(), pair[1], pair[0], func(path string) error { got[path] = true; return nil }); err != nil {
				t.Fatal(err)
			}
			keys := func(m map[string]bool) []string {
				a := make([]string, 0, len(m))
				for k := range m {
					a = append(a, k)
				}
				slices.Sort(a)
				return a
			}
			if !slices.Equal(keys(got), keys(want)) {
				t.Fatalf("got %q want %q", keys(got), keys(want))
			}
		})
	}
}

func TestHistoryEqualImportRejectsTruncatedBoundaries(t *testing.T) {
	ctx := context.WithValue(t.Context(), historySourceKey{}, &historySource{indexedByGit: true})
	var prefix []byte
	for range 4 {
		prefix = append(prefix, []byte("100644 file\x00")...)
		prefix = append(prefix, make([]byte, 20)...)
	}
	for _, tail := range [][]byte{[]byte("100644 truncated"), append([]byte("100644 file\x00"), make([]byte, 19)...)} {
		a := append(bytes.Clone(prefix), tail...)
		b := bytes.Clone(a)
		if err := skipEqualHistoryEntries(ctx, &a, &b); err != io.ErrUnexpectedEOF {
			t.Fatalf("truncated entry accepted: %v", err)
		}
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	a, b := bytes.Clone(prefix), bytes.Clone(prefix)
	if err := skipEqualHistoryEntries(cancelCtx, &a, &b); err != context.Canceled {
		t.Fatalf("ignored cancellation: %v", err)
	}
}
