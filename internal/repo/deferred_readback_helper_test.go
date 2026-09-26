package repo

import (
	"bytes"
	"context"
	"fmt"
	"gyit/internal/store"
	"io"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func deferredCheckReadback(t *testing.T, backend store.Store, source, tip string, paths []string) []map[string]any {
	t.Helper()
	r, err := New(backend, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := time.Now()
	s, err := r.Open(ctx, tip)
	if err != nil {
		t.Fatal(err)
	}
	var entries []Entry
	after := ""
	for {
		page, err := s.ReadDir(ctx, s.Tree, after, 128)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, page...)
		if len(page) < 128 {
			break
		}
		after = page[len(page)-1].Name
	}
	rootSeconds := time.Since(start).Seconds()
	expected := command(t, source, "ls-tree", "-z", tip)
	var want []string
	for _, row := range strings.Split(expected, "\x00") {
		if row != "" {
			want = append(want, row)
		}
	}
	sort.Strings(want)
	var got []string
	for _, entry := range entries {
		kind := "blob"
		if entry.Mode == 040000 {
			kind = "tree"
		}
		if entry.Mode == 0160000 {
			kind = "commit"
		}
		got = append(got, fmt.Sprintf("%06o %s %s\t%s", entry.Mode, kind, entry.OID, entry.Name))
	}
	sort.Strings(got)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatal("root listing differs from Git")
	}
	reads := []map[string]any{{"name": "root", "seconds": rootSeconds, "over_1s": rootSeconds > 1, "ok": true}}
	for _, path := range paths {
		start := time.Now()
		entry, err := s.Resolve(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		data := make([]byte, entry.Size+1)
		n, err := s.ReadAt(ctx, entry.OID, data, 0)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		seconds := time.Since(start).Seconds()
		gitData, err := git(ctx, source, "show", tip+":"+path).Output()
		if err != nil {
			t.Fatal(err)
		}
		if int64(n) != entry.Size || !bytes.Equal(data[:n], gitData) {
			t.Fatalf("file mismatch: %s", path)
		}
		metadata := strings.Fields(command(t, source, "ls-tree", "-l", tip, "--", path))
		if len(metadata) < 5 || metadata[0] != fmt.Sprintf("%06o", entry.Mode) || metadata[1] != "blob" || metadata[2] != entry.OID || metadata[3] != strconv.FormatInt(entry.Size, 10) {
			t.Fatalf("file identity/mode/size mismatch: %s", path)
		}
		reads = append(reads, map[string]any{"name": "file:" + path, "seconds": seconds, "over_1s": seconds > 1, "ok": true})
	}
	return reads
}
