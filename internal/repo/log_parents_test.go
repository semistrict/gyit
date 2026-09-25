package repo

import (
	"bytes"
	"strings"
	"testing"

	"gat/internal/store"
)

func TestMergeHeaderDisambiguatesParents(t *testing.T) {
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first := "1234567a" + strings.Repeat("0", 32)
	collision := "1234567b" + strings.Repeat("0", 32)
	second := "fedcba9" + strings.Repeat("0", 33)
	value, err := marshal(object{Kind: "blob", Size: 1})
	if err != nil {
		t.Fatal(err)
	}
	w := &indexWriter{ctx: t.Context(), store: backend, prefix: "parent-abbreviation"}
	e, err := w.save(page{Items: []item{{"o/" + first, value}, {"o/" + collision, value}, {"o/" + second, value}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = w.flush(); err != nil {
		t.Fatal(err)
	}
	s := &Snapshot{idx: &index{store: backend, cache: newCache(1 << 20), root: e.ID}}
	entry := LogEntry{SHA: strings.Repeat("f", 40), Parents: []string{first, second}}
	if err = s.abbreviateLogParents(t.Context(), &entry); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = WriteLogEntry(&out, entry, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "\nMerge: 1234567a fedcba9\n") {
		t.Fatalf("merge header must retain unique parent prefixes: %q", out.String())
	}
	if entry.Parents[0] != first || entry.Parents[1] != second {
		t.Fatal("display abbreviated the full parent identities")
	}
}
