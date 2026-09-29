//go:build !js

package repo

import (
	"fmt"
	"strings"
	"testing"

	"gyit/internal/store"
)

// Every advertised ref reaches the reference index, and staging rows never
// reach either index.
func TestImportAdvertisedReferencesPublishesExactRows(t *testing.T) {
	ctx := t.Context()
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProgressive(ctx, backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	head := strings.Repeat("a", 40)
	var advertisement strings.Builder
	fmt.Fprintf(&advertisement, "ref: refs/heads/main HEAD\n%s HEAD\n", head)
	const branches = 9
	for i := range branches {
		fmt.Fprintf(&advertisement, "%s refs/heads/b%d\n", strings.Repeat(fmt.Sprint(i), 40), i)
	}
	fmt.Fprintf(&advertisement, "%s refs/heads/main\n", head)
	tag, peeled := strings.Repeat("c", 40), strings.Repeat("d", 40)
	fmt.Fprintf(&advertisement, "%s refs/tags/v1\n%s refs/tags/v1^{}\n", tag, peeled)

	sha, err := p.ImportAdvertisedReferences(ctx, strings.NewReader(advertisement.String()), "main")
	if err != nil {
		t.Fatal(err)
	}
	if sha != head {
		t.Fatalf("resolved %s, want %s", sha, head)
	}

	main := p.index()
	for _, prefix := range []string{"a/", "o/", "r/", "s/"} {
		rows, err := main.scan(ctx, prefix, "", 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 0 {
			t.Fatalf("main index has %d %q rows, first %q", len(rows), prefix, rows[0].Key)
		}
	}
	var root pageRef
	if err := main.get(ctx, "refs-root", &root); err != nil {
		t.Fatal(err)
	}
	refs := &index{store: p.store, cache: p.cache, root: root, containers: true}
	var keys []string
	for _, prefix := range []string{"a/", "o/", "r/", "s/"} {
		rows, err := refs.scan(ctx, prefix, "", 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			keys = append(keys, row.Key)
		}
	}
	want := []string{"a/" + tag, "r/HEAD"}
	for i := range branches {
		want = append(want, fmt.Sprintf("r/refs/heads/b%d", i))
	}
	want = append(want, "r/refs/heads/main", "r/refs/tags/v1")
	if strings.Join(keys, "\n") != strings.Join(want, "\n") {
		t.Fatalf("reference index keys:\n%s\nwant:\n%s", strings.Join(keys, "\n"), strings.Join(want, "\n"))
	}
	var ref reference
	if err := refs.get(ctx, "r/HEAD", &ref); err != nil {
		t.Fatal(err)
	}
	if ref != (reference{Commit: head, ObjectID: head, SymbolicTarget: "refs/heads/main"}) {
		t.Fatalf("HEAD = %+v", ref)
	}
	if err := refs.get(ctx, "r/refs/tags/v1", &ref); err != nil {
		t.Fatal(err)
	}
	if ref != (reference{Commit: peeled, ObjectID: tag}) {
		t.Fatalf("v1 = %+v", ref)
	}
}
