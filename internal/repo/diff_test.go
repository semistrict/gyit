package repo

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"gyit/internal/store"
)

func TestDiffMatchesGitFrequentBlankReplacement(t *testing.T) {
	// A common blank surrounded by enough replaced lines is discarded by
	// Git's default matcher. A minimal edit script instead keeps it as context.
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", "/dev/null")
	source := t.TempDir()
	command(t, source, "init", "-q")
	write(t, source, "file", []byte(strings.Repeat("\n", 4)))
	before := commit(t, source)
	write(t, source, "file", []byte("a\nb\nc\nd\ne\n\nf\ng\n"))
	after := commit(t, source)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Import(t.Context(), backend, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	r, err := New(backend, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	old, err := r.Open(t.Context(), before)
	if err != nil {
		t.Fatal(err)
	}
	current, err := r.Open(t.Context(), after)
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	if err := old.Diff(t.Context(), current, DiffOptions{Context: 3}, &got); err != nil {
		t.Fatal(err)
	}
	want, err := git(t.Context(), source, "-c", "diff.algorithm=myers", "diff", "--abbrev=7", "--no-ext-diff", "--no-renames", before, after).Output()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("diff differs from Git:\n got:\n%s\nwant:\n%s", got.Bytes(), want)
	}
}

func TestPatchLineDiffDoesNotSpendWorkMatchingAbsentLines(t *testing.T) {
	before := make([]string, 200)
	var after []string
	for i := range before {
		before[i] = "\n"
		for j := 0; j < 7; j++ {
			after = append(after, fmt.Sprintf("new line %d/%d\n", i, j))
		}
		after = append(after, "\n")
	}
	// All nonblank lines are provably inserted. They should not consume the
	// bounded Myers search budget after common-line matches are suppressed.
	edits, err := patchLineDiff(t.Context(), before, after)
	if err != nil {
		t.Fatal(err)
	}
	var result strings.Builder
	i, j := 0, 0
	for _, edit := range edits {
		if edit.a != i || edit.b != j {
			t.Fatal("invalid edit coordinates")
		}
		switch edit.kind {
		case ' ':
			if i >= len(before) || j >= len(after) || before[i] != after[j] {
				t.Fatal("matched different lines")
			}
			result.WriteString(before[i])
			i++
			j++
		case '-':
			if i >= len(before) {
				t.Fatal("removed beyond old file")
			}
			i++
		case '+':
			if j >= len(after) {
				t.Fatal("added beyond new file")
			}
			result.WriteString(after[j])
			j++
		}
	}
	if i != len(before) || j != len(after) || result.String() != strings.Join(after, "") {
		t.Fatal("patch did not reconstruct target")
	}
}

func TestPatchLineDiffProducesApplicableEdits(t *testing.T) {
	rng := rand.New(rand.NewPCG(73, 991))
	for trial := 0; trial < 1000; trial++ {
		a, b := make([]string, rng.IntN(45)), make([]string, rng.IntN(45))
		for _, lines := range [][]string{a, b} {
			for i := range lines {
				if rng.IntN(3) == 0 {
					lines[i] = "\n"
				} else {
					lines[i] = string(rune('a'+rng.IntN(6))) + "\n"
				}
			}
		}
		if trial%2 == 0 {
			a = append([]string{"shared prefix\n", "\n"}, a...)
			b = append([]string{"shared prefix\n", "\n"}, b...)
		}
		if trial%3 == 0 {
			a = append(a, "\n", "shared suffix\n")
			b = append(b, "\n", "shared suffix\n")
		}
		if trial%5 == 0 {
			for _, lines := range [][]string{a, b} {
				if len(lines) > 0 {
					lines[len(lines)-1] = strings.TrimSuffix(lines[len(lines)-1], "\n")
				}
			}
		}
		for _, pair := range [][2][]string{{a, b}, {b, a}} {
			a, b := pair[0], pair[1]
			edits, err := patchLineDiff(t.Context(), a, b)
			if err != nil {
				t.Fatal(err)
			}
			i, j := 0, 0
			for _, edit := range edits {
				if edit.a != i || edit.b != j {
					t.Fatalf("invalid coordinates: %+v; want %d,%d", edit, i, j)
				}
				switch edit.kind {
				case ' ':
					if i >= len(a) || j >= len(b) || a[i] != b[j] {
						t.Fatal("matched different lines")
					}
					i++
					j++
				case '-':
					if i >= len(a) {
						t.Fatal("removed beyond old file")
					}
					i++
				case '+':
					if j >= len(b) {
						t.Fatal("added beyond new file")
					}
					j++
				default:
					t.Fatalf("invalid edit kind %q", edit.kind)
				}
			}
			if i != len(a) || j != len(b) {
				t.Fatalf("incomplete patch: %d/%d %d/%d", i, len(a), j, len(b))
			}
		}
	}
}
