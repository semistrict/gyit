package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPatchLineDiffGitTieAndIndent(t *testing.T) {
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", "/dev/null")
	cases := []struct{ name, old, next string }{
		{"bidirectional match choice", "d\nd\nb\nb\na\na\na\nb\n", "b\nd\nc\nb\nc\nd\n"},
		{"repeated tokens", "a\nb\na\nc\nb\na\n", "b\na\nb\nc\na\nb\n"},
		{"replaced sections", "old one\n\nold two\n\nold three\n\nend\n", "new one\n\nnew two\n\nend\n"},
		{"comment boundary", "export A\n\n#\n# old description\nexport B\n", "export A\n\n#\n# new description\nexport C\n\n#\n# old description\nexport B\n"},
		{"indent boundary", "start\nif (a) {\n\tcall();\n}\n\nend\n", "start\nif (b) {\n\tcall();\n}\n\nif (a) {\n\tcall();\n}\n\nend\n"},
		{"missing final newline", "a\n\nb\n\na", "b\n\na\n\nb"},
	}
	for _, tc := range cases {
		for _, reverse := range []bool{false, true} {
			name, a, b := tc.name, tc.old, tc.next
			if reverse {
				name += " reverse"
				a, b = b, a
			}
			t.Run(name, func(t *testing.T) { checkPatchGit(t, a, b) })
		}
	}
}

func TestPatchMatcherMinimalAndBounded(t *testing.T) {
	rng := rand.New(rand.NewPCG(811, 947))
	for trial := 0; trial < 500; trial++ {
		a, b := make([]string, rng.IntN(20)), make([]string, rng.IntN(20))
		for _, lines := range [][]string{a, b} {
			for i := range lines {
				lines[i] = string(rune('a' + rng.IntN(4)))
			}
		}
		left, right, err := patchChangedLines(t.Context(), a, b)
		if err != nil {
			t.Fatal(err)
		}
		cost := 0
		for _, changed := range append(left, right...) {
			if changed {
				cost++
			}
		}
		// Independent dynamic programming cost, without matcher tie rules.
		row := make([]int, len(b)+1)
		for j := range row {
			row[j] = j
		}
		for i := range a {
			prior := row[0]
			row[0] = i + 1
			for j := range b {
				previous := row[j+1]
				if a[i] == b[j] {
					row[j+1] = prior
				} else {
					row[j+1] = 1 + min(row[j], row[j+1])
				}
				prior = previous
			}
		}
		if cost != row[len(b)] {
			t.Fatalf("nonminimal patch subdivision: %v %v got %d want %d", a, b, cost, row[len(b)])
		}
	}
	a, b := make([]string, 5000), make([]string, 5000)
	for i := range a {
		a[i] = fmt.Sprintf("line %d", i)
		b[len(b)-1-i] = a[i]
	}
	if _, _, err := patchChangedLines(t.Context(), a, b); err == nil || !strings.Contains(err.Error(), "bounded edit-work limit") {
		t.Fatalf("unbounded subdivision accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := patchChangedLines(ctx, a, b); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}

func checkPatchGit(t *testing.T, old, next string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	for name, data := range map[string]string{"old": old, "new": next} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.CommandContext(ctx, "git", "-c", "diff.algorithm=myers", "-c", "diff.indentHeuristic=true", "diff", "--no-index", "--no-ext-diff", "--no-color", "--unified=100000", "old", "new")
	cmd.Dir = dir
	want, err := cmd.Output()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("native patch oracle: %v\n%s", err, want)
	}
	start := bytes.Index(want, []byte("\n@@ "))
	if start < 0 {
		t.Fatalf("missing native patch hunk: %s", want)
	}
	start += bytes.IndexByte(want[start+1:], '\n') + 2
	want = want[start:]
	a, err := textLines([]byte(old))
	if err != nil {
		t.Fatal(err)
	}
	b, err := textLines([]byte(next))
	if err != nil {
		t.Fatal(err)
	}
	edits, err := patchLineDiff(ctx, a, b)
	if err != nil {
		t.Fatal(err)
	}
	var got strings.Builder
	for _, e := range edits {
		line := ""
		if e.kind == '+' {
			line = b[e.b]
		} else {
			line = a[e.a]
		}
		got.WriteByte(e.kind)
		got.WriteString(line)
		if !strings.HasSuffix(line, "\n") {
			got.WriteString("\n\\ No newline at end of file\n")
		}
	}
	if got.String() != string(want) {
		t.Fatalf("patch alignment differs from Git for old=%q new=%q:\n got:\n%s\nwant:\n%s", old, next, got.String(), want)
	}
}
