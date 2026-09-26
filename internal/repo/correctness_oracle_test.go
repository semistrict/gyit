package repo

import (
	"bytes"
	"context"
	"fmt"
	"gyit/internal/store"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

type correctnessOutput struct{ bytes.Buffer }

func (w *correctnessOutput) Write(p []byte) (int, error) {
	if len(p) > (16<<20)-w.Len() {
		return 0, fmt.Errorf("bounded Git oracle exceeded16MiB")
	}
	return w.Buffer.Write(p)
}

// Stdout is data. Stderr is retained separately and never enters an OID/parser.
func correctnessGit(t *testing.T, ctx context.Context, source, input string, args ...string) []byte {
	t.Helper()
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", source}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_NO_REPLACE_OBJECTS=1", "GIT_NO_LAZY_FETCH=1")
	cmd.WaitDelay = time.Second
	cmd.Stdin = strings.NewReader(input)
	var output correctnessOutput
	var stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("Git oracle %v: %v: %s", args, err, stderr.Bytes())
	}
	return bytes.Clone(output.Bytes())
}

type correctnessRef struct {
	Name           string `json:"name"`
	Commit         string `json:"commit"`
	ObjectID       string `json:"object_id"`
	SymbolicTarget string `json:"symbolic_target"`
}
type correctnessSourceRefs struct {
	Refs          []correctnessRef `json:"refs"`
	Tips          []string         `json:"tips"`
	NonCommitRefs int              `json:"non_commit_refs"`
	Seconds       float64          `json:"seconds"`
	Over1s        bool             `json:"over_1s"`
}

func correctnessExpectedRefs(t *testing.T, ctx context.Context, source, expectedHead string) correctnessSourceRefs {
	t.Helper()
	started := time.Now()
	raw := correctnessGit(t, ctx, source, "", "for-each-ref", "--format=%(refname)%00%(objectname)%00%(symref)")
	var rows []correctnessRef
	for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\x00")
		if len(parts) != 3 || len(parts[1]) != 40 {
			t.Fatal("invalid independent source ref")
		}
		rows = append(rows, correctnessRef{Name: parts[0], ObjectID: parts[1], SymbolicTarget: parts[2]})
	}
	head := strings.TrimSpace(string(correctnessGit(t, ctx, source, "", "rev-parse", "--verify", "HEAD")))
	if head != expectedHead {
		t.Fatal("source HEAD changed")
	}
	// The fixed complete source has symbolic HEAD. A changed shape is a refusal,
	// not permission to substitute a different fixture or omit HEAD coverage.
	symbolic := strings.TrimSpace(string(correctnessGit(t, ctx, source, "", "symbolic-ref", "-q", "HEAD")))
	rows = append(rows, correctnessRef{Name: "HEAD", ObjectID: head, SymbolicTarget: symbolic})
	var input strings.Builder
	for _, row := range rows {
		fmt.Fprintf(&input, "%s^{}\n", row.ObjectID)
	}
	peeled := strings.Split(strings.TrimSuffix(string(correctnessGit(t, ctx, source, input.String(), "cat-file", "--batch-check=%(objectname) %(objecttype)")), "\n"), "\n")
	if len(peeled) != len(rows) {
		t.Fatal("incomplete independent ref peeling")
	}
	result := correctnessSourceRefs{}
	tips := map[string]bool{}
	for i, line := range peeled {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatal("invalid peeled source ref")
		}
		if fields[1] != "commit" && fields[1] != "tree" && fields[1] != "blob" {
			t.Fatal("invalid peeled source ref kind")
		}
		if fields[1] != "commit" {
			result.NonCommitRefs++
		}
		if len(fields[0]) != 40 {
			t.Fatal("invalid peeled commit")
		}
		rows[i].Commit = fields[0]
		result.Refs = append(result.Refs, rows[i])
		if fields[1] == "commit" {
			tips[fields[0]] = true
		}
	}
	sort.Slice(result.Refs, func(i, j int) bool { return result.Refs[i].Name < result.Refs[j].Name })
	for id := range tips {
		result.Tips = append(result.Tips, id)
	}
	sort.Strings(result.Tips)
	if len(result.Tips) == 0 {
		t.Fatal("source has no independent commit tips")
	}
	result.Seconds = time.Since(started).Seconds()
	result.Over1s = result.Seconds > 1
	return result
}

func correctnessCheckRefs(t *testing.T, ctx context.Context, backend store.Store, m manifest, want correctnessSourceRefs) map[string]any {
	t.Helper()
	started := time.Now()
	tips := append([]string(nil), m.Tips...)
	sort.Strings(tips)
	if !reflect.DeepEqual(tips, want.Tips) {
		t.Fatalf("published tips differ from independent source refs: got%d want%d", len(tips), len(want.Tips))
	}
	idx := &index{store: backend, cache: newCache(DefaultCacheBytes), root: m.Refs}
	refs, err := readViewRefs(ctx, idx)
	if err != nil {
		t.Fatal(err)
	}
	var got []correctnessRef
	for _, ref := range refs {
		got = append(got, correctnessRef{ref.name, ref.Commit, ref.ObjectID, ref.SymbolicTarget})
	}
	sort.Slice(got, func(i, j int) bool { return got[i].Name < got[j].Name })
	if !reflect.DeepEqual(got, want.Refs) {
		t.Fatalf("published exact refs differ: got%d want%d", len(got), len(want.Refs))
	}
	seconds := time.Since(started).Seconds()
	return map[string]any{"operation": "published_refs", "non_commit_refs": want.NonCommitRefs, "full_git_refs_parity": true, "seconds": seconds, "over_1s": seconds > 1, "ok": true, "refs": len(got), "tips": len(tips)}
}

func correctnessParseEntries(t *testing.T, raw []byte) []Entry {
	t.Helper()
	var entries []Entry
	for _, row := range bytes.Split(raw, []byte{0}) {
		if len(row) == 0 {
			continue
		}
		header, name, ok := bytes.Cut(row, []byte{'\t'})
		fields := strings.Fields(string(header))
		if !ok || len(fields) != 4 || len(fields[2]) != 40 {
			t.Fatal("invalid independent directory row")
		}
		mode, err := strconv.ParseUint(fields[0], 8, 32)
		if err != nil {
			t.Fatal(err)
		}
		size := int64(0)
		if fields[1] == "blob" {
			size, err = strconv.ParseInt(fields[3], 10, 64)
			if err != nil || size < 0 {
				t.Fatal("invalid independent file size")
			}
		}
		entries = append(entries, Entry{Name: string(name), OID: fields[2], Mode: uint32(mode), Size: size})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries
}
