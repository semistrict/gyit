package repo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func historyStreamFixture(t *testing.T, format string) (string, string) {
	t.Helper()
	source := t.TempDir()
	command(t, source, "init", "--bare", "-q", "--object-format="+format, "-b", "main")
	var input strings.Builder
	for i := 1; i <= 40; i++ {
		ref := "main"
		if i >= 15 && i <= 24 {
			ref = "side"
		}
		if i >= 35 {
			ref = "other"
		}
		fmt.Fprintf(&input, "commit refs/heads/%s\nmark :%d\ncommitter Test <test@example.test> %d +0000\ndata 7\nfixture\n", ref, i, 1700000000+i)
		if i == 15 {
			input.WriteString("from :10\n")
		}
		if i == 25 {
			input.WriteString("merge :24\n")
		}
		if i == 35 {
			input.WriteString("from " + strings.Repeat("0", map[string]int{"sha1": 40, "sha256": 64}[format]) + "\n")
		}
		mode := "100644"
		if i%3 == 0 {
			mode = "100755"
		}
		fmt.Fprintf(&input, "M %s inline \"odd\\tname\\nline\"\ndata 5\nline\n\n", mode)
		if i%2 == 0 {
			input.WriteString("D swap\nM 100644 inline swap/child\ndata 3\nhi\n\n")
		} else {
			input.WriteString("D swap\nM 120000 inline swap\ndata 4\nfile\n")
		}
		input.WriteByte('\n')
	}
	c := git(t.Context(), source, "fast-import", "--quiet")
	c.Stdin = strings.NewReader(input.String())
	if out, e := c.CombinedOutput(); e != nil {
		t.Fatalf("fixture: %v %s", e, out)
	}
	return source, "refs/heads/main\nrefs/heads/side\nrefs/heads/other\n"
}

func serialHistory(t *testing.T, source, input string) []byte {
	t.Helper()
	cmd := git(t.Context(), source, "-c", "log.showSignature=false", "log", "--stdin", "--reverse", "--topo-order", "--format=%H%x00%T%x00%P", "-z", "-r", "--raw", "--no-abbrev", "--no-renames", "--no-ext-diff", "--no-textconv", "--diff-merges=first-parent", "--root")
	cmd.Stdin = strings.NewReader(input)
	out, e := cmd.Output()
	if e != nil {
		t.Fatal(e)
	}
	return out
}

func TestHistoryStreamMatchesGit(t *testing.T) {
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			source, input := historyStreamFixture(t, format)
			for _, incremental := range []bool{false, true} {
				t.Run(fmt.Sprint(incremental), func(t *testing.T) {
					selected := input
					if incremental {
						selected += "^" + command(t, source, "rev-parse", "main~6") + "\n"
					}
					want := serialHistory(t, source, selected)
					for _, workers := range []int{1, 4} {
						tmp := t.TempDir()
						r, e := openHistoryStream(t.Context(), source, selected, tmp, workers, 4)
						if e != nil {
							t.Fatal(e)
						}
						got, e := io.ReadAll(r)
						r.Close()
						if e != nil {
							t.Fatal(e)
						}
						if !bytes.Equal(got, want) {
							t.Fatalf("workers=%d history differs: got %d bytes want %d", workers, len(got), len(want))
						}
						entries, e := os.ReadDir(tmp)
						if e != nil || len(entries) != 0 {
							t.Fatalf("scratch remains: %v %v", entries, e)
						}
					}
				})
			}
		})
	}
}

func TestHistoryStreamCloseAndCancel(t *testing.T) {
	source, input := historyStreamFixture(t, "sha1")
	for _, action := range []string{"close", "cancel"} {
		t.Run(action, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			tmp := t.TempDir()
			r, e := openHistoryStream(ctx, source, input, tmp, 4, 1)
			if e != nil {
				t.Fatal(e)
			}
			// Read only a prefix, then abandon a stream with more queued batches.
			if _, e = io.ReadFull(r, make([]byte, 16)); e != nil {
				t.Fatal(e)
			}
			done := make(chan error, 1)
			if action == "cancel" {
				cancel()
				go func() { _, e := io.Copy(io.Discard, r); r.Close(); done <- e }()
			} else {
				go func() { done <- r.Close() }()
			}
			select {
			case e := <-done:
				if action == "cancel" && e == nil {
					t.Fatal("canceled stream reported success")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("stream did not join on cancellation")
			}
			entries, e := os.ReadDir(tmp)
			if e != nil || len(entries) != 0 {
				t.Fatalf("scratch remains: %v %v", entries, e)
			}
		})
	}
}

func TestHistoryStreamSourceError(t *testing.T) {
	source, _ := historyStreamFixture(t, "sha1")
	tmp := t.TempDir()
	r, e := openHistoryStream(t.Context(), source, "refs/heads/missing\n", tmp, 4, 4)
	if e != nil {
		t.Fatal(e)
	}
	_, e = io.Copy(io.Discard, r)
	r.Close()
	if e == nil {
		t.Fatal("missing source revision accepted")
	}
	entries, _ := os.ReadDir(tmp)
	if len(entries) != 0 {
		t.Fatalf("scratch remains: %s", filepath.Join(tmp, entries[0].Name()))
	}
}

func TestHistoryStreamDiffFailure(t *testing.T) {
	source, input := historyStreamFixture(t, "sha1")
	// A valid commit header can be enumerated even though its tree is absent.
	missing := strings.Repeat("1", 40)
	body := "tree " + missing + "\nauthor Test <test@example.test> 1700001000 +0000\ncommitter Test <test@example.test> 1700001000 +0000\n\nbroken tree\n"
	cmd := git(t.Context(), source, "hash-object", "-t", "commit", "-w", "--stdin")
	cmd.Stdin = strings.NewReader(body)
	oid, e := cmd.Output()
	if e != nil {
		t.Fatal(e)
	}
	command(t, source, "update-ref", "refs/heads/broken", strings.TrimSpace(string(oid)))
	tmp := t.TempDir()
	r, e := openHistoryStream(t.Context(), source, input+"refs/heads/broken\n", tmp, 4, 1)
	if e != nil {
		t.Fatal(e)
	}
	_, e = io.Copy(io.Discard, r)
	r.Close()
	if e == nil || !strings.Contains(e.Error(), "read history diffs") {
		t.Fatalf("worker failure lost: %v", e)
	}
	entries, e := os.ReadDir(tmp)
	if e != nil || len(entries) != 0 {
		t.Fatalf("scratch remains: %v %v", entries, e)
	}
}
