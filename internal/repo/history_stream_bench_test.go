package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This benchmark walks only a verified 16,384-commit region of the cached
// source. It neither imports that repository nor fetches objects or refs.
func BenchmarkHistoryStreamLinux(b *testing.B) {
	if os.Getenv("GAT_LINUX_HISTORY_BENCH") != "1" {
		b.Skip("set GAT_LINUX_HISTORY_BENCH=1 with the cached Linux source")
	}
	source, e := filepath.Abs("../../.testdata/linux-repo.git")
	if e != nil {
		b.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(b.Context(), 10*time.Second)
	raw, e := git(ctx, source, "rev-list", "--max-count=16384", "--parents", "HEAD").Output()
	cancel()
	if e != nil {
		b.Fatal(e)
	}
	selected := make(map[string]bool)
	boundary := make(map[string]bool)
	rows := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(rows) != 16384 {
		b.Fatal("benchmark requires 16384 cached commits")
	}
	for _, row := range rows {
		f := strings.Fields(row)
		selected[f[0]] = true
	}
	for _, row := range rows {
		for _, p := range strings.Fields(row)[1:] {
			if !selected[p] {
				boundary[p] = true
			}
		}
	}
	var input strings.Builder
	input.WriteString("HEAD\n")
	for p := range boundary {
		fmt.Fprintf(&input, "^%s\n", p)
	}
	ctx, cancel = context.WithTimeout(b.Context(), 10*time.Second)
	cmd := git(ctx, source, "rev-list", "--stdin", "--reverse", "--topo-order")
	cmd.Stdin = strings.NewReader(input.String())
	actual, e := cmd.Output()
	cancel()
	if e != nil {
		b.Fatal(e)
	}
	ids := strings.Fields(string(actual))
	if len(ids) != len(selected) {
		b.Fatal("bounded region differs")
	}
	for _, id := range ids {
		if !selected[id] {
			b.Fatal("unexpected commit outside bounded region")
		}
	}
	measure := func(mode string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(b.Context(), 15*time.Second)
		defer cancel()
		hash := sha256.New()
		if mode == "serial" {
			cmd := git(ctx, source, "-c", "log.showSignature=false", "log", "--stdin", "--reverse", "--topo-order", "--format=%H%x00%T%x00%P", "-z", "-r", "--raw", "--no-abbrev", "--no-renames", "--no-ext-diff", "--no-textconv", "--diff-merges=first-parent", "--root")
			cmd.Stdin = strings.NewReader(input.String())
			cmd.Stdout = hash
			if e := cmd.Run(); e != nil {
				return nil, e
			}
		} else {
			tmp, e := os.MkdirTemp(b.TempDir(), "run-*")
			if e != nil {
				return nil, e
			}
			defer os.RemoveAll(tmp)
			r, e := openHistoryStream(ctx, source, input.String(), tmp, 8, historyDiffBatchSize)
			if e != nil {
				return nil, e
			}
			_, e = io.Copy(hash, r)
			closeErr := r.Close()
			if e != nil {
				return nil, e
			}
			if closeErr != nil {
				return nil, closeErr
			}
			left, e := os.ReadDir(tmp)
			if e != nil {
				return nil, e
			}
			if len(left) != 0 {
				return nil, fmt.Errorf("history scratch remains")
			}
		}
		return hash.Sum(nil), nil
	}
	want, e := measure("serial")
	if e != nil {
		b.Fatal(e)
	}
	for _, mode := range []string{"serial", "parallel8"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				got, e := measure(mode)
				elapsed := time.Since(start)
				if e != nil {
					b.Fatal(e)
				}
				if !bytes.Equal(got, want) {
					b.Fatal("history output changed")
				}
				if elapsed > time.Second {
					b.Logf("SLOW >1s: %.6fs", elapsed.Seconds())
				}
			}
			b.ReportMetric(16384, "commits/op")
			b.Logf("exact history SHA256=%x", want)
		})
	}
}
