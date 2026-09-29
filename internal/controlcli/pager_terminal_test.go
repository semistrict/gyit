//go:build linux || darwin

package controlcli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Exercise pageLog's terminal selection, not just its underlying pipe helper.
// The producer cannot finish until the pager has consumed its first line.
func TestTerminalPagerShowsResultsBeforeProducerFinishes(t *testing.T) {
	script, err := exec.LookPath("script")
	if err != nil {
		t.Skip("terminal test requires script")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	args := []string{"-q", "/dev/null", os.Args[0], "-test.run=^TestTerminalPagerHelper$"}
	if runtime.GOOS == "linux" {
		quoted := "'" + strings.ReplaceAll(os.Args[0], "'", "'\\''") + "'"
		args = []string{"-q", "-c", quoted + " -test.run=^TestTerminalPagerHelper$", "/dev/null"}
	}
	cmd := exec.CommandContext(ctx, script, args...)
	cmd.Env = append(os.Environ(), "GYIT_TERMINAL_HELPER=1", "TERM=xterm", "GYIT_PAGER=IFS= read -r line; printf '%s\\n' \"$line\"; touch \"$GYIT_PAGER_MARKER\"; cat", "GYIT_PAGER_MARKER="+filepath.Join(t.TempDir(), "read"))
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "STREAM_FINISHED") {
		t.Fatalf("terminal pager withheld early results: %v\n%s", err, out)
	}
}

func TestTerminalPagerHelper(t *testing.T) {
	if os.Getenv("GYIT_TERMINAL_HELPER") != "1" {
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := pageLog(ctx, os.Stdout, os.Stderr, func(ctx context.Context, w io.Writer) error {
		if _, err := io.WriteString(w, "first available commit\n"); err != nil {
			return err
		}
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			if _, err := os.Stat(os.Getenv("GYIT_PAGER_MARKER")); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
		_, err := io.WriteString(w, "last available commit\n")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("STREAM_FINISHED")
}
