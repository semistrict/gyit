package controlcli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPagerStreamsAndWaits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var out, stderr bytes.Buffer
	// The trailer appears after stdin EOF: the pager must outlive production.
	err := runPager(ctx, "cat; printf trailer", &out, &stderr, func(ctx context.Context, w io.Writer) error {
		_, err := io.WriteString(w, "commit output\n")
		return err
	})
	if err != nil || out.String() != "commit output\ntrailer" {
		t.Fatal(out.String(), stderr.String(), err)
	}
}
func TestPagerQuitCancelsProducer(t *testing.T) {
	for _, exit := range []string{"0", "7"} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		var out, stderr bytes.Buffer
		err := runPager(ctx, "exit "+exit, &out, &stderr, func(ctx context.Context, w io.Writer) error {
			// More than a pipe buffer guarantees exit unblocks an in-flight write.
			for i := 0; i < 100; i++ {
				if _, err := io.WriteString(w, strings.Repeat("line\n", 65536)); err != nil {
					return err
				}
			}
			return nil
		})
		cancel()
		if exit == "0" && err != nil {
			t.Fatal("normal pager quit became an error", err)
		}
		if exit == "7" && (err == nil || !strings.Contains(err.Error(), "exit status 7")) {
			t.Fatal("lost pager failure", err)
		}
	}
}
func TestPagerPreservesProducerErrors(t *testing.T) {
	stop := errors.New("upstream failed")
	for _, write := range []bool{false, true} {
		var out, stderr bytes.Buffer
		err := runPager(context.Background(), "cat; printf trailer", &out, &stderr, func(ctx context.Context, w io.Writer) error {
			if write {
				if _, err := io.WriteString(w, "partial\n"); err != nil {
					return err
				}
			}
			return stop
		})
		if !errors.Is(err, stop) {
			t.Fatal("lost upstream error", err)
		}
		if !write && out.Len() != 0 {
			t.Fatal("started pager without output")
		}
	}
}
func TestPagerLessEnvironment(t *testing.T) {
	// Environment is passed to the chosen pager, including explicit empty LESS.
	for _, value := range []string{"custom", ""} {
		t.Setenv("LESS", value)
		var out, stderr bytes.Buffer
		err := runPager(context.Background(), `cat >/dev/null; printf '%s' "$LESS"`, &out, &stderr, func(ctx context.Context, w io.Writer) error { _, err := io.WriteString(w, "content"); return err })
		if err != nil || out.String() != value {
			t.Fatal("LESS override", out.String(), err)
		}
	}
}
func TestRedirectedLogNeverStartsPager(t *testing.T) {
	t.Setenv("GYIT_PAGER", "exit 7")
	var out, stderr bytes.Buffer
	err := pageLog(context.Background(), &out, &stderr, func(ctx context.Context, w io.Writer) error { _, err := io.WriteString(w, "raw\n"); return err })
	if err != nil || out.String() != "raw\n" {
		t.Fatal(out.String(), err)
	}
}

func TestPagerReadingDoesNotConsumeFetchDeadline(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	var out, stderr bytes.Buffer
	// Slow reading must not backpressure a short-lived fetch. This is larger
	// than a pipe buffer; a direct fetch-to-pager pipe would miss the deadline.
	err := spoolLog(context.Background(), "sleep 0.3; cat; printf trailer", &out, &stderr, func(ctx context.Context, w io.Writer) error {
		fetch, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		for i := 0; i < 256; i++ {
			if err := fetch.Err(); err != nil {
				return err
			}
			if _, err := io.WriteString(w, strings.Repeat("x", 4096)); err != nil {
				return err
			}
		}
		return fetch.Err()
	})
	if err != nil || out.Len() != (1<<20)+len("trailer") || !strings.HasSuffix(out.String(), "trailer") {
		t.Fatal(out.Len(), err)
	}
	entries, err := os.ReadDir(os.Getenv("TMPDIR"))
	if err != nil || len(entries) != 0 {
		t.Fatal("spool leaked", entries, err)
	}
}

func TestPagerSpoolPreservesErrorsAndCleansUp(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	stop := errors.New("fetch failed")
	var out, stderr bytes.Buffer
	err := spoolLog(context.Background(), "exit 0", &out, &stderr, func(ctx context.Context, w io.Writer) error {
		if _, err := io.WriteString(w, strings.Repeat("partial\n", 65536)); err != nil {
			return err
		}
		return stop
	})
	if !errors.Is(err, stop) {
		t.Fatal("pager hid a fetch error", err)
	}
	entries, err := os.ReadDir(os.Getenv("TMPDIR"))
	if err != nil || len(entries) != 0 {
		t.Fatal("spool leaked", entries, err)
	}
	var data bytes.Buffer
	bounded := &limitedSpool{out: &data, left: 3}
	if n, err := bounded.Write([]byte("four")); err == nil || n != 0 || data.Len() != 0 {
		t.Fatal("spool exceeded its limit")
	}
}
