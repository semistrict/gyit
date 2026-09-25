package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gat/internal/controlcli"
	"gat/internal/mount"
	"gat/internal/repo"
	"gat/internal/store"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		if !errors.Is(err, repo.ErrViewNoMatch) {
			fmt.Fprintln(os.Stderr, "gat:", err)
		}
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: gat <import|mount|status|switch|log|diff|blame|annotate|show|ls-tree|ls-files|cat-file|grep|branch|tag|show-ref|rev-parse|rev-list|merge-base|shortlog|ls|cat> [options]")
	}
	if controlcli.IsViewCommand(args[0]) || args[0] == "switch" || args[0] == "status" || args[0] == "log" || args[0] == "diff" || args[0] == "blame" || args[0] == "annotate" {
		return controlcli.Run(ctx, args, os.Stdout, os.Stderr)
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	location := f.String("store", "", "local path, file:///path, or s3://bucket/prefix")
	endpoint := f.String("endpoint", "", "S3-compatible endpoint URL (optional)")
	region := f.String("region", "", "AWS region (otherwise use standard AWS configuration)")
	source := f.String("repo", ".", "source Git repository")
	noDeltas := f.Bool("no-deltas", false, "store independent chunks without delta compression")
	depth := f.Int("delta-depth", 0, "maximum chunk delta depth (1..8; 0: retain native deltas when supported, otherwise 1)")
	candidates := f.Int("delta-candidates", 0, "chunk base candidates (1..8; 0: automatic, otherwise selects chunk conversion)")
	workers := f.Int("workers", 0, "import workers (0: automatic, at most four)")
	rev := f.String("rev", "", "import this revision's history (default: all refs)")
	tmp := f.String("temp-dir", "", "directory for disk-backed import staging")
	sha := f.String("sha", "", "full imported commit ID")
	cache := f.Int("cache-mib", 32, "maximum cached index and file data in MiB")
	socket := f.String("socket", "", "mount control socket path (required for mount)")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if *location == "" {
		return fmt.Errorf("--store is required")
	}
	s, err := store.Open(ctx, *location, *endpoint, *region)
	if err != nil {
		return err
	}
	if args[0] == "import" {
		last := time.Now()
		lastMode := ""
		stats, err := repo.Import(ctx, s, repo.ImportOptions{Repo: *source, Revision: *rev, DisableDeltas: *noDeltas, DeltaDepth: *depth, DeltaCandidates: *candidates, CompressionWorkers: *workers, TempDir: *tmp, Progress: func(s repo.Stats) {
			if s.ImportMode != lastMode {
				if s.FallbackReason != "" {
					fmt.Fprintf(os.Stderr, "import mode: %s (%s)\n", s.ImportMode, s.FallbackReason)
				} else {
					fmt.Fprintf(os.Stderr, "import mode: %s\n", s.ImportMode)
				}
				lastMode = s.ImportMode
			}
			if time.Since(last) >= 5*time.Second {
				fmt.Fprintf(os.Stderr, "imported %d objects, %d MiB read, %d MiB uploaded\n", s.Objects, s.Bytes>>20, s.UploadedBytes>>20)
				last = time.Now()
			}
		}})
		if err != nil {
			return err
		}
		fmt.Printf("generation %s (%d new objects, %d compressed bytes uploaded)\n", stats.Generation, stats.Objects, stats.UploadedBytes)
		return nil
	}
	if *cache < 0 || *cache > 4096 {
		return fmt.Errorf("--cache-mib must be between 0 and 4096")
	}
	r, err := repo.New(s, *cache<<20)
	if err != nil {
		return err
	}
	if args[0] == "mount" {
		if f.NArg() != 1 || *socket == "" {
			return fmt.Errorf("mount requires --sha, --socket, and a mountpoint")
		}
		return mount.Run(ctx, r, *sha, f.Arg(0), *socket)
	}
	snapshot, err := r.Open(ctx, *sha)
	if err != nil {
		return err
	}
	path := ""
	if f.NArg() > 0 {
		path = strings.Trim(f.Arg(0), "/")
	}
	e, err := snapshot.Resolve(ctx, path)
	if err != nil {
		return err
	}
	switch args[0] {
	case "ls":
		if e.Mode != 0040000 {
			return fmt.Errorf("path is not a directory")
		}
		after := ""
		for {
			entries, err := snapshot.ReadDir(ctx, e.OID, after, 128)
			if err != nil {
				return err
			}
			for _, e := range entries {
				fmt.Printf("%06o %12d %q\n", e.Mode, e.Size, e.Name)
				after = e.Name
			}
			if len(entries) < 128 {
				return nil
			}
		}
	case "cat":
		if e.Mode == 0040000 || e.Mode == 0160000 {
			return fmt.Errorf("path is a directory")
		}
		buf := make([]byte, repo.ChunkSize)
		off := int64(0)
		for {
			n, err := snapshot.ReadAt(ctx, e.OID, buf, off)
			if n > 0 {
				if _, writeErr := os.Stdout.Write(buf[:n]); writeErr != nil {
					return writeErr
				}
				off += int64(n)
			}
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
