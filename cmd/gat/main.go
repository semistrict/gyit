package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
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
		return fmt.Errorf("usage: gat <import|mount|status|switch|checkout|log|diff|blame|annotate|show|ls-tree|ls-files|cat-file|grep|branch|tag|show-ref|rev-parse|rev-list|merge-base|shortlog|ls|cat> [options]")
	}
	if controlcli.IsViewCommand(args[0]) || args[0] == "switch" || args[0] == "checkout" || args[0] == "status" || args[0] == "log" || args[0] == "diff" || args[0] == "blame" || args[0] == "annotate" {
		return controlcli.Run(ctx, args, os.Stdout, os.Stderr)
	}
	switch args[0] {
	case "import", "mount", "ls", "cat":
	default:
		return fmt.Errorf("unknown subcommand %q; run gat without arguments for usage", args[0])
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

	f.Int("cache-mib", 0, "deprecated: no separate RAM data cache; ignored")
	diskDir := f.String("disk-cache-dir", "", "disk cache directory (default: user cache directory per mount)")
	diskMiB := f.Int64("disk-cache-mib", 4096, "maximum decoded disk cache in MiB (also reserves 20 GiB free disk); 0 disables")
	socket := f.String("socket", "", "optional legacy Unix control socket (default: virtual control file)")
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
	if *diskMiB < 0 || *diskMiB > 1<<30 {
		return fmt.Errorf("--disk-cache-mib must be between 0 and 1073741824")
	}
	if args[0] == "mount" && f.NArg() != 1 {
		return fmt.Errorf("mount requires --sha and a mountpoint")
	}
	var r *repo.Repository
	{
		identity := *location
		if !strings.Contains(identity, "://") {
			identity, err = filepath.Abs(identity)
			if err != nil {
				return err
			}
		}
		identity = *endpoint + "\x00" + *region + "\x00" + identity
		dir := *diskDir
		if dir == "" {
			base, e := os.UserCacheDir()
			if e != nil {
				return e
			}
			scope := args[0]
			if args[0] == "mount" {
				scope, err = filepath.Abs(f.Arg(0))
				if err != nil {
					return err
				}
			}
			dir = filepath.Join(base, "gat", "decoded-v1", fmt.Sprintf("%x", sha256.Sum256([]byte(scope))))
		}
		cached, e := repo.NewDisk(s, dir, identity, *diskMiB<<20)
		if e != nil {
			return e
		}
		defer cached.Close()
		r = cached
	}
	if r == nil {
		r, err = repo.New(s, 0)
	}
	if err != nil {
		return err
	}
	if args[0] == "mount" {
		if f.NArg() != 1 {
			return fmt.Errorf("mount requires --sha and a mountpoint")
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
