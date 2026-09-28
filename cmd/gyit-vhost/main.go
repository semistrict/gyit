//go:build linux && gyit_virtiofs

// Linux virtio-fs server for the GitHub namespace and prepared scan benchmarks.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"gyit/internal/githubfs"
	"gyit/internal/githubmount"
	"gyit/internal/store"
)

type measuredStore struct {
	store.Store
	requests, bytes, nanos atomic.Int64
	errors                 atomic.Int64
}

func (s *measuredStore) Get(ctx context.Context, k string, off, n int64) ([]byte, string, error) {
	start := time.Now()
	b, version, err := s.Store.Get(ctx, k, off, n)
	s.requests.Add(1)
	s.bytes.Add(int64(len(b)))
	s.nanos.Add(time.Since(start).Nanoseconds())
	if err != nil {
		s.errors.Add(1)
	}
	return b, version, err
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	if path := os.Getenv("GYIT_CPU_PROFILE"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			return err
		}
		defer pprof.StopCPUProfile()
	}
	location := flag.String("store", "", "durable GitHub repository store root (or a single repository with --prepared)")
	prepared := flag.Bool("prepared", false, "benchmark only: serve a fixed prepared repository without GitHub discovery")
	snapshotSHA := flag.String("sha", "", "full commit ID of the prepared snapshot")
	socket := flag.String("socket", "", "vhost-user socket")
	state := flag.String("state", "", "local process state directory")
	cache := flag.String("cache", "", "bounded decoded cache directory")
	target := flag.String("repository", "acme/large", "namespace owner/repository")
	remoteBase := flag.String("remote-base", "", "Git remote base; defaults to GitHub (file URLs support local integration fixtures)")
	metrics := flag.String("metrics", "", "request counters written on shutdown")
	metadataTTL := flag.Duration("metadata-ttl", time.Second, "metadata cache lifetime; live namespaces cap this at one second")
	flag.Parse()
	if *location == "" || *socket == "" || *state == "" || *cache == "" {
		log.Fatal("store, socket, state and cache are required")
	}
	if !*prepared && *snapshotSHA != "" {
		return fmt.Errorf("--sha requires --prepared (benchmark only)")
	}
	parts := strings.Split(*target, "/")
	if len(parts) != 2 {
		log.Fatal("repository must be owner/repository")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if !*prepared {
		source, err := githubfs.New(githubfs.Options{DataDir: *state, CacheDir: *cache, CacheBytes: 4 << 30, StoreRoot: *location, RemoteBase: *remoteBase})
		if err != nil {
			return err
		}
		defer source.Close()
		// Discovery, NOTICE and selected revisions can change in a live namespace.
		go githubmount.ServeVirtio(source, *socket, *metadataTTL)
		<-ctx.Done()
		return nil
	}
	backend, err := store.Open(ctx, *location, "", "")
	if err != nil {
		log.Fatal(err)
	}
	if c, ok := backend.(io.Closer); ok {
		defer c.Close()
	}
	measured := &measuredStore{Store: backend}
	defer func() {
		if *metrics != "" {
			b, _ := json.MarshalIndent(map[string]int64{"requests": measured.requests.Load(), "bytes": measured.bytes.Load(), "request_nanoseconds": measured.nanos.Load(), "errors": measured.errors.Load()}, "", "  ")
			if err := os.WriteFile(*metrics, append(b, '\n'), 0600); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}
	}()
	var source *githubfs.FS
	options := githubfs.Options{DataDir: *state, CacheDir: *cache, CacheBytes: 4 << 30}
	identity := githubfs.Target{Owner: parts[0], Repository: parts[1]}
	if *snapshotSHA == "" {
		return fmt.Errorf("--prepared requires --sha")
	}
	source, err = githubfs.NewPrepared(ctx, options, measured, identity, *snapshotSHA)

	if err != nil {
		return err
	}
	defer source.Close()
	go githubmount.ServeVirtio(source, *socket, *metadataTTL)
	<-ctx.Done()
	return nil
}
