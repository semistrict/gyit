// Command benchmark_reads measures metadata I/O for explicitly selected directories.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"gyit/internal/repo"
	"gyit/internal/store"
)

type measuredStore struct {
	store.Store
	gets, bytes, payload int
}

func (s *measuredStore) Get(ctx context.Context, key string, off, size int64) ([]byte, string, error) {
	b, token, err := s.Store.Get(ctx, key, off, size)
	s.gets++
	s.bytes += len(b)
	if len(key) >= 6 && key[:6] == "packs/" {
		s.payload++
	}
	return b, token, err
}

func run() error {
	location := flag.String("store", "", "existing local store")
	revision := flag.String("rev", "master", "imported revision")
	flag.Parse()
	if *location == "" || flag.NArg() == 0 {
		return fmt.Errorf("supply --store, --rev and directory paths (use . for root)")
	}
	backend, err := store.NewLocal(*location)
	if err != nil {
		return err
	}
	ctx := context.Background()
	fmt.Println("path\tstate\tseconds\tentries\tGETs\tfetched_bytes\tpayload_GETs\tflag")
	for _, path := range flag.Args() {
		if path == "." {
			path = ""
		}
		measured := &measuredStore{Store: backend}
		r, err := repo.New(measured, 32<<20)
		if err != nil {
			return err
		}
		for _, state := range []string{"cold", "warm"} {
			measured.gets, measured.bytes, measured.payload = 0, 0, 0
			start := time.Now()
			s, err := r.OpenRevision(ctx, *revision, "")
			if err != nil {
				return err
			}
			e, err := s.Resolve(ctx, path)
			if err != nil {
				return err
			}
			after, entries := "", 0
			for {
				batch, err := s.ReadDir(ctx, e.OID, after, 128)
				if err != nil {
					return err
				}
				entries += len(batch)
				if len(batch) < 128 {
					break
				}
				after = batch[len(batch)-1].Name
			}
			elapsed := time.Since(start)
			flag := ""
			if elapsed > time.Second {
				flag = "SLOW"
			}
			fmt.Printf("%s\t%s\t%.6f\t%d\t%d\t%d\t%d\t%s\n", path, state,
				elapsed.Seconds(), entries, measured.gets, measured.bytes, measured.payload, flag)
			if measured.payload != 0 {
				return fmt.Errorf("directory navigation fetched file-content packs")
			}
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
