package repo

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"

	"golang.org/x/sync/errgroup"
)

type metadataReader struct {
	io.Reader
	files []*os.File
	dir   string
}

func (r *metadataReader) Close() error {
	var first error
	for _, f := range r.files {
		if err := f.Close(); first == nil {
			first = err
		}
	}
	if err := os.RemoveAll(r.dir); first == nil {
		first = err
	}
	return first
}

// Object type/size queries are independent. For large inputs, use up to four
// Git processes; results remain on disk until all processes have succeeded.
// The caller's size lookup and optional sort do not depend on response order.
// Small inputs avoid partitioning and extra process startup altogether.
func objectMetadata(ctx context.Context, ids *os.File, tmp, source string) (_ io.ReadCloser, retErr error) {
	stat, err := ids.Stat()
	if err != nil {
		return nil, err
	}
	workers := 1
	if stat.Size() > 8<<20 {
		workers = min(4, runtime.GOMAXPROCS(0))
	}
	dir, err := os.MkdirTemp(tmp, "object-metadata-*")
	if err != nil {
		return nil, err
	}
	result := &metadataReader{dir: dir}
	defer func() {
		if retErr != nil {
			result.Close()
		}
	}()
	inputs := make([]*os.File, workers)
	inputs[0] = ids
	if workers > 1 {
		// Only names/hints are partitioned, never object contents. Buffered
		// files keep memory independent of the number of source objects.
		defer func() {
			for _, f := range inputs {
				if f != nil {
					f.Close()
					os.Remove(f.Name())
				}
			}
		}()
		clear(inputs)
		writers := make([]*bufio.Writer, workers)
		for i := range inputs {
			inputs[i], err = os.CreateTemp(dir, "input-*")
			if err != nil {
				return nil, err
			}
			writers[i] = bufio.NewWriterSize(inputs[i], 64<<10)
		}
		scan := bufio.NewScanner(ids)
		scan.Buffer(make([]byte, 64<<10), 1<<20)
		// Preserve the exact input bytes, including a trailing carriage return
		// in a path hint; the metadata pass must not reinterpret file names.
		scan.Split(metadataLine)
		line := 0
		for scan.Scan() {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			w := writers[line/256%workers]
			line++
			if _, err := w.Write(scan.Bytes()); err != nil {
				return nil, err
			}
		}
		if err := scan.Err(); err != nil {
			return nil, err
		}
		for i, w := range writers {
			if err := w.Flush(); err != nil {
				return nil, err
			}
			if _, err := inputs[i].Seek(0, io.SeekStart); err != nil {
				return nil, err
			}
		}
	}
	for range workers {
		f, err := os.CreateTemp(dir, "output-*")
		if err != nil {
			return nil, err
		}
		result.files = append(result.files, f)
	}
	group, queryCtx := errgroup.WithContext(ctx)
	for i := range workers {
		group.Go(func() error {
			check := git(queryCtx, source, "cat-file", "--buffer", "--batch-check=%(objectname) %(objecttype) %(objectsize) %(rest)")
			check.Stdin, check.Stdout = inputs[i], result.files[i]
			var stderr bytes.Buffer
			check.Stderr = &stderr
			if err := check.Run(); err != nil {
				return fmt.Errorf("read object sizes: %w: %s", err, stderr.String())
			}
			return nil
		})
	}
	// An error cancels and joins every subprocess before closing any files.
	if err := group.Wait(); err != nil {
		return nil, err
	}
	readers := make([]io.Reader, len(result.files))
	for i, f := range result.files {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		readers[i] = f
	}
	result.Reader = io.MultiReader(readers...)
	return result, nil
}

func metadataLine(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[:i+1], nil
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}
