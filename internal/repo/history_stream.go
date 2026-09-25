package repo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sync/errgroup"
)

const historyDiffBatchSize = 2048

type historyStream struct {
	*io.PipeReader
	cancel context.CancelFunc
	done   chan struct{}
}

func (s *historyStream) Close() error {
	s.cancel()
	err := s.PipeReader.Close()
	<-s.done
	return err
}

type historyDiffBatch struct {
	input, path string
	done        chan struct{}
	err         error
}

// Commit enumeration fixes parent-before-child order. Independent raw diffs run
// concurrently; the consumer sees their original order. The window bounds both
// queued OIDs and the number of temporary batch files. Raw diff output stays on
// disk, so an unusually large commit cannot grow the importer heap without bound.
// Closing the stream joins every subprocess before removing its scratch files.
func openHistoryStream(parent context.Context, source, input, tmp string, workers, batchSize int) (io.ReadCloser, error) {
	if workers < 1 || workers > 8 || batchSize < 1 || batchSize > historyDiffBatchSize {
		return nil, fmt.Errorf("invalid history pipeline limits")
	}
	dir, err := os.MkdirTemp(tmp, "history-diffs-*")
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	reader, writer := io.Pipe()
	stream := &historyStream{PipeReader: reader, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(stream.done)
		stop := context.AfterFunc(ctx, func() { writer.CloseWithError(ctx.Err()) })
		err := produceHistory(ctx, source, input, dir, workers, batchSize, writer)
		stop()
		cleanupErr := os.RemoveAll(dir)
		if err == nil {
			err = cleanupErr
		}
		writer.CloseWithError(err)
	}()
	return stream, nil
}

func produceHistory(ctx context.Context, source, input, dir string, workers, batchSize int, out io.Writer) error {
	group, ctx := errgroup.WithContext(ctx)
	jobs := make(chan *historyDiffBatch)
	ordered := make(chan *historyDiffBatch, workers)
	window := make(chan struct{}, workers)
	group.Go(func() error {
		defer close(jobs)
		defer close(ordered)
		cmd := git(ctx, source, "rev-list", "--stdin", "--reverse", "--topo-order")
		cmd.Stdin = strings.NewReader(input)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return err
		}
		if err = cmd.Start(); err != nil {
			return err
		}
		waited := false
		defer func() {
			if !waited {
				cmd.Process.Kill()
				cmd.Wait()
			}
		}()
		var batch strings.Builder
		count, ordinal := 0, 0
		submit := func() error {
			if count == 0 {
				return nil
			}
			select {
			case window <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			job := &historyDiffBatch{input: batch.String(), path: filepath.Join(dir, fmt.Sprintf("%08x", ordinal)), done: make(chan struct{})}
			ordinal++
			batch.Reset()
			count = 0
			select {
			case ordered <- job:
			case <-ctx.Done():
				return ctx.Err()
			}
			select {
			case jobs <- job:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		scan := bufio.NewScanner(stdout)
		scan.Buffer(make([]byte, 128), 128)
		for scan.Scan() {
			id := scan.Text()
			decoded, e := hex.DecodeString(id)
			if e != nil || (len(decoded) != 20 && len(decoded) != 32) {
				return fmt.Errorf("invalid enumerated history ID")
			}
			batch.WriteString(id)
			batch.WriteByte('\n')
			count++
			if count == batchSize {
				if err = submit(); err != nil {
					return err
				}
			}
		}
		if err = scan.Err(); err != nil {
			return err
		}
		if err = cmd.Wait(); err != nil {
			waited = true
			return fmt.Errorf("enumerate history: %w: %s", err, stderr.String())
		}
		waited = true
		return submit()
	})
	for range workers {
		group.Go(func() error {
			for {
				var job *historyDiffBatch
				select {
				case <-ctx.Done():
					return ctx.Err()
				case next, ok := <-jobs:
					if !ok {
						return nil
					}
					job = next
				}
				job.err = writeHistoryDiffBatch(ctx, source, job)
				close(job.done)
				if job.err != nil {
					return job.err
				}
			}
		})
	}
	group.Go(func() error {
		for {
			var job *historyDiffBatch
			select {
			case <-ctx.Done():
				return ctx.Err()
			case next, ok := <-ordered:
				if !ok {
					return nil
				}
				job = next
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-job.done:
			}
			if job.err != nil {
				return job.err
			}
			f, err := os.Open(job.path)
			if err != nil {
				return err
			}
			_, err = io.Copy(out, f)
			closeErr := f.Close()
			if err == nil {
				err = closeErr
			}
			if err != nil {
				return err
			}
			if err = os.Remove(job.path); err != nil {
				return err
			}
			<-window
		}
	})
	return group.Wait()
}

func writeHistoryDiffBatch(ctx context.Context, source string, job *historyDiffBatch) error {
	f, err := os.Create(job.path)
	if err != nil {
		return err
	}
	cmd := git(ctx, source, "-c", "log.showSignature=false", "log", "--stdin", "--no-walk=unsorted", "--format=%H%x00%T%x00%P", "-z", "-r", "--raw", "--no-abbrev", "--no-renames", "--no-ext-diff", "--no-textconv", "--diff-merges=first-parent", "--root")
	cmd.Stdin = strings.NewReader(job.input)
	cmd.Stdout = f
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err = cmd.Run()
	closeErr := f.Close()
	if err != nil {
		return fmt.Errorf("read history diffs: %w: %s", err, stderr.String())
	}
	return closeErr
}
