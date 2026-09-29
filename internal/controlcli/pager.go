package controlcli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
)

// Page only terminal output. Embedders and shell pipelines keep the raw stream.
func pageLog(ctx context.Context, stdout, stderr io.Writer, produce func(context.Context, io.Writer) error) error {
	return pageOutput(ctx, stdout, stderr, produce, false)
}

func pageOutput(ctx context.Context, stdout, stderr io.Writer, produce func(context.Context, io.Writer) error, buffered bool) error {
	if !terminalOutput(stdout) || os.Getenv("TERM") == "dumb" {
		return produce(ctx, stdout)
	}
	pager := "less"
	for _, key := range []string{"GYIT_PAGER", "GIT_PAGER", "PAGER"} {
		if value, ok := os.LookupEnv(key); ok {
			pager = value
			break
		}
	}
	if strings.TrimSpace(pager) == "" || strings.TrimSpace(pager) == "cat" {
		return produce(ctx, stdout)
	}
	// A missing default pager should not make the command unusable.
	if pager == "less" {
		if _, err := exec.LookPath("less"); err != nil {
			return produce(ctx, stdout)
		}
	}
	if buffered {
		return spoolOutput(ctx, pager, stdout, stderr, produce)
	}
	return runPager(ctx, pager, stdout, stderr, produce)
}

// Show retains its bounded buffered RPC, so time spent reading cannot exhaust
// the network deadline or leave a server worker blocked on terminal input.
// Use disk, not an output-sized memory buffer; never materialize repository data.
func spoolOutput(ctx context.Context, pager string, stdout, stderr io.Writer, produce func(context.Context, io.Writer) error) error {
	file, err := os.CreateTemp("", "gyit-log-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	spool := &limitedSpool{out: file, left: 256 << 20}
	produceErr := produce(ctx, spool)
	pos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if pos == 0 {
		return produceErr
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	pagerErr := runPager(ctx, pager, stdout, stderr, func(ctx context.Context, out io.Writer) error { _, err := io.Copy(out, file); return err })
	if produceErr != nil {
		return produceErr
	}
	return pagerErr
}

type limitedSpool struct {
	out  io.Writer
	left int64
}

func (s *limitedSpool) Write(data []byte) (int, error) {
	if int64(len(data)) > s.left {
		return 0, fmt.Errorf("log output exceeds the 256 MiB pager spool limit")
	}
	n, err := s.out.Write(data)
	s.left -= int64(n)
	return n, err
}

func runPager(ctx context.Context, pager string, stdout, stderr io.Writer, produce func(context.Context, io.Writer) error) error {
	// The request context ends when the pager exits, but successfully finishing
	// the request must not kill a pager while the user is still reading it.
	producerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	output := &pagerWriter{ctx: ctx, cancel: cancel, pager: pager, stdout: stdout, stderr: stderr}
	err := produce(producerCtx, output)
	if output.pipe == nil {
		return err
	} // No output: no empty pager for -n 0 or errors.
	_ = output.pipe.Close()
	<-output.done
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if output.waitErr != nil {
		return fmt.Errorf("pager: %w", output.waitErr)
	}
	if output.exitedDuringWrite || (errors.Is(err, context.Canceled) && producerCtx.Err() != nil) || errors.Is(err, syscall.EPIPE) {
		return nil
	}
	return err
}

type pagerWriter struct {
	ctx               context.Context
	cancel            context.CancelFunc
	pager             string
	stdout, stderr    io.Writer
	pipe              *os.File
	done              chan struct{}
	waitErr           error // Published by closing done.
	exitedDuringWrite bool
	once              sync.Once
	startErr          error
}

func (p *pagerWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	p.once.Do(func() {
		// A real OS pipe keeps exec from inserting a copying goroutine. Closing its
		// write end when the pager exits promptly unblocks a producer on a full pipe.
		read, write, err := os.Pipe()
		if err != nil {
			p.startErr = err
			return
		}
		cmd := exec.CommandContext(p.ctx, "sh", "-c", p.pager)
		if p.pager == "less" {
			cmd = exec.CommandContext(p.ctx, "less")
		}
		cmd.Stdin = read
		cmd.Stdout = p.stdout
		cmd.Stderr = p.stderr
		cmd.Env = os.Environ()
		if _, ok := os.LookupEnv("LESS"); !ok {
			cmd.Env = append(cmd.Env, "LESS=FRX")
		}
		if err := cmd.Start(); err != nil {
			read.Close()
			write.Close()
			p.startErr = err
			return
		}
		read.Close()
		p.pipe = write
		p.done = make(chan struct{})
		go func() { p.waitErr = cmd.Wait(); p.cancel(); _ = write.Close(); close(p.done) }()
	})
	if p.startErr != nil {
		return 0, p.startErr
	}
	n, err := p.pipe.Write(data)
	if errors.Is(err, os.ErrClosed) || errors.Is(err, syscall.EPIPE) {
		p.exitedDuringWrite = true
	}
	return n, err
}
