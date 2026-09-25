package repo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type streamingMetadata struct {
	f      *os.File
	pos    int64
	cancel context.CancelFunc
	ctx    context.Context
	done   chan struct{}
	err    error
}

func (m *streamingMetadata) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		stat, e := m.f.Stat()
		if e != nil {
			return 0, e
		}
		finished := false
		select {
		case <-m.done:
			finished = true
		default:
		}
		if stat.Size()-m.pos >= int64(len(p)) || finished {
			n, e := m.f.ReadAt(p, m.pos)
			m.pos += int64(n)
			if e == io.EOF && finished && m.err != nil {
				return n, m.err
			}
			return n, e
		}
		select {
		case <-m.ctx.Done():
			return 0, m.ctx.Err()
		case <-m.done:
		case <-time.After(2 * time.Millisecond):
		}
	}
}
func (m *streamingMetadata) Close() error {
	m.cancel()
	<-m.done
	e := m.f.Close()
	os.Remove(m.f.Name())
	if m.err != nil {
		return m.err
	}
	return e
}
func streamGitOutput(parent context.Context, tmp, source, input string, args ...string) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(parent)
	f, e := os.CreateTemp(tmp, "source-stream-*")
	if e != nil {
		cancel()
		return nil, e
	}
	cmd := git(ctx, source, args...)
	if input != "" {
		cmd.Stdin = strings.NewReader(input)
	}
	cmd.Stdout = f
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if e = cmd.Start(); e != nil {
		cancel()
		f.Close()
		os.Remove(f.Name())
		return nil, e
	}
	m := &streamingMetadata{f: f, cancel: cancel, ctx: ctx, done: make(chan struct{})}
	go func() {
		if e := cmd.Wait(); e != nil {
			m.err = fmt.Errorf("source stream: %w: %s", e, stderr.String())
		}
		close(m.done)
	}()
	return m, nil
}
