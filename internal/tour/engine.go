// Package tour runs the production repository reader against an embedded store.
// The shell and transport are demonstrations; repository decoding is shared.
package tour

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"gyit/internal/repo"
	"gyit/internal/store"
)

//go:embed testdata/repository.zip
var fixture []byte

type Event struct {
	ID      int    `json:"id"`
	Key     string `json:"key"`
	Offset  int64  `json:"offset"`
	Length  int64  `json:"length"`
	Bytes   int    `json:"bytes"`
	Preview string `json:"preview"`
	Phase   string `json:"phase"`
}
type Result struct {
	Output   string `json:"output"`
	Error    string `json:"error,omitempty"`
	CWD      string `json:"cwd"`
	SHA      string `json:"sha"`
	Requests int    `json:"requests"`
	Bytes    int    `json:"bytes"`
}

// Engine is a serial command session, like one shell. Callers serialize Run.
type Engine struct {
	objects               map[string][]byte
	expected              map[string][]byte
	reader                *repo.Repository
	snapshot              *repo.Snapshot
	cwd                   string
	emit                  func(Event)
	delay                 time.Duration
	requests, transferred int
}

func New() (*Engine, error) {
	z, err := zip.NewReader(bytes.NewReader(fixture), int64(len(fixture)))
	if err != nil {
		return nil, err
	}
	e := &Engine{objects: map[string][]byte{}, expected: map[string][]byte{}}
	total := 0
	for _, f := range z.File {
		r, err := f.Open()
		if err != nil {
			return nil, err
		}
		b, err := io.ReadAll(io.LimitReader(r, (8<<20)+1))
		r.Close()
		if err != nil {
			return nil, err
		}
		total += len(b)
		if len(b) > 8<<20 || total > 32<<20 {
			return nil, errors.New("fixture exceeds size limit")
		}
		if k, ok := strings.CutPrefix(f.Name, "objects/"); ok {
			e.objects[k] = b
		} else if k, ok := strings.CutPrefix(f.Name, "expected/"); ok {
			e.expected[k] = b
		}
	}
	return e, nil
}
func (e *Engine) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	b, ok := e.objects[key]
	if !ok {
		return nil, "", store.ErrNotFound
	}
	end := int64(len(b))
	if n >= 0 {
		if off < 0 || off > end || n > end-off {
			return nil, "", io.ErrUnexpectedEOF
		}
		end = off + n
	} else if n != -1 || off != 0 {
		return nil, "", errors.New("invalid whole-object range")
	}
	b = b[off:end]
	e.requests++
	id := e.requests
	event := Event{ID: id, Key: key, Offset: off, Length: n, Bytes: len(b), Phase: "request"}
	if e.emit != nil {
		e.emit(event)
	}
	if e.delay > 0 {
		timer := time.NewTimer(e.delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, "", ctx.Err()
		case <-timer.C:
		}
	}
	e.transferred += len(b)
	event.Phase = "response"
	event.Preview = hex.EncodeToString(b[:min(48, len(b))])
	if e.emit != nil {
		e.emit(event)
	}
	hash := sha256.Sum256(b)
	return bytes.Clone(b), hex.EncodeToString(hash[:]), nil
}
func (*Engine) Put(context.Context, string, []byte, string) error {
	return errors.New("demo object store is read-only")
}
func (e *Engine) open(ctx context.Context, revision string) error {
	if e.reader == nil {
		r, err := repo.New(e, 8<<20)
		if err != nil {
			return err
		}
		e.reader = r
	}
	current := ""
	if e.snapshot != nil {
		current = e.snapshot.SHA
	}
	s, err := e.reader.OpenRevision(ctx, revision, current)
	if err != nil {
		return err
	}
	e.snapshot = s
	return nil
}
func (e *Engine) name(p string) string {
	if strings.HasPrefix(p, "/") {
		return strings.TrimPrefix(path.Clean(p), "/")
	}
	return strings.TrimPrefix(path.Clean(path.Join("/", e.cwd, p)), "/")
}

// Run executes a bounded command adapter, not an operating-system shell.
// Pacing delays actual store requests solely to make them visible in the tour.
func (e *Engine) Run(ctx context.Context, command string, pacing time.Duration, emit func(Event)) Result {
	e.requests = 0
	e.transferred = 0
	e.emit = emit
	e.delay = max(0, min(pacing, 500*time.Millisecond))
	var out strings.Builder
	err := e.run(ctx, strings.Fields(command), &out)
	r := Result{Output: out.String(), CWD: "/" + e.cwd, Requests: e.requests, Bytes: e.transferred}
	if e.snapshot != nil {
		r.SHA = e.snapshot.SHA
	}
	if err != nil {
		r.Error = err.Error()
	}
	e.emit = nil
	return r
}
func (e *Engine) run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return nil
	}
	if args[0] == "gyit" {
		args = args[1:]
		if len(args) == 0 {
			return errors.New("expected gyit log or gyit checkout")
		}
	}
	cmd := args[0]
	args = args[1:]
	switch cmd {
	case "help":
		fmt.Fprintln(out, "ls [-al] [path]     cat <path>     cd <path>     pwd\ngyit log [-n 10] [path]     gyit checkout main|v1|SHA\ncache clear\n\nA command adapter, not an OS shell. No pipes, quoting or writes.\nReal Go reader · embedded object store · 8 MiB decoded memory cache.")
		return nil
	case "pwd":
		fmt.Fprintln(out, "/"+e.cwd)
		return nil
	case "cache":
		if len(args) != 1 || args[0] != "clear" {
			return errors.New("usage: cache clear")
		}
		sha := "HEAD"
		if e.snapshot != nil {
			sha = e.snapshot.SHA
		}
		if e.reader != nil {
			e.reader.Close()
		}
		e.reader = nil
		e.snapshot = nil
		if err := e.open(ctx, sha); err != nil {
			return err
		}
		fmt.Fprintln(out, "Decoded cache cleared; selected revision reopened (bootstrap reads shown).")
		return nil
	case "ls", "cat", "cd", "log", "checkout":
	default:
		return fmt.Errorf("unknown command %q; type help", cmd)
	}
	if e.snapshot == nil {
		if err := e.open(ctx, "HEAD"); err != nil {
			return err
		}
	}
	switch cmd {
	case "checkout":
		if len(args) != 1 {
			return errors.New("usage: gyit checkout main|v1|SHA")
		}
		if err := e.open(ctx, args[0]); err != nil {
			return err
		}
		e.cwd = ""
		fmt.Fprintf(out, "Selected %s\n", e.snapshot.SHA)
		return nil
	case "log":
		n := 10
		var paths []string
		for i := 0; i < len(args); i++ {
			if args[i] == "-n" {
				i++
				if i == len(args) {
					return errors.New("-n requires a count")
				}
				v, err := strconv.Atoi(args[i])
				if err != nil || v < 0 || v > 100 {
					return errors.New("count must be 0–100")
				}
				n = v
			} else if strings.HasPrefix(args[i], "-") {
				return errors.New("supported option: -n <count>")
			} else {
				paths = append(paths, e.name(args[i]))
			}
		}
		first := true
		return e.snapshot.LogWithOptions(ctx, repo.LogOptions{Count: n, Paths: paths, FullCommitIDs: true}, func(entry repo.LogEntry) error {
			if !first {
				fmt.Fprintln(out)
			}
			first = false
			return repo.WriteLogEntry(out, entry, false)
		})
	}
	long := false
	if cmd == "ls" && len(args) > 0 && strings.HasPrefix(args[0], "-") {
		if args[0] != "-l" && args[0] != "-al" && args[0] != "-la" {
			return errors.New("supported options: -l, -al")
		}
		long = true
		args = args[1:]
	}
	if len(args) > 1 || ((cmd == "cat" || cmd == "cd") && len(args) != 1) {
		return fmt.Errorf("usage: %s [path]", cmd)
	}
	name := e.cwd
	if len(args) == 1 {
		if strings.HasPrefix(args[0], "/") {
			name = strings.TrimPrefix(path.Clean(args[0]), "/")
		} else {
			name = e.name(args[0])
		}
	}
	entry, err := e.snapshot.Resolve(ctx, name)
	if err != nil {
		return err
	}
	switch cmd {
	case "cd":
		if entry.Mode != 0040000 {
			return errors.New("not a directory")
		}
		e.cwd = name
		return nil
	case "cat":
		if entry.Mode == 0040000 {
			return errors.New("is a directory")
		}
		if entry.Size > 1<<20 {
			return errors.New("demo output limit: 1 MiB")
		}
		b := make([]byte, entry.Size)
		n, err := e.snapshot.ReadAt(ctx, entry.OID, b, 0)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		_, err = out.Write(b[:n])
		return err
	case "ls":
		entries := []repo.Entry{entry}
		if entry.Mode == 0040000 {
			entries = nil
			after := ""
			for {
				batch, readErr := e.snapshot.ReadDirectory(ctx, entry, after, 128)
				if readErr != nil {
					return readErr
				}
				entries = append(entries, batch...)
				if len(batch) < 128 {
					break
				}
				if len(entries) >= 1024 {
					return errors.New("demo directory limit: 1024 entries")
				}
				after = batch[len(batch)-1].Name
			}
		}
		for _, v := range entries {
			suffix := ""
			if v.Mode == 0040000 {
				suffix = "/"
			}
			if long {
				fmt.Fprintf(out, "%06o %8d %s%s\n", v.Mode, v.Size, v.Name, suffix)
			} else {
				fmt.Fprintf(out, "%s%s\n", v.Name, suffix)
			}
		}
	}
	return nil
}
