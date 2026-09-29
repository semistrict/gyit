// Package spill provides a bounded external key/value sort for import staging.
// Values with the same key retain the latest Add, including across disk runs.
package spill

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
)

const maxPart = 8 << 20
const mergeFanIn = 8

type row struct {
	data     []byte
	keySize  int
	sequence int
}

func (r row) key() []byte   { return r.data[:r.keySize] }
func (r row) value() []byte { return r.data[r.keySize:] }

// Sorter owns a temporary subdirectory. Close removes every staging file.
// The run budget charges payloads plus conservative per-record overhead.
// Merges open at most eight runs and retain at most one 16 MiB record per run,
// plus fixed I/O buffers. Data buffers and open-file count have fixed limits.
type Sorter struct {
	dir         string
	limit, used int
	rows        []row
	runs        []string
	closed      bool
}

func New(parent string, runBytes int) (*Sorter, error) {
	if runBytes < 1024 {
		return nil, fmt.Errorf("sort run budget must be at least 1024 bytes")
	}
	dir, err := os.MkdirTemp(parent, "sort-*")
	if err != nil {
		return nil, err
	}
	return &Sorter{dir: dir, limit: runBytes}, nil
}

// Add copies its inputs. Keys and values are each limited to 8 MiB, and an
// individual record including overhead must fit the requested run budget.
func (s *Sorter) Add(key, value []byte) error {
	if s.closed {
		return os.ErrClosed
	}
	cost := len(key) + len(value) + 64
	if len(key) > maxPart || len(value) > maxPart || cost > s.limit {
		return fmt.Errorf("sort record exceeds staging limit")
	}
	if s.used+cost > s.limit {
		if err := s.flush(); err != nil {
			return err
		}
	}
	data := make([]byte, len(key)+len(value))
	copy(data, key)
	copy(data[len(key):], value)
	s.rows = append(s.rows, row{data, len(key), len(s.rows)})
	s.used += cost
	return nil
}

func (s *Sorter) sortRows() {
	slices.SortFunc(s.rows, func(a, b row) int {
		if order := bytes.Compare(a.key(), b.key()); order != 0 {
			return order
		}
		return b.sequence - a.sequence
	})
}

func writeRow(w *bufio.Writer, key, value []byte) error {
	var sizes [8]byte
	binary.LittleEndian.PutUint32(sizes[:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(sizes[4:], uint32(len(value)))
	if _, err := w.Write(sizes[:]); err != nil {
		return err
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	_, err := w.Write(value)
	return err
}

func (s *Sorter) save(write func(*bufio.Writer) error) (string, error) {
	f, err := os.CreateTemp(s.dir, "run-*")
	if err != nil {
		return "", err
	}
	w := bufio.NewWriterSize(f, 64<<10)
	err = write(w)
	if err == nil {
		err = w.Flush()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

func (s *Sorter) flush() error {
	if len(s.rows) == 0 {
		return nil
	}
	s.sortRows()
	path, err := s.save(func(w *bufio.Writer) error {
		for i, r := range s.rows {
			if i > 0 && bytes.Equal(r.key(), s.rows[i-1].key()) {
				continue
			}
			if err := writeRow(w, r.key(), r.value()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.runs = append(s.runs, path)
	clear(s.rows)
	s.rows, s.used = s.rows[:0], 0
	return nil
}

// Walk emits ordered, unique keys. Slices are read-only and valid only during the callback.
// Repeated Walk calls are supported; Add may also append later updates.
func (s *Sorter) Walk(ctx context.Context, emit func(key, value []byte) error) error {
	if s.closed {
		return os.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(s.runs) == 0 {
		s.sortRows()
		for i, r := range s.rows {
			if err := ctx.Err(); err != nil {
				return err
			}
			if i > 0 && bytes.Equal(r.key(), s.rows[i-1].key()) {
				continue
			}
			if err := emit(r.key(), r.value()); err != nil {
				return err
			}
		}
		return nil
	}
	if err := s.flush(); err != nil {
		return err
	}
	for len(s.runs) > mergeFanIn {
		var next []string
		for start := 0; start < len(s.runs); start += mergeFanIn {
			group := s.runs[start:min(start+mergeFanIn, len(s.runs))]
			path, err := s.save(func(w *bufio.Writer) error {
				return merge(ctx, group, func(key, value []byte) error { return writeRow(w, key, value) })
			})
			if err != nil {
				// Keep the input runs intact until the complete pass succeeds.
				for _, path := range next {
					_ = os.Remove(path)
				}
				return err
			}
			next = append(next, path)
		}
		for _, path := range s.runs {
			if err := os.Remove(path); err != nil {
				return err
			}
		}
		s.runs = next
	}
	return merge(ctx, s.runs, emit)
}

// Take consumes another sorter's records, treating them as newer than this
// sorter's existing records. It moves its staging directory instead of reading,
// copying and sorting every record again. Both directories must be on the same
// filesystem, and other must have an equal or smaller run budget. A successful
// transfer closes other; its later Close cannot remove the transferred files.
// On failure both sorters retain their records and can still be closed or read.
func (s *Sorter) Take(other *Sorter) error {
	if s.closed || other.closed {
		return os.ErrClosed
	}
	if s == other || other.limit > s.limit {
		return fmt.Errorf("invalid sort transfer or incompatible run budget")
	}
	if len(s.runs) == 0 && len(other.runs) == 0 && other.used <= s.limit-s.used {
		// Small stages keep their owned buffers and never write a disk run.
		if err := os.RemoveAll(other.dir); err != nil {
			return err
		}
		base := len(s.rows)
		for _, r := range other.rows {
			r.sequence += base
			s.rows = append(s.rows, r)
		}
		s.used += other.used
		other.rows, other.dir, other.closed = nil, "", true
		return nil
	}
	if err := s.flush(); err != nil {
		return err
	}
	if err := other.flush(); err != nil {
		return err
	}
	// A private parent gives rename a non-existent destination without a
	// name-allocation race. The single rename transfers even nested inputs.
	dir, err := os.MkdirTemp(s.dir, "take-*")
	if err != nil {
		return err
	}
	destination := filepath.Join(dir, "runs")
	paths := make([]string, len(other.runs))
	for i, path := range other.runs {
		relative, err := filepath.Rel(other.dir, path)
		if err != nil {
			_ = os.Remove(dir)
			return err
		}
		paths[i] = filepath.Join(destination, relative)
	}
	if err := os.Rename(other.dir, destination); err != nil {
		_ = os.Remove(dir)
		return err
	}
	s.runs = append(s.runs, paths...)
	other.rows, other.runs, other.dir, other.closed = nil, nil, "", true
	return nil
}

func (s *Sorter) Close() error {
	s.closed = true
	s.rows = nil
	if s.dir == "" {
		return nil
	}
	return os.RemoveAll(s.dir)
}

type run struct {
	file  *os.File
	input *bufio.Reader
	row
	order int
}

func (r *run) advance() error {
	var sizes [8]byte
	if _, err := io.ReadFull(r.input, sizes[:]); err != nil {
		return err
	}
	nk, nv := binary.LittleEndian.Uint32(sizes[:4]), binary.LittleEndian.Uint32(sizes[4:])
	if nk > maxPart || nv > maxPart {
		return fmt.Errorf("invalid sort run record size")
	}
	n := int(nk + nv)
	if cap(r.data) < n {
		r.data = make([]byte, n)
	}
	r.data, r.keySize = r.data[:n], int(nk)
	_, err := io.ReadFull(r.input, r.data)
	if err == io.EOF && n > 0 {
		err = io.ErrUnexpectedEOF
	}
	return err
}

type runHeap []*run

func (h runHeap) Len() int { return len(h) }
func (h runHeap) Less(i, j int) bool {
	if order := bytes.Compare(h[i].key(), h[j].key()); order != 0 {
		return order < 0
	}
	return h[i].order > h[j].order
}
func (h runHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *runHeap) Push(x any)   { *h = append(*h, x.(*run)) }
func (h *runHeap) Pop() any {
	last := len(*h) - 1
	r := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return r
}

func merge(ctx context.Context, paths []string, emit func(key, value []byte) error) error {
	var readers []*run
	defer func() {
		for _, r := range readers {
			r.file.Close()
		}
	}()
	var h runHeap
	advance := func(r *run) error {
		if err := r.advance(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		heap.Push(&h, r)
		return nil
	}
	for i, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		r := &run{file: f, input: bufio.NewReaderSize(f, 64<<10), order: i}
		readers = append(readers, r)
		if err := advance(r); err != nil {
			return err
		}
	}
	for len(h) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		winner := heap.Pop(&h).(*run)
		if err := emit(winner.key(), winner.value()); err != nil {
			return err
		}
		// Each run is already unique. Keep the winning buffer unchanged while
		// discarding older copies of this key in the other run heads.
		for len(h) > 0 && bytes.Equal(h[0].key(), winner.key()) {
			if err := advance(heap.Pop(&h).(*run)); err != nil {
				return err
			}
		}
		if err := advance(winner); err != nil {
			return err
		}
	}
	return nil
}
