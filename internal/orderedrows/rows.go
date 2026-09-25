// Package orderedrows stages records that already have their final key order.
// Values are opaque protobuf records; the temporary framing contains only lengths.
package orderedrows

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const MaxKey = 128
const MaxValue = 8 << 20

type Metrics struct {
	Rows, KeyBytes, ValueBytes, FramedBytes uint64
}

// Spool owns one temporary file and bounded buffers. It has one writer, followed
// by one reader after Seal. Close removes its file, including on an error path.
type Spool struct {
	file           *os.File
	path           string
	writer         *bufio.Writer
	reader         *bufio.Reader
	previous       [MaxKey]byte
	previousSize   int
	row            []byte
	metrics        Metrics
	err            error
	sealed, closed bool
}

func New(parent string) (*Spool, error) {
	f, err := os.CreateTemp(parent, "ordered-archive-*")
	if err != nil {
		return nil, err
	}
	return &Spool{file: f, path: f.Name(), writer: bufio.NewWriterSize(f, 64<<10)}, nil
}

func (s *Spool) Metrics() Metrics { return s.metrics }

func (s *Spool) fail(err error) error {
	if s.err == nil {
		s.err = err
	}
	return s.err
}

func (s *Spool) Add(ctx context.Context, key, value []byte) error {
	if s.closed {
		return os.ErrClosed
	}
	if s.err != nil {
		return s.err
	}
	if s.sealed {
		return fmt.Errorf("ordered archive stream is sealed")
	}
	if err := ctx.Err(); err != nil {
		return s.fail(err)
	}
	if len(key) == 0 || len(key) > MaxKey || len(value) > MaxValue {
		return s.fail(fmt.Errorf("ordered archive row exceeds limits"))
	}
	if s.previousSize > 0 && bytes.Compare(s.previous[:s.previousSize], key) >= 0 {
		return s.fail(fmt.Errorf("ordered archive keys are not strictly increasing"))
	}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[4:], uint32(len(value)))
	for _, part := range [][]byte{header[:], key, value} {
		if _, err := s.writer.Write(part); err != nil {
			return s.fail(err)
		}
	}
	s.previousSize = copy(s.previous[:], key)
	s.metrics.Rows++
	s.metrics.KeyBytes += uint64(len(key))
	s.metrics.ValueBytes += uint64(len(value))
	s.metrics.FramedBytes += uint64(8 + len(key) + len(value))
	return nil
}

func (s *Spool) Seal(ctx context.Context) error {
	if s.closed {
		return os.ErrClosed
	}
	if s.err != nil {
		return s.err
	}
	if err := ctx.Err(); err != nil {
		return s.fail(err)
	}
	if s.sealed {
		return nil
	}
	if err := s.writer.Flush(); err != nil {
		return s.fail(err)
	}
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return s.fail(err)
	}
	s.writer = nil
	s.reader = bufio.NewReaderSize(s.file, 64<<10)
	s.sealed = true
	return nil
}

// Next returns borrowed slices valid until the next call. End of stream uses
// io.EOF, including for an empty sealed stream; partial records are errors.
func (s *Spool) Next(ctx context.Context) ([]byte, []byte, error) {
	if s.closed {
		return nil, nil, os.ErrClosed
	}
	if s.err != nil {
		return nil, nil, s.err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, s.fail(err)
	}
	if !s.sealed {
		return nil, nil, fmt.Errorf("ordered archive stream is not sealed")
	}
	var header [8]byte
	if _, err := io.ReadFull(s.reader, header[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil, io.EOF
		}
		return nil, nil, s.fail(err)
	}
	nk, nv := binary.LittleEndian.Uint32(header[:4]), binary.LittleEndian.Uint32(header[4:])
	if nk == 0 || nk > MaxKey || nv > MaxValue {
		return nil, nil, s.fail(fmt.Errorf("invalid ordered archive row lengths"))
	}
	n := int(nk + nv)
	if cap(s.row) < n {
		s.row = make([]byte, n)
	}
	s.row = s.row[:n]
	if _, err := io.ReadFull(s.reader, s.row); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, nil, s.fail(err)
	}
	return s.row[:nk], s.row[nk:], nil
}

func (s *Spool) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.writer, s.reader, s.row = nil, nil, nil
	return errors.Join(s.file.Close(), os.Remove(s.path))
}

// Cursor supplies ordered records using io.EOF for exhaustion. Returned slices
// remain valid until that cursor is called again.
type Cursor func(context.Context) ([]byte, []byte, error)

type mergeInput struct {
	next         Cursor
	key, value   []byte
	previous     [MaxKey]byte
	previousSize int
	ready, done  bool
}

func (s *mergeInput) advance(ctx context.Context) error {
	if s.done || s.ready {
		return nil
	}
	k, v, err := s.next(ctx)
	if errors.Is(err, io.EOF) {
		s.done = true
		return nil
	}
	if err != nil {
		return err
	}
	if len(k) == 0 || len(k) > MaxKey || len(v) > MaxValue {
		return fmt.Errorf("ordered merge row exceeds limits")
	}
	if s.previousSize > 0 && bytes.Compare(s.previous[:s.previousSize], k) >= 0 {
		return fmt.Errorf("ordered merge input is not strictly increasing")
	}
	s.previousSize = copy(s.previous[:], k)
	s.key, s.value, s.ready = k, v, true
	return nil
}

// Merge returns a bounded cursor over two disjoint ordered inputs. Duplicate
// keys are errors: an identity must never take both archive and fallback routes.
func Merge(left, right Cursor) Cursor {
	inputs := [2]mergeInput{{next: left}, {next: right}}
	pending := -1
	var failed error
	return func(ctx context.Context) ([]byte, []byte, error) {
		if failed != nil {
			return nil, nil, failed
		}
		if err := ctx.Err(); err != nil {
			failed = err
			return nil, nil, err
		}
		if pending >= 0 {
			inputs[pending].ready = false
			pending = -1
		}
		for i := range inputs {
			if err := inputs[i].advance(ctx); err != nil {
				failed = err
				return nil, nil, err
			}
		}
		if inputs[0].done && inputs[1].done {
			return nil, nil, io.EOF
		}
		pick := 0
		if inputs[0].done {
			pick = 1
		} else if !inputs[1].done {
			order := bytes.Compare(inputs[0].key, inputs[1].key)
			if order == 0 {
				failed = fmt.Errorf("duplicate archive and fallback key")
				return nil, nil, failed
			}
			if order > 0 {
				pick = 1
			}
		}
		pending = pick
		return inputs[pick].key, inputs[pick].value, nil
	}
}
