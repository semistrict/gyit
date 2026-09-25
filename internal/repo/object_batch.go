package repo

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

type objectRequest struct{ oid, hint string }

// objectBatch queues only object names, never file bodies. An explicit flush
// lets Git fill its output buffer without a pipe round trip for each object.
// The caller consumes every queued response before asking for the next batch.
type objectBatch struct {
	// direct preserves enumeration order while another source reader owns the body.
	direct  func(string) bool
	scan    *bufio.Scanner
	input   *bufio.Writer
	include func(string) (bool, error)
	oidSize int
	pending []objectRequest
	next    int
}

func (b *objectBatch) take() (string, string, error) {
	if b.next == len(b.pending) {
		clear(b.pending)
		b.pending, b.next = b.pending[:0], 0
		hintBytes := 0
		for len(b.pending) < 128 && hintBytes < 512<<10 && b.scan.Scan() {
			oid, hint, _ := strings.Cut(b.scan.Text(), " ")
			if len(oid) != b.oidSize {
				return "", "", fmt.Errorf("invalid source object ID")
			}
			include, err := b.include(oid)
			if err != nil {
				return "", "", err
			}
			if !include {
				continue
			}
			b.pending = append(b.pending, objectRequest{oid, hint})
			hintBytes += len(hint)
			if b.direct != nil && b.direct(oid) {
				continue
			}
			if _, err := fmt.Fprintf(b.input, "contents %s\n", oid); err != nil {
				return "", "", err
			}
		}
		if err := b.scan.Err(); err != nil {
			return "", "", err
		}
		if len(b.pending) == 0 {
			return "", "", io.EOF
		}
		if _, err := b.input.WriteString("flush\n"); err != nil {
			return "", "", err
		}
		if err := b.input.Flush(); err != nil {
			return "", "", err
		}
	}
	r := b.pending[b.next]
	b.next++
	return r.oid, r.hint, nil
}
