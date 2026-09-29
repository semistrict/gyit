package repo

import (
	"bytes"
	"context"
	"fmt"
	"strings"
)

const maxHistoryFile = 8 << 20
const maxHistoryLines = 100000
const maxDiffCells = 2 << 20

type lineEdit struct {
	kind byte
	a, b int
}

func textLines(data []byte) ([]string, error) {
	if bytes.IndexByte(data, 0) >= 0 {
		return nil, fmt.Errorf("blame requires a text file")
	}
	if len(data) == 0 {
		return nil, nil
	}
	lines := strings.SplitAfter(string(data), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) > maxHistoryLines {
		return nil, fmt.Errorf("text exceeds the 100000-line limit")
	}
	return lines, nil
}

// Myers' shortest edit script, with bounded trace storage and cancellation.
// Exceeding the work limit is an error, never an approximate blame result.
func lineDiff(ctx context.Context, a, b []string) ([]lineEdit, error) {
	edits, err := rawLineDiff(ctx, a, b)
	if err != nil {
		return nil, err
	}
	return alignChanges(a, b, edits), nil
}

func rawLineDiff(ctx context.Context, a, b []string) ([]lineEdit, error) {
	prefix := 0
	for prefix < len(a) && prefix < len(b) && a[prefix] == b[prefix] {
		prefix++
	}
	suffix := 0
	for suffix < len(a)-prefix && suffix < len(b)-prefix && a[len(a)-1-suffix] == b[len(b)-1-suffix] {
		suffix++
	}
	aa, bb := a[prefix:len(a)-suffix], b[prefix:len(b)-suffix]
	var middle []lineEdit
	if len(aa) == 0 || len(bb) == 0 {
		for i := range aa {
			middle = append(middle, lineEdit{'-', i, 0})
		}
		for j := range bb {
			middle = append(middle, lineEdit{'+', len(aa), j})
		}
	} else {
		n, m := len(aa), len(bb)
		v := make([]int, 2*(n+m)+3)
		off := n + m + 1
		var trace [][]int
		cells := 0
		done := false
		for d := 0; d <= n+m && !done; d++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			cells += 2*d + 1
			if cells > maxDiffCells {
				return nil, fmt.Errorf("text comparison exceeds bounded edit-work limit")
			}
			for k := -d; k <= d; k += 2 {
				x := 0
				if k == -d || (k != d && v[off+k-1] < v[off+k+1]) {
					x = v[off+k+1]
				} else {
					x = v[off+k-1] + 1
				}
				y := x - k
				for x < n && y < m && aa[x] == bb[y] {
					x++
					y++
				}
				v[off+k] = x
				if x >= n && y >= m {
					done = true
					break
				}
			}
			trace = append(trace, append([]int(nil), v[off-d:off+d+1]...))
		}
		x, y := n, m
		for d := len(trace) - 1; d > 0; d-- {
			prev := trace[d-1]
			at := func(k int) int { return prev[k+d-1] }
			k := x - y
			pk := k - 1
			if k == -d || (k != d && at(k-1) < at(k+1)) {
				pk = k + 1
			}
			px := at(pk)
			py := px - pk
			for x > px && y > py {
				x--
				y--
				middle = append(middle, lineEdit{' ', x, y})
			}
			if x == px {
				y--
				middle = append(middle, lineEdit{'+', x, y})
			} else {
				x--
				middle = append(middle, lineEdit{'-', x, y})
			}
		}
		for x > 0 && y > 0 {
			x--
			y--
			middle = append(middle, lineEdit{' ', x, y})
		}
		for i, j := 0, len(middle)-1; i < j; i, j = i+1, j-1 {
			middle[i], middle[j] = middle[j], middle[i]
		}
	}
	out := make([]lineEdit, 0, prefix+len(middle)+suffix)
	for i := 0; i < prefix; i++ {
		out = append(out, lineEdit{' ', i, i})
	}
	for _, e := range middle {
		e.a += prefix
		e.b += prefix
		out = append(out, e)
	}
	for i := 0; i < suffix; i++ {
		out = append(out, lineEdit{' ', len(a) - suffix + i, len(b) - suffix + i})
	}
	return out, nil
}

// Adjacent equal lines allow an edit range to move without changing the patch.
// Move ranges together on both sides, preferring a position aligned with a
// replacement on the opposite side over splitting it into separate edits.
type editRange struct{ lo, hi int }

func changedAt(bits []bool, i int) bool { return i >= 0 && i < len(bits) && bits[i] }
func firstRange(bits []bool) editRange {
	r := editRange{}
	for changedAt(bits, r.hi) {
		r.hi++
	}
	return r
}
func (r *editRange) next(bits []bool) bool {
	if r.hi == len(bits) {
		return false
	}
	r.lo = r.hi + 1
	r.hi = r.lo
	for changedAt(bits, r.hi) {
		r.hi++
	}
	return true
}
func (r *editRange) previous(bits []bool) bool {
	if r.lo == 0 {
		return false
	}
	r.hi = r.lo - 1
	r.lo = r.hi
	for changedAt(bits, r.lo-1) {
		r.lo--
	}
	return true
}
func (r *editRange) up(lines []string, bits []bool) bool {
	if r.lo == 0 || lines[r.lo-1] != lines[r.hi-1] {
		return false
	}
	r.lo--
	bits[r.lo] = true
	r.hi--
	bits[r.hi] = false
	for changedAt(bits, r.lo-1) {
		r.lo--
	}
	return true
}
func (r *editRange) down(lines []string, bits []bool) bool {
	if r.hi == len(lines) || lines[r.lo] != lines[r.hi] {
		return false
	}
	bits[r.lo] = false
	r.lo++
	bits[r.hi] = true
	r.hi++
	for changedAt(bits, r.hi) {
		r.hi++
	}
	return true
}

func compactChangesUsing(lines []string, bits, opposite []bool, chooseEnd func([]string, int, int, int) int) {
	r, other := firstRange(bits), firstRange(opposite)
	for {
		if r.lo != r.hi {
			earliest, matching := 0, false
			for {
				size := r.hi - r.lo
				matching = false
				for r.up(lines, bits) {
					other.previous(opposite)
				}
				earliest = r.hi
				if other.hi > other.lo {
					matching = true
				}
				for r.down(lines, bits) {
					other.next(opposite)
					if other.hi > other.lo {
						matching = true
					}
				}
				if size == r.hi-r.lo {
					break
				}
			}
			if r.hi != earliest && matching {
				for other.hi == other.lo {
					if !r.up(lines, bits) {
						break
					}
					other.previous(opposite)
				}
			} else if r.hi != earliest && chooseEnd != nil {
				end := chooseEnd(lines, earliest, r.hi, r.hi-r.lo)
				for r.hi > end && r.up(lines, bits) {
					other.previous(opposite)
				}
			}
		}
		if !r.next(bits) {
			return
		}
		other.next(opposite)
	}
}
func alignChanges(a, b []string, edits []lineEdit) []lineEdit {
	left, right := make([]bool, len(a)), make([]bool, len(b))
	for _, e := range edits {
		if e.kind == '-' {
			left[e.a] = true
		}
		if e.kind == '+' {
			right[e.b] = true
		}
	}
	return alignChangedLines(a, b, left, right, len(edits))
}

func alignChangedLines(a, b []string, left, right []bool, capacity int) []lineEdit {
	return alignChangedLinesUsing(a, b, left, right, capacity, nil)
}

func alignChangedLinesUsing(a, b []string, left, right []bool, capacity int, chooseEnd func([]string, int, int, int) int) []lineEdit {
	compactChangesUsing(a, left, right, chooseEnd)
	compactChangesUsing(b, right, left, chooseEnd)
	out := make([]lineEdit, 0, capacity)
	i, j := 0, 0
	for i < len(a) || j < len(b) {
		if changedAt(left, i) {
			out = append(out, lineEdit{'-', i, j})
			i++
		} else if changedAt(right, j) {
			out = append(out, lineEdit{'+', i, j})
			j++
		} else {
			out = append(out, lineEdit{' ', i, j})
			i++
			j++
		}
	}
	return out
}
