// SPDX-License-Identifier: LGPL-2.1-or-later
// Adapted from Git's LibXDiff, xdiff/xdiffi.c.
// Copyright (C) 2003 Davide Libenzi <davidel@xmailserver.org>.
// See third_party/xdiff/NOTICE and third_party/xdiff/LGPL-2.1.

package repo

import (
	"context"
	"fmt"
)

// Patches use the meeting point of forward and backward Myers searches. A
// forward-only trace can select another equally short match sequence, changing
// Git-compatible hunks when repeated lines have several possible counterparts.
// Blame retains its separate minimal matcher. Both searches remain bounded.
func patchChangedLines(ctx context.Context, a, b []string) ([]bool, []bool, error) {
	left, right := make([]bool, len(a)), make([]bool, len(b))
	type box struct{ a0, a1, b0, b1 int }
	stack := []box{{0, len(a), 0, len(b)}}
	offset := len(b) + 1
	forward := make([]int, len(a)+len(b)+3)
	backward := make([]int, len(forward))
	work := 0
	for len(stack) != 0 {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		q := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for q.a0 < q.a1 && q.b0 < q.b1 && a[q.a0] == b[q.b0] {
			q.a0++
			q.b0++
		}
		for q.a0 < q.a1 && q.b0 < q.b1 && a[q.a1-1] == b[q.b1-1] {
			q.a1--
			q.b1--
		}
		if q.a0 == q.a1 {
			for i := q.b0; i < q.b1; i++ {
				right[i] = true
			}
			continue
		}
		if q.b0 == q.b1 {
			for i := q.a0; i < q.a1; i++ {
				left[i] = true
			}
			continue
		}
		x, y, err := patchMiddle(ctx, a, b, q.a0, q.a1, q.b0, q.b1, forward, backward, offset, &work)
		if err != nil {
			return nil, nil, err
		}
		stack = append(stack, box{x, q.a1, y, q.b1}, box{q.a0, x, q.b0, y})
	}
	return left, right, nil
}

func patchMiddle(ctx context.Context, a, b []string, a0, a1, b0, b1 int, forward, backward []int, offset int, work *int) (int, int, error) {
	low, high := a0-b1, a1-b0
	fmid, bmid := a0-b0, a1-b1
	fmin, fmax, bmin, bmax := fmid, fmid, bmid, bmid
	odd := (fmid-bmid)&1 != 0
	forward[offset+fmid], backward[offset+bmid] = a0, a1
	for {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		if fmin > low {
			fmin--
			forward[offset+fmin-1] = -1
		} else {
			fmin++
		}
		if fmax < high {
			fmax++
			forward[offset+fmax+1] = -1
		} else {
			fmax--
		}
		for diagonal := fmax; diagonal >= fmin; diagonal -= 2 {
			*work++
			if *work > maxDiffCells {
				return 0, 0, fmt.Errorf("text comparison exceeds bounded edit-work limit")
			}
			x := forward[offset+diagonal+1]
			if forward[offset+diagonal-1] >= x {
				x = forward[offset+diagonal-1] + 1
			}
			y := x - diagonal
			for x < a1 && y < b1 && a[x] == b[y] {
				x++
				y++
			}
			forward[offset+diagonal] = x
			if odd && bmin <= diagonal && diagonal <= bmax && backward[offset+diagonal] <= x {
				return x, y, nil
			}
		}
		if bmin > low {
			bmin--
			backward[offset+bmin-1] = len(a) + len(b) + 1
		} else {
			bmin++
		}
		if bmax < high {
			bmax++
			backward[offset+bmax+1] = len(a) + len(b) + 1
		} else {
			bmax--
		}
		for diagonal := bmax; diagonal >= bmin; diagonal -= 2 {
			*work++
			if *work > maxDiffCells {
				return 0, 0, fmt.Errorf("text comparison exceeds bounded edit-work limit")
			}
			x := backward[offset+diagonal+1] - 1
			if backward[offset+diagonal-1] < backward[offset+diagonal+1] {
				x = backward[offset+diagonal-1]
			}
			y := x - diagonal
			for x > a0 && y > b0 && a[x-1] == b[y-1] {
				x--
				y--
			}
			backward[offset+diagonal] = x
			if !odd && fmin <= diagonal && diagonal <= fmax && x <= forward[offset+diagonal] {
				return x, y, nil
			}
		}
	}
}
