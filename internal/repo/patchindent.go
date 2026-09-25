// SPDX-License-Identifier: LGPL-2.1-or-later
// Adapted from Git's LibXDiff, xdiff/xdiffi.c.
// Copyright (C) 2003 Davide Libenzi <davidel@xmailserver.org>.
// See third_party/xdiff/NOTICE and third_party/xdiff/LGPL-2.1.

package repo

// Pure insertions/deletions can slide across equal boundary lines. Git scores
// both boundaries using indentation and nearby blank lines, favoring complete
// blocks. All scans and sliding windows are bounded as in the native matcher.
func patchGroupEnd(lines []string, earliest, latest, size int) int {
	first := max(earliest, latest-size-1, latest-100)
	bestEnd := first
	var best patchSplitScore
	for end := first; end <= latest; end++ {
		a, b := patchSplit(lines, end), patchSplit(lines, end-size)
		score := patchSplitScore{a.indent + b.indent, a.penalty + b.penalty}
		cmp := 0
		if score.indent > best.indent {
			cmp = 1
		} else if score.indent < best.indent {
			cmp = -1
		}
		if end == first || 60*cmp+score.penalty-best.penalty <= 0 {
			bestEnd, best = end, score
		}
	}
	return bestEnd
}

type patchSplitScore struct{ indent, penalty int }

func patchSplit(lines []string, at int) patchSplitScore {
	indent, preIndent, postIndent := -1, -1, -1
	preBlank, postBlank := 0, 0
	if at < len(lines) {
		indent = patchIndent(lines[at])
	}
	for i := at - 1; i >= 0; i-- {
		preIndent = patchIndent(lines[i])
		if preIndent != -1 {
			break
		}
		preBlank++
		if preBlank == 20 {
			preIndent = 0
			break
		}
	}
	for i := at + 1; i < len(lines); i++ {
		postIndent = patchIndent(lines[i])
		if postIndent != -1 {
			break
		}
		postBlank++
		if postBlank == 20 {
			postIndent = 0
			break
		}
	}
	score := patchSplitScore{}
	if preIndent == -1 && preBlank == 0 {
		score.penalty++
	}
	if at == len(lines) {
		score.penalty += 21
	}
	if indent == -1 {
		postBlank++
	} else {
		postBlank = 0
	}
	blank := preBlank + postBlank
	score.penalty -= 30 * blank
	score.penalty += 6 * postBlank
	if indent == -1 {
		indent = postIndent
	}
	score.indent = indent
	switch {
	case indent == -1 || preIndent == -1 || indent == preIndent:
	case indent > preIndent:
		if blank > 0 {
			score.penalty += 10
		} else {
			score.penalty -= 4
		}
	case postIndent != -1 && postIndent > indent:
		if blank > 0 {
			score.penalty += 17
		} else {
			score.penalty += 24
		}
	default:
		if blank > 0 {
			score.penalty += 17
		} else {
			score.penalty += 23
		}
	}
	return score
}

func patchIndent(line string) int {
	column := 0
	for i := range len(line) {
		switch line[i] {
		case ' ':
			column++
		case '\t':
			column += 8 - column%8
		case '\r', '\n', '\v', '\f':
		default:
			return column
		}
		if column >= 200 {
			return 200
		}
	}
	return -1
}
