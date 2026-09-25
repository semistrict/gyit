package repo

import "context"

// Git's default patch matcher can discard a common line surrounded by many
// replaced lines. Keeping that weak match would split a replacement merely to
// preserve a blank or brace. Blame continues to use the minimal line matcher.
func patchLineDiff(ctx context.Context, a, b []string) ([]lineEdit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := 0
	for prefix < len(a) && prefix < len(b) && a[prefix] == b[prefix] {
		prefix++
	}
	suffix := 0
	for suffix < len(a)-prefix && suffix < len(b)-prefix && a[len(a)-1-suffix] == b[len(b)-1-suffix] {
		suffix++
	}
	counts := func(lines []string) map[string]int {
		out := make(map[string]int, len(lines))
		for _, line := range lines {
			out[line]++
		}
		return out
	}
	aCounts, bCounts := counts(a), counts(b)
	left := weakPatchMatches(a, bCounts, prefix, len(a)-suffix)
	right := weakPatchMatches(b, aCounts, prefix, len(b)-suffix)
	if left == nil {
		left = make([]bool, len(a))
	}
	if right == nil {
		right = make([]bool, len(b))
	}
	// Git's preparation also removes lines with no possible counterpart.
	// Their edits are already known; searching them after discarding weak
	// matches can needlessly exhaust the bounded edit-work budget.
	for i, line := range a {
		if bCounts[line] == 0 {
			left[i] = true
		}
	}
	for i, line := range b {
		if aCounts[line] == 0 {
			right[i] = true
		}
	}
	filter := func(lines []string, changed []bool) ([]string, []int) {
		kept := make([]string, 0, len(lines))
		positions := make([]int, 0, len(lines))
		for i, line := range lines {
			if !changed[i] {
				kept = append(kept, line)
				positions = append(positions, i)
			}
		}
		return kept, positions
	}
	aa, ai := filter(a, left)
	bb, bi := filter(b, right)
	deleted, inserted, err := patchChangedLines(ctx, aa, bb)
	if err != nil {
		return nil, err
	}
	for i, changed := range deleted {
		if changed {
			left[ai[i]] = true
		}
	}
	for i, changed := range inserted {
		if changed {
			right[bi[i]] = true
		}
	}
	return alignChangedLinesUsing(a, b, left, right, len(a)+len(b), patchGroupEnd), nil
}

// The frequency threshold is Git's power-of-two square-root approximation,
// capped at 1024. Frequencies include common prefixes/suffixes, but only the
// changed middle is eligible for suppression or its bounded neighbor scan.
func weakPatchMatches(lines []string, opposite map[string]int, start, end int) []bool {
	threshold := 1
	for n := len(lines); n > 0; n >>= 2 {
		threshold <<= 1
	}
	threshold = min(threshold, 1024)
	const (
		unmatched = iota
		ordinary
		frequent
	)
	kinds := make([]byte, end-start)
	for i, line := range lines[start:end] {
		switch count := opposite[line]; {
		case count == 0:
			kinds[i] = unmatched
		case count < threshold:
			kinds[i] = ordinary
		default:
			kinds[i] = frequent
		}
	}
	var changed []bool
	for i, kind := range kinds {
		if kind != frequent {
			continue
		}
		unmatchedCount, frequentCount := 0, 0
		bothSides := true
		for _, direction := range []int{-1, 1} {
			sideUnmatched := 0
			frequentCount++
			for distance := 1; distance <= 100; distance++ {
				j := i + direction*distance
				if j < 0 || j >= len(kinds) || kinds[j] == ordinary {
					break
				}
				if kinds[j] == unmatched {
					sideUnmatched++
				} else {
					frequentCount++
				}
			}
			bothSides = bothSides && sideUnmatched > 0
			unmatchedCount += sideUnmatched
		}
		if bothSides && unmatchedCount > 3*frequentCount {
			if changed == nil {
				changed = make([]bool, len(lines))
			}
			changed[start+i] = true
		}
	}
	return changed
}
