package repo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"
)

type showRename struct {
	source int
	score  uint64
}

// showDiffChanges pairs exact renames using Git's basename preference, then
// compares edited regular files with Git's content-span score. A competing
// inexact pairing is rejected instead of silently guessing at Git's heuristics.
// Two span histograms bound comparison memory; file reads share one 256 MiB cap.
func showDiffChanges(ctx context.Context, old, to *Snapshot, changes []showChange, opt DiffOptions, noRenames bool, out io.Writer) error {
	if noRenames {
		for _, c := range changes {
			if err := old.diffFile(ctx, to, c.name, c.a, c.b, opt, out); err != nil {
				return err
			}
		}
		return nil
	}
	deleted := map[string][]int{}
	var sources, dests []int
	for i, c := range changes {
		if c.a.OID != "" && c.b.OID == "" {
			sources = append(sources, i)
			deleted[c.a.OID] = append(deleted[c.a.OID], i)
		} else if c.a.OID == "" && c.b.OID != "" {
			dests = append(dests, i)
		}
	}
	pairs := map[int]showRename{}
	used := make([]bool, len(changes))
	inspected := 0
	for _, d := range dests {
		if err := ctx.Err(); err != nil {
			return err
		}
		target := changes[d]
		best, rank, examined := -1, -1, 0
		for _, s := range deleted[target.b.OID] {
			inspected++
			if inspected > 1000000 {
				return fmt.Errorf("exact rename comparisons exceed bounded query budget; use --no-renames")
			}
			source := changes[s]
			if used[s] || ((source.a.Mode&0170000 != 0100000 || target.b.Mode&0170000 != 0100000) && source.a.Mode != target.b.Mode) {
				continue
			}
			score := 0
			if path.Base(source.name) == path.Base(target.name) {
				score = 1
			}
			if score > rank {
				best, rank = s, score
			}
			examined++
			if rank == 1 || examined == 100 {
				break
			}
		}
		if best >= 0 {
			pairs[d] = showRename{best, 60000}
			used[best] = true
		}
	}
	remainingSources := []int{}
	remainingDests := []int{}
	for _, s := range sources {
		if !used[s] {
			remainingSources = append(remainingSources, s)
		}
	}
	for _, d := range dests {
		if _, ok := pairs[d]; !ok {
			remainingDests = append(remainingDests, d)
		}
	}
	if len(remainingSources) > 0 && len(remainingDests) > 0 {
		if len(remainingSources) > 4096/len(remainingDests) {
			return fmt.Errorf("edited rename comparisons exceed bounded query budget; use --no-renames")
		}
		budget := &renameSearch{remaining: renameReadBudget}
		targetCounts := make([]uint64, renameBuckets)
		sourceCounts := make([]uint64, renameBuckets)
		candidates := map[int]showRename{}
		sourceMatch := map[int]int{}
		for _, d := range remainingDests {
			target := changes[d].b
			if target.Mode&0170000 != 0100000 || target.Size == 0 {
				continue
			}
			loaded := false
			for _, s := range remainingSources {
				source := changes[s].a
				if source.Mode&0170000 != 0100000 || source.Size == 0 || min(source.Size, target.Size)*2 < max(source.Size, target.Size) {
					continue
				}
				if !loaded {
					clear(targetCounts)
					if err := to.renameSpans(ctx, target, targetCounts, budget); err != nil {
						return fmt.Errorf("%w; use --no-renames", err)
					}
					loaded = true
				}
				clear(sourceCounts)
				if err := old.renameSpans(ctx, source, sourceCounts, budget); err != nil {
					return fmt.Errorf("%w; use --no-renames", err)
				}
				var copied uint64
				for i, n := range sourceCounts {
					copied += min(n, targetCounts[i])
				}
				score := copied * 60000 / uint64(max(source.Size, target.Size))
				if score < 30000 {
					continue
				}
				if _, exists := candidates[d]; exists {
					return fmt.Errorf("ambiguous edited rename sources for %q; use --no-renames", changes[d].name)
				}
				if previous, exists := sourceMatch[s]; exists {
					return fmt.Errorf("ambiguous edited rename destinations %q and %q; use --no-renames", changes[previous].name, changes[d].name)
				}
				candidates[d] = showRename{s, score}
				sourceMatch[s] = d
			}
		}
		for d, pair := range candidates {
			pairs[d] = pair
			used[pair.source] = true
		}
	}
	for i, c := range changes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if used[i] {
			continue
		}
		if pair, ok := pairs[i]; ok {
			source := changes[pair.source]
			if err := showRenamePatch(ctx, old, to, source.name, c.name, source.a, c.b, pair.score, opt, out); err != nil {
				return err
			}
		} else if err := old.diffFile(ctx, to, c.name, c.a, c.b, opt, out); err != nil {
			return err
		}
	}
	return nil
}

type showPatchBuffer struct{ bytes.Buffer }

func (b *showPatchBuffer) WriteString(s string) (int, error) { return b.Write([]byte(s)) }

func (b *showPatchBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 32<<20 {
		return 0, fmt.Errorf("rename patch exceeds 32 MiB output budget; use --name-only")
	}
	return b.Buffer.Write(p)
}

func showRenamePatch(ctx context.Context, old, to *Snapshot, from, name string, a, b Entry, score uint64, opt DiffOptions, out io.Writer) error {
	if opt.NameOnly {
		_, err := fmt.Fprintln(out, quoteDiffPath(name))
		return err
	}
	if opt.NameStatus {
		_, err := fmt.Fprintf(out, "R%03d\t%s\t%s\n", score/600, quoteDiffPath(from), quoteDiffPath(name))
		return err
	}
	var patch showPatchBuffer
	// Existing per-file rendering supplies the bounded line diff. Only its
	// pathname headers change; hunk contents are copied byte-for-byte.
	if err := old.diffFile(ctx, to, name, a, b, DiffOptions{Context: opt.Context}, &patch); err != nil {
		return err
	}
	rest := patch.String()
	if rest != "" {
		_, rest, _ = strings.Cut(rest, "\n")
	}
	var header strings.Builder
	fmt.Fprintf(&header, "diff --git %s %s\n", quoteDiffPath("a/"+from), quoteDiffPath("b/"+name))
	if a.Mode != b.Mode {
		for i := 0; i < 2; i++ {
			line, remaining, ok := strings.Cut(rest, "\n")
			if !ok {
				return fmt.Errorf("invalid rename patch mode header")
			}
			header.WriteString(line)
			header.WriteByte('\n')
			rest = remaining
		}
	}
	fmt.Fprintf(&header, "similarity index %d%%\nrename from %s\nrename to %s\n", score/600, quoteDiffPath(from), quoteDiffPath(name))
	if _, err := io.WriteString(out, header.String()); err != nil {
		return err
	}
	for rest != "" {
		line, remaining, ok := strings.Cut(rest, "\n")
		if strings.HasPrefix(line, "@@ ") {
			_, err := io.WriteString(out, rest)
			return err
		}
		if line == "--- "+patchPath(quoteDiffPath("a/"+name)) {
			line = "--- " + patchPath(quoteDiffPath("a/"+from))
		}
		if strings.HasPrefix(line, "Binary files ") {
			line = strings.Replace(line, "Binary files "+quoteDiffPath("a/"+name)+" and ", "Binary files "+quoteDiffPath("a/"+from)+" and ", 1)
		}
		if ok {
			line += "\n"
		}
		if _, err := io.WriteString(out, line); err != nil {
			return err
		}
		rest = remaining
	}
	return nil
}
