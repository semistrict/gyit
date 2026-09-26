package repo

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	"gyit/internal/pathspec"
)

type showChange struct {
	name string
	a, b Entry
}

func (r *Repository) ViewShow(ctx context.Context, current *Snapshot, opt ViewOptions, out io.Writer) error {
	f := viewFlags("show")
	var oneline, noPatch, nameOnly, nameStatus, firstParent, patch, noRenames bool
	var contextLines int
	f.BoolVar(&oneline, "oneline", false, "")
	f.BoolVar(&noPatch, "s", false, "")
	f.BoolVar(&noPatch, "no-patch", false, "")
	f.BoolVar(&patch, "p", false, "")
	f.BoolVar(&patch, "patch", false, "")
	f.BoolVar(&nameOnly, "name-only", false, "")
	f.BoolVar(&nameStatus, "name-status", false, "")
	f.BoolVar(&firstParent, "first-parent", false, "")
	f.BoolVar(&firstParent, "dd", false, "")
	f.BoolVar(&noRenames, "no-renames", false, "")
	f.IntVar(&contextLines, "U", 3, "")
	f.IntVar(&contextLines, "unified", 3, "")
	var before, paths []string
	for i, a := range opt.Args {
		if a == "--" {
			before = opt.Args[:i]
			paths = opt.Args[i+1:]
			break
		}
	}
	if before == nil {
		before = opt.Args
	}
	if err := parseViewFlags(f, before); err != nil {
		return err
	}
	if contextLines < 0 || contextLines > 100 {
		return invalidRevision("diff context must be 0..100")
	}
	if nameOnly && nameStatus {
		return invalidRevision("choose --name-only or --name-status")
	}
	selectors := f.Args()
	if len(selectors) == 0 {
		selectors = []string{"HEAD"}
	}
	matcher, err := pathspec.Compile(paths, opt.Prefix)
	if err != nil {
		return err
	}
	shown := false
	for _, selector := range selectors {
		s, oid, o, err := r.resolveViewObject(ctx, current, opt.Prefix, selector)
		if err != nil {
			return err
		}
		var entry LogEntry
		if o.Kind == "commit" {
			if err = s.Log(ctx, 1, false, func(e LogEntry) error { entry = e; return nil }); err != nil {
				return err
			}
			// A path filter selects commits as well as their displayed changes,
			// including --no-patch. Decide before emitting headers or separators.
			if len(matcher.Patterns) > 0 {
				parents := entry.Parents
				if firstParent && len(parents) > 1 {
					parents = parents[:1]
				}
				selected, _, err := s.simplifyLogMatcher(ctx, s.SHA, parents, matcher, current)
				if err != nil {
					return err
				}
				if !selected {
					continue
				}
			}
		}
		if shown && o.Kind != "blob" && !oneline {
			if _, err = io.WriteString(out, "\n"); err != nil {
				return err
			}
		}
		if o.Kind != "blob" {
			shown = true
		}
		switch o.Kind {
		case "blob":
			if err = viewCopyBlob(ctx, s, oid, o.Size, out); err != nil {
				return err
			}
		case "tree":
			if _, err = fmt.Fprintf(out, "tree %s\n\n", selector); err != nil {
				return err
			}
			entries, eerr := viewTreeEntries(ctx, s, oid)
			if eerr != nil {
				return eerr
			}
			for _, e := range entries {
				name := e.Name
				if e.Mode == 0040000 {
					name += "/"
				}
				if _, err = fmt.Fprintln(out, quoteDiffPath(name)); err != nil {
					return err
				}
			}
		case "commit":
			if err = WriteLogEntry(out, entry, oneline); err != nil {
				return err
			}
			if noPatch {
				continue
			}
			if len(entry.Parents) > 1 && !firstParent {
				if err = s.showCombined(ctx, current, entry.Parents, matcher, DiffOptions{NameOnly: nameOnly, NameStatus: nameStatus, Context: contextLines}, out); err != nil {
					return err
				}
				if !oneline {
					if _, err = io.WriteString(out, "\n"); err != nil {
						return err
					}
				}
				continue
			}
			old := &Snapshot{idx: s.idx}
			if len(entry.Parents) > 0 {
				old.Tree, err = s.commitTree(ctx, entry.Parents[0])
				if err != nil {
					return err
				}
			}
			var changes []showChange
			retained := 0
			options := DiffOptions{NameOnly: nameOnly, NameStatus: nameStatus, Context: contextLines}
			err = s.walkChanges(ctx, old.Tree, s.Tree, matcher, func(name string, a, b Entry) error {
				ok, e := matcher.Match(name, func(_ string, reqs []pathspec.Requirement) (bool, error) {
					return current.matchAttributes(ctx, name, reqs)
				})
				if e != nil {
					return e
				}
				if !ok {
					return nil
				}
				retained += len(name) + 256
				if retained > 32<<20 || len(changes) >= 100000 {
					return fmt.Errorf("show exceeds bounded changed-path buffer; narrow path filters")
				}
				changes = append(changes, showChange{name, a, b})
				return nil
			})
			if err != nil {
				return err
			}
			sort.SliceStable(changes, func(i, j int) bool { return changes[i].name < changes[j].name })
			if len(changes) > 0 && !oneline {
				if _, err = io.WriteString(out, "\n"); err != nil {
					return err
				}
			}
			if err = showDiffChanges(ctx, old, s, changes, options, noRenames, out); err != nil {
				return err
			}

		default:
			return fmt.Errorf("cannot show %s objects", strings.TrimSpace(o.Kind))
		}
	}
	return nil
}
