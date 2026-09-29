package repo

import (
	"context"
	"errors"
	"fmt"
	"path"

	"gyit/internal/pathspec"
)

var stopTreeWalk = errors.New("stop tree walk")

// walkChanges merges paged directory iterators. Unchanged subtrees and excluded
// prefixes are pruned; file data is never read. No flat checkout is retained.
func (s *Snapshot) walkChanges(ctx context.Context, before, after string, filter *pathspec.Matcher, visit func(string, Entry, Entry) error) error {
	var walk func(string, Entry, Entry, int) error
	walk = func(name string, a, b Entry, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if a.OID == b.OID && a.Mode == b.Mode {
			return nil
		}
		if depth > 256 {
			return fmt.Errorf("history directory depth exceeds 256")
		}
		ad, bd := a.Mode == 0040000, b.Mode == 0040000
		if !ad && !bd {
			return visit(name, a, b)
		}
		if a.OID != "" && !ad {
			if err := visit(name, a, Entry{}); err != nil {
				return err
			}
		}
		if b.OID != "" && !bd {
			if err := visit(name, Entry{}, b); err != nil {
				return err
			}
		}
		if filter != nil && !filter.MayDescend(name) {
			return nil
		}
		ai, bi := dirIterator{s: s}, dirIterator{s: s}
		if ad {
			ai.tree = a.OID
		}
		if bd {
			bi.tree = b.OID
		}
		a, err := ai.next(ctx)
		if err != nil {
			return err
		}
		b, err = bi.next(ctx)
		if err != nil {
			return err
		}
		for a.OID != "" || b.OID != "" {
			var x, y Entry
			switch {
			case b.OID == "" || (a.OID != "" && a.Name < b.Name):
				x = a
				a, err = ai.next(ctx)
			case a.OID == "" || b.Name < a.Name:
				y = b
				b, err = bi.next(ctx)
			default:
				x, y = a, b
				a, err = ai.next(ctx)
				if err == nil {
					b, err = bi.next(ctx)
				}
			}
			if err != nil {
				return err
			}
			child := x.Name
			if child == "" {
				child = y.Name
			}
			if err := walk(path.Join(name, child), x, y, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	a, b := Entry{}, Entry{}
	if before != "" {
		a = Entry{OID: before, Mode: 0040000}
	}
	if after != "" {
		b = Entry{OID: after, Mode: 0040000}
	}
	return walk("", a, b, 0)
}

func (s *Snapshot) commitTree(ctx context.Context, sha string) (string, error) {
	if sha == "" {
		return "", nil
	}
	var o object
	err := s.idx.get(ctx, "o/"+sha, &o)
	return o.Tree, err
}
func (s *Snapshot) changedByPathspec(ctx context.Context, before, after string, m *pathspec.Matcher, attrs *Snapshot) (bool, error) {
	err := s.walkChanges(ctx, before, after, m, func(name string, a, b Entry) error {
		yes, err := m.Match(name, func(name string, req []pathspec.Requirement) (bool, error) {
			return attrs.matchAttributes(ctx, name, req)
		})
		if err != nil {
			return err
		}
		if yes {
			return stopTreeWalk
		}
		return nil
	})
	if errors.Is(err, stopTreeWalk) {
		return true, nil
	}
	return false, err
}
func (s *Snapshot) MatchesPathspec(ctx context.Context, raw, prefix string) (bool, error) {
	m, err := pathspec.Compile([]string{raw}, prefix)
	if err != nil {
		return false, invalidRevision(err.Error())
	}
	if paths := m.LiteralPaths(); len(paths) == 1 {
		name := paths[0]
		if name == "." {
			name = ""
		}
		e, err := s.logPathEntry(ctx, name)
		if IsNotFound(err) {
			return false, nil
		}
		return e.OID != "", err
	}
	return s.changedByPathspec(ctx, "", s.Tree, m, s)
}
func (s *Snapshot) simplifyLogMatcher(ctx context.Context, sha string, parents []string, m *pathspec.Matcher, attrs *Snapshot) (bool, []string, error) {
	tree, err := s.commitTree(ctx, sha)
	if err != nil {
		return false, nil, err
	}
	if len(parents) == 0 {
		changed, err := s.changedByPathspec(ctx, "", tree, m, attrs)
		return changed, nil, err
	}
	for _, parent := range parents {
		previous, err := s.commitTree(ctx, parent)
		if err != nil {
			return false, nil, err
		}
		changed, err := s.changedByPathspec(ctx, previous, tree, m, attrs)
		if err != nil {
			return false, nil, err
		}
		if !changed {
			return false, []string{parent}, nil
		}
	}
	return true, parents, nil
}
