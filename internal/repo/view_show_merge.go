package repo

import (
	"context"
	"errors"
	"fmt"
	"io"

	"gyit/internal/pathspec"
	"gyit/internal/store"
)

var ErrCombinedPatchUnsupported = errors.New("combined merge patch rendering is not supported; use --first-parent")

// showCombined proves that a merge has no combined patch by checking whether
// each changed path matches at least one parent. This covers merges of separate
// files and merges choosing one parent's complete file. A result differing from
// every parent requires the full dense-combined hunk algorithm, so it fails
// explicitly before writing a patch; it never substitutes first-parent output.
func (s *Snapshot) showCombined(ctx context.Context, current *Snapshot, parents []string, matcher *pathspec.Matcher, opts DiffOptions, out io.Writer) error {
	if len(parents) < 2 {
		return fmt.Errorf("combined diff needs multiple parents")
	}
	if len(parents) > 64 {
		return fmt.Errorf("combined diff exceeds 64-parent limit")
	}
	snapshots := make([]*Snapshot, len(parents))
	for i, sha := range parents {
		tree, err := s.commitTree(ctx, sha)
		if err != nil {
			return err
		}
		snapshots[i] = &Snapshot{idx: s.idx, SHA: sha, Tree: tree}
	}
	return s.walkChanges(ctx, snapshots[0].Tree, s.Tree, matcher, func(name string, a, b Entry) error {
		selected, err := matcher.Match(name, func(n string, requirements []pathspec.Requirement) (bool, error) {
			return current.matchAttributes(ctx, n, requirements)
		})
		if err != nil || !selected {
			return err
		}
		for _, parent := range snapshots[1:] {
			e, err := parent.Resolve(ctx, name)
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return err
			}
			if errors.Is(err, store.ErrNotFound) {
				e = Entry{}
			}
			if e.OID == b.OID && e.Mode == b.Mode {
				return nil
			}
		}
		return ErrCombinedPatchUnsupported
	})
}
