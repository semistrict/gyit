package repo

import (
	"context"
	"errors"
	"io"
	"iter"

	"gyit/internal/orderedrows"
	"gyit/internal/spill"
)

// updateSortedOrdered merges already-ordered archive identities with the smaller
// fallback sort. It uses the existing page builder and published representation.
func (idx *index) updateSortedOrdered(ctx context.Context, records *spill.Sorter, archive orderedrows.Cursor) (pageRef, error) {
	type record struct{ key, value []byte }
	stopped := errors.New("ordered index stopped consuming fallback records")
	sequence := func(yield func(record, error) bool) {
		err := records.Walk(ctx, func(key, value []byte) error {
			if !yield(record{key, value}, nil) {
				return stopped
			}
			return nil
		})
		if err != nil && !errors.Is(err, stopped) {
			yield(record{}, err)
		}
	}
	next, stop := iter.Pull2(sequence)
	defer stop()
	merged := orderedrows.Merge(archive, func(ctx context.Context) ([]byte, []byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		r, err, ok := next()
		if !ok {
			return nil, nil, io.EOF
		}
		return r.key, r.value, err
	})
	c := &changes{read: func() ([]byte, []byte, error) {
		key, value, err := merged(ctx)
		if errors.Is(err, io.EOF) {
			return nil, nil, nil
		}
		return key, value, err
	}}
	if err := c.next(); err != nil {
		return pageRef{}, err
	}
	return idx.updateChanges(ctx, c)
}
