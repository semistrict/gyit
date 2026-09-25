package repo

import "context"

// Merge headers must disambiguate parent IDs just like the commit's own ID.
func (s *Snapshot) abbreviateLogParents(ctx context.Context, entry *LogEntry) error {
	if len(entry.Parents) < 2 {
		return nil
	}
	refs := &index{store: s.idx.store, cache: s.idx.cache}
	entry.ShortParents = make([]string, len(entry.Parents))
	for i, parent := range entry.Parents {
		short, err := abbreviate(ctx, s.idx, refs, parent)
		if err != nil {
			return err
		}
		entry.ShortParents[i] = short
	}
	return nil
}
