//go:build !js

package repo

import "context"

// Each view owns at most the existing 32 MiB acquisition decoder workspace.
// Bound simultaneous views across repositories, not separately per mount.
var historyReadViews = make(chan struct{}, 2)

// UsePublishedHistorySources makes existing local acquisition files an optional
// read accelerator. It never fetches data or publishes records. Call during
// repository setup, before concurrent readers start. Empty directories work
// normally: object-store reads remain authoritative and sufficient.
func (p *Progressive) UsePublishedHistorySources(dirs ...string) {
	dirs = append([]string(nil), dirs...)
	p.historyReadView = func(ctx context.Context) (context.Context, func(), error) {
		select {
		case historyReadViews <- struct{}{}:
		case <-ctx.Done():
			return ctx, func() {}, ctx.Err()
		}
		// Prefer the complete ancestry source. Opening every acquisition lane
		// also maps and validates unrelated packs created by ordinary file reads.
		// This is only an accelerator: objects absent here still resolve through
		// their authoritative recipes and bytes in Store. Before full ancestry
		// arrives, keep the shallow and gap windows together so advancing one
		// boundary does not evict the locally available newer portion of history.
		groups := [][]string{dirs}
		if len(dirs) > 1 {
			groups = [][]string{dirs[:1], dirs[1:]}
		}
		for _, group := range groups {
			source, err := p.publishedHistorySource(ctx, group)
			if err != nil {
				<-historyReadViews
				return ctx, func() {}, err
			}
			if len(source.packs) == 0 {
				source.close()
				continue
			}
			return context.WithValue(ctx, historySourceKey{}, source), func() {
				source.close()
				<-historyReadViews
			}, nil
		}
		<-historyReadViews
		return ctx, func() {}, nil
	}
}
