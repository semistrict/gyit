package repo

import "context"

// A view is private to one synchronous traversal. No background loader may
// retain its mapped pack bytes after release. Reopen only at an object gap and
// after publication advances; long streaming queries can discover later packs.
type historyReadView struct {
	p         *Progressive
	base, ctx context.Context
	release   func()
	token     string
}

func (v *historyReadView) refresh() error {
	if v.p.historyReadView == nil {
		return nil
	}
	v.p.mu.RLock()
	token := v.p.token
	v.p.mu.RUnlock()
	if v.release != nil && token == v.token {
		return nil
	}
	v.close()
	ctx, release, err := v.p.historyReadView(v.base)
	if err != nil {
		release()
		return err
	}
	v.ctx, v.release, v.token = ctx, release, token
	return nil
}

func (v *historyReadView) close() {
	if v.release != nil {
		v.release()
		v.release = nil
	}
	v.ctx = v.base
}

func (v *historyReadView) local() bool {
	source, ok := v.ctx.Value(historySourceKey{}).(*historySource)
	return ok && len(source.packs) != 0
}
