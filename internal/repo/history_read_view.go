package repo

import (
	"context"
	"time"
)

// A consumer blocked this long (a paused pager) gives up its read view, which
// is bounded process-wide. Reads continue through Store, and the view reopens
// at the next object gap.
const historyViewIdle = 250 * time.Millisecond

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

// yield runs fn, which may block on the consumer, and releases the view if fn
// is still running after historyViewIdle. The traversal does not use the view
// while fn runs, and loaded commits own their bytes, so nothing is left mapped.
func (v *historyReadView) yield(fn func() error) error {
	if v.release == nil {
		return fn()
	}
	released := make(chan struct{})
	timer := time.AfterFunc(historyViewIdle, func() {
		v.close()
		close(released)
	})
	err := fn()
	if !timer.Stop() {
		<-released
	}
	return err
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
