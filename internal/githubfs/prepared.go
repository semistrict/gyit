package githubfs

import (
	"context"
	"fmt"
	"strings"

	"gyit/internal/repo"
	"gyit/internal/store"
)

// NewPrepared serves a durable progressive snapshot without a local
// Git repository, acquisition worker, or writable copy of the object store.
// The caller owns backend, for the lifetime of FS.
func NewPrepared(ctx context.Context, o Options, backend store.Store, target Target, sha string) (*FS, error) {
	if backend == nil || !validName(target.Owner) || !validName(target.Repository) || !fullSHA(sha) {
		return nil, fmt.Errorf("prepared progressive repository requires a target and full commit ID")
	}
	f, err := New(o)
	if err != nil {
		return nil, err
	}
	f.preparedOnly = true
	fail := func(err error) (*FS, error) { f.Close(); return nil, err }
	p, err := repo.NewProgressive(ctx, backend, f.cache, o.DataDir)
	if err != nil {
		return fail(err)
	}
	s, err := p.Open(ctx, sha)
	if err != nil {
		return fail(err)
	}
	done := make(chan struct{})
	close(done)
	target.Owner = strings.ToLower(target.Owner)
	target.Repository = strings.ToLower(target.Repository)
	f.jobs[target.Key()] = &job{target: target, display: target.Owner + "/" + target.Repository, progressive: p, snapshot: s, done: done, generation: 2}
	f.owners[target.Owner] = true
	return f, nil
}
