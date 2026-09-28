package repo

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
	"path/filepath"
	"strings"
	"sync"
)

// Progressive is a repository-wide immutable object pool. Acquisition belongs
// to the single writer; mounted readers only access Store and decoded cache.
// Missing requests are delegated without holding any reader/index mutex.
type Progressive struct {
	store           store.Store
	cache           *cache
	temp            string
	writer          sync.Mutex
	mu              sync.RWMutex
	root            pageRef
	token           string
	Demand          func(context.Context, []string) error
	DemandCommits   func(context.Context, []string, int) error
	ResolveRevision func(context.Context, string) (string, error)
	slots           chan struct{}
	historyBuild    chan struct{}
}

func newProgressive(ctx context.Context, backend store.Store, disk decodedCache, temp string) (*Progressive, error) {
	p := &Progressive{store: backend, cache: newCache(0), temp: temp, slots: make(chan struct{}, 2), historyBuild: make(chan struct{}, 1)}
	p.cache.disk = disk
	b, token, err := backend.Get(ctx, "HEAD", 0, -1)
	if errors.Is(err, store.ErrNotFound) {
		p.token = "*"
		return p, nil
	}
	if err != nil {
		return nil, err
	}
	var m pb.ProgressiveManifest
	if err = proto.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if m.Version != 1 || m.Index == nil {
		return nil, fmt.Errorf("invalid progressive manifest")
	}
	p.root, p.token = decodePageRef(m.Index), token
	return p, nil
}
func (p *Progressive) index() *index {
	p.mu.RLock()
	root := p.root
	p.mu.RUnlock()
	return &index{store: p.store, cache: p.cache, root: root, containers: true}
}
func (p *Progressive) get(ctx context.Context, key string, m proto.Message) error {
	if source, ok := ctx.Value(historySourceKey{}).(*historySource); ok && strings.HasPrefix(key, "g/") {
		o, err := source.lookup(strings.TrimPrefix(key, "g/"))
		if err != nil {
			return err
		}
		if o != nil {
			proto.Reset(m)
			proto.Merge(m, o)
			return nil
		}
	}
	return p.index().get(ctx, key, m)
}
func (p *Progressive) ObjectSize(ctx context.Context, oid string) (int64, error) {
	if source, ok := ctx.Value(historySourceKey{}).(*historySource); ok {
		if raw, ok := source.cache.get("progressive-object/" + oid); ok {
			return int64(len(raw) - 1), nil
		}
	}
	var o pb.ProgressiveObject
	if err := p.get(ctx, "g/"+oid, &o); err != nil {
		return 0, err
	}
	return o.Size, nil
}

func (p *Progressive) HasPreparedSnapshot(ctx context.Context) (bool, error) {
	items, err := p.index().scan(ctx, "tree/", "", 1)
	return len(items) != 0, err
}
func (p *Progressive) Ensure(ctx context.Context, oids []string) error {
	missing := make([]string, 0)
	for _, oid := range oids {
		_, err := p.ObjectSize(ctx, oid)
		if errors.Is(err, store.ErrNotFound) {
			missing = append(missing, oid)
		} else if err != nil {
			return err
		}
	}
	if len(missing) == 0 {
		return nil
	}
	if p.Demand == nil {
		return fmt.Errorf("objects not yet available: %w", store.ErrNotFound)
	}
	return p.Demand(ctx, missing)
}
func (p *Progressive) Open(ctx context.Context, sha string) (*Snapshot, error) {
	// Decoded content may be admitted while staging an immutable pack, but a
	// revision becomes available only after its object index is published.
	if _, err := p.ObjectSize(ctx, sha); err != nil {
		return nil, err
	}
	raw, kind, err := p.object(ctx, sha)
	if err != nil {
		return nil, err
	}
	if kind != 1 {
		return nil, fmt.Errorf("revision is not a commit")
	}
	tree, _, err := parseCommit(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	return &Snapshot{SHA: sha, Tree: tree, progressive: p, idx: p.historyIndex()}, nil
}
func (p *Progressive) State(ctx context.Context, sha string) (*pb.ProgressiveState, error) {
	s := &pb.ProgressiveState{}
	e := p.get(ctx, "state/"+sha, s)
	return s, e
}
func validProgressiveOID(oid string) bool {
	b, e := hex.DecodeString(oid)
	return e == nil && len(b) == 20 && hex.EncodeToString(b) == oid
}
func progressivePackKey(pack string, segment int64) string {
	return filepath.ToSlash(fmt.Sprintf("packs/progressive/%s/%08x", pack, segment))
}
