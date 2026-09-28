package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
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

func NewProgressive(ctx context.Context, backend store.Store, disk *store.DiskCache, temp string) (*Progressive, error) {
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
func (p *Progressive) stage(fn func(*bolt.Bucket) error) error {
	f, err := os.CreateTemp(p.temp, "progressive-*.db")
	if err != nil {
		return err
	}
	name := f.Name()
	f.Close()
	defer os.Remove(name)
	db, err := bolt.Open(name, 0600, &bolt.Options{NoSync: true})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(func(tx *bolt.Tx) error {
		b, e := tx.CreateBucket([]byte("changes"))
		if e != nil {
			return e
		}
		return fn(b)
	})
}
func progressivePut(b *bolt.Bucket, key string, m proto.Message) error {
	raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if e != nil {
		return e
	}
	return b.Put([]byte(key), raw)
}

// publish is called with writer held. Failed CAS leaves the current view intact.
func (p *Progressive) publish(ctx context.Context, b *bolt.Bucket) error {
	return p.publishUploads(ctx, b, nil)
}
func (p *Progressive) publishUploads(ctx context.Context, b *bolt.Bucket, uploads *publicationUploads) error {
	backend := p.store
	if uploads != nil {
		backend = uploads
	}
	idx := p.index()
	idx.store = backend
	root, err := idx.update(ctx, b)
	if err != nil {
		return err
	}
	raw, err := proto.Marshal(&pb.ProgressiveManifest{Version: 1, Index: encodePageRef(root)})
	if err != nil {
		return err
	}
	key := fmt.Sprintf("generations/%x", sha256.Sum256(raw))
	if err = backend.Put(ctx, key, raw, ""); err != nil {
		return err
	}
	if uploads != nil {
		if err = uploads.wait(); err != nil {
			return err
		}
	}
	var token string
	if writer, ok := p.store.(store.VersionedWriter); ok {
		token, err = writer.PutVersion(ctx, "HEAD", raw, p.token)
		if err != nil {
			return err
		}
		if token == "" || token == "*" {
			return fmt.Errorf("invalid publication version")
		}
	} else {
		if err = p.store.Put(ctx, "HEAD", raw, p.token); err != nil {
			return err
		}
		var head []byte
		head, token, err = p.store.Get(ctx, "HEAD", 0, -1)
		if err != nil {
			return err
		}
		if !bytes.Equal(raw, head) {
			return store.ErrConflict
		}
	}
	p.mu.Lock()
	p.root, p.token = root, token
	p.mu.Unlock()
	return nil
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
func (p *Progressive) SetState(ctx context.Context, sha string, state *pb.ProgressiveState) error {
	p.writer.Lock()
	defer p.writer.Unlock()
	return p.stage(func(b *bolt.Bucket) error {
		if err := progressivePut(b, "state/"+sha, state); err != nil {
			return err
		}
		return p.publish(ctx, b)
	})
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
