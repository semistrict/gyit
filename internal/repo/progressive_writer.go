//go:build !js

package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
	"os"
	"runtime/trace"
)

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
	return p.publishIndex(ctx, uploads, func(idx *index) (pageRef, error) { return idx.update(ctx, b) })
}

// The caller holds writer. Build against the latest root, then atomically
// publish it only after all immutable data and metadata uploads have finished.
func (p *Progressive) publishIndex(ctx context.Context, uploads *publicationUploads, update func(*index) (pageRef, error)) error {
	backend := p.store
	if uploads != nil {
		backend = uploads
	}
	p.mu.RLock()
	rootBefore, expected := p.root, p.token
	p.mu.RUnlock()
	idx := &index{store: backend, cache: p.cache, root: rootBefore, containers: true}
	var root pageRef
	var err error
	trace.WithRegion(ctx, "publication-build-index", func() { root, err = update(idx) })
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
		trace.WithRegion(ctx, "publication-upload-wait", func() { err = uploads.wait() })
		if err != nil {
			return err
		}
	}
	var token string
	region := trace.StartRegion(ctx, "publication-CAS")
	defer region.End()
	if writer, ok := p.store.(store.VersionedWriter); ok {
		token, err = writer.PutVersion(ctx, "HEAD", raw, expected)
		if err != nil {
			return err
		}
		if token == "" || token == "*" {
			return fmt.Errorf("invalid publication version")
		}
	} else {
		if err = p.store.Put(ctx, "HEAD", raw, expected); err != nil {
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
	close(p.historyChanged)
	p.historyChanged = make(chan struct{})
	p.mu.Unlock()
	return nil
}
