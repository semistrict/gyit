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
