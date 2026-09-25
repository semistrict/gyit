package repo

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"math"

	"gat/internal/orderedrows"
	storagev1 "gat/internal/gen/gat/storage/v1"
	"gat/internal/store"
	"google.golang.org/protobuf/proto"
)

// directPartKey is the final lookup order, not the size/part staging order.
func directPartKey(p *storagev1.DirectBlobPart) []byte {
	return binary.BigEndian.AppendUint64(bytes.Clone(p.Oid), p.Part)
}

// pullDirectParts owns a suspended producer. stop resumes and joins it, including
// all producer defers, before returning. No worker goroutine or queued rows remain.
func pullDirectParts(ctx context.Context, walk func(context.Context, func(*storagev1.DirectBlobPart) error) error) (orderedrows.Cursor, func()) {
	stopped := errors.New("ordered direct parts consumer stopped")
	seq := func(yield func(*storagev1.DirectBlobPart, error) bool) {
		err := walk(ctx, func(p *storagev1.DirectBlobPart) error {
			if !yield(p, nil) {
				return stopped
			}
			return nil
		})
		if err != nil && !errors.Is(err, stopped) {
			yield(nil, err)
		}
	}
	next, stop := iter.Pull2(seq)
	return func(ctx context.Context) ([]byte, []byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		p, err, ok := next()
		if !ok {
			return nil, nil, io.EOF
		}
		if err != nil {
			return nil, nil, err
		}
		if p == nil {
			return nil, nil, fmt.Errorf("nil ordered direct part")
		}
		value, err := proto.MarshalOptions{Deterministic: true}.Marshal(p)
		return directPartKey(p), value, err
	}, stop
}

func decodeOrderedDirectPart(key, value []byte) (*storagev1.DirectBlobPart, error) {
	if (len(key) != 28 && len(key) != 40) || len(value) > directBlobPageBytes {
		return nil, fmt.Errorf("ordered direct part row exceeds limits")
	}
	p := &storagev1.DirectBlobPart{}
	if err := (proto.UnmarshalOptions{RecursionLimit: MaxDeltaDepth + 4}).Unmarshal(value, p); err != nil {
		return nil, err
	}
	if (len(p.Oid) != 20 && len(p.Oid) != 32) || !bytes.Equal(key, directPartKey(p)) || p.Size > math.MaxInt64 {
		return nil, fmt.Errorf("ordered direct part identity mismatch")
	}
	if p.Size == 0 {
		if p.Part != 0 || p.Chunk != nil {
			return nil, fmt.Errorf("invalid empty ordered direct part")
		}
	} else if p.Part > (p.Size-1)/ChunkSize || p.Chunk == nil {
		return nil, fmt.Errorf("invalid ordered direct part")
	}
	if p.Chunk != nil {
		if len(p.Chunk.ArchiveRecipe) > 8192 {
			return nil, fmt.Errorf("ordered archive recipe exceeds limit")
		}
		depth := 0
		for base := p.Chunk.Base; base != nil; base = base.Base {
			depth++
			if depth > MaxDeltaDepth {
				return nil, fmt.Errorf("ordered direct dependency exceeds limit")
			}
		}
	}
	return p, nil
}

// buildOrdered merges final archive parts with joined ordinary parts. The caller
// owns and closes the sealed archive spool. Each archive key is OID || BE64(part)
// and its value is a deterministic DirectBlobPart protobuf. Only admitted,
// positive, single-part archived blobs belong to this stream.
func (d *directBlobStage) buildOrdered(ctx context.Context, backend store.Store, archive orderedrows.Cursor) (pageRef, error) {
	fallback, stop := pullDirectParts(ctx, d.walkParts)
	defer stop()
	if archive == nil {
		archive = func(context.Context) ([]byte, []byte, error) { return nil, nil, io.EOF }
	}
	var archiveKey []byte
	markedArchive := func(ctx context.Context) ([]byte, []byte, error) {
		key, value, err := archive(ctx)
		if err == nil {
			// The cursor owns these bytes until its next call. Merge does not
			// advance that cursor while its current row is still pending.
			archiveKey = key
		}
		return key, value, err
	}
	next := orderedrows.Merge(markedArchive, fallback)
	b := &directBlobBuilder{w: &indexWriter{ctx: ctx, store: backend, prefix: "blobs-" + rand.Text()}}
	var prior []byte
	var size, part uint64
	finish := func() error {
		if prior != nil && size != 0 && part != (size-1)/ChunkSize+1 {
			return fmt.Errorf("incomplete ordered direct blob")
		}
		return nil
	}
	for {
		key, value, err := next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return pageRef{}, err
		}
		p, err := decodeOrderedDirectPart(key, value)
		if err != nil {
			return pageRef{}, err
		}
		if bytes.Equal(key, archiveKey) {
			c := p.Chunk
			if p.Size == 0 || p.Size > ChunkSize || p.Part != 0 || c == nil || len(c.ArchiveRecipe) == 0 || c.Base != nil || c.Pack != "" || c.Offset != 0 || c.Length != 0 || c.Hash != fmt.Sprintf("git-sha1:%x", p.Oid) {
				return pageRef{}, fmt.Errorf("invalid ordered archive part")
			}
		}
		if !bytes.Equal(prior, p.Oid) {
			if err := finish(); err != nil {
				return pageRef{}, err
			}
			prior, size, part = bytes.Clone(p.Oid), p.Size, 0
		}
		if p.Size != size || p.Part != part {
			return pageRef{}, fmt.Errorf("inconsistent ordered direct blob parts")
		}
		part++
		if err := b.add(p); err != nil {
			return pageRef{}, err
		}
	}
	if err := finish(); err != nil {
		return pageRef{}, err
	}
	if err := ctx.Err(); err != nil {
		return pageRef{}, err
	}
	return b.finish()
}
