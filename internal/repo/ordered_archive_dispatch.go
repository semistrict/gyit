package repo

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"

	archivewire "gat/internal/archive/wire"
	storagev1 "gat/internal/gen/gat/storage/v1"
	"gat/internal/gitdelta"
	orderedrows "gat/internal/orderedrows"
	"gat/internal/store"
	"google.golang.org/protobuf/proto"
)

// The only producer walks immutable source OIDs in their existing sorted order.
// Main and Blobs contain final records, not inputs to another sort or size join.
// Their readers are available only after finish. No OID population is retained.
type orderedArchiveDispatch struct {
	Main, Blobs                                       *orderedrows.Spool
	ctx                                               context.Context
	cancel                                            context.CancelFunc
	reader                                            *io.PipeReader
	done                                              chan struct{}
	err                                               error
	stopOnce, closeOnce                               sync.Once
	closeErr                                          error
	counted                                           bool
	trees                                             *importedNativeTree
	blobs                                             archiveNativeReader
	stats                                             Stats
	selected, commits, tags, blobBypass, treeRejected int64
}

type orderedArchiveInventory struct{ d *orderedArchiveDispatch }

func (r *orderedArchiveInventory) Read(p []byte) (int, error) { return r.d.reader.Read(p) }
func (r *orderedArchiveInventory) Close() error               { r.d.stop(); return nil }

// Both the returned stream and sourceInfo own the producer's lifetime. Closing
// the stream joins it but preserves the sealed spools for finalization; closing
// sourceInfo joins first and then removes those exact spool files.
func newOrderedArchiveInventory(ctx context.Context, tmp string, info *sourceInfo, backend store.Store) (io.ReadCloser, error) {
	if info == nil || info.metadata == nil || info.archive == nil || info.ordered != nil {
		return nil, fmt.Errorf("ordered archive requires one qualified native source")
	}
	main, err := orderedrows.New(tmp)
	if err != nil {
		return nil, err
	}
	blobs, err := orderedrows.New(tmp)
	if err != nil {
		return nil, errors.Join(err, main.Close())
	}
	var random [16]byte
	if _, err = rand.Read(random[:]); err != nil {
		return nil, errors.Join(err, main.Close(), blobs.Close())
	}
	cursor, err := info.metadata.NewOIDCursor(ctx)
	if err != nil {
		return nil, errors.Join(err, main.Close(), blobs.Close())
	}
	ctx, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	d := &orderedArchiveDispatch{Main: main, Blobs: blobs, ctx: ctx, cancel: cancel, reader: reader, done: make(chan struct{})}
	d.trees = &importedNativeTree{ordered: main, worker: &earlyWorker{}, pages: &indexWriter{ctx: ctx, store: backend, prefix: "tree-archive-ordered-" + hex.EncodeToString(random[:])}}
	d.blobs.archive = info.archive
	info.ordered = d
	info.archive.ordered = d
	go func() {
		defer close(d.done)
		stop := context.AfterFunc(ctx, func() { writer.CloseWithError(ctx.Err()) })
		defer stop()
		out := bufio.NewWriterSize(writer, 64<<10)
		streamingTrace("ordered_dispatch_start", -1)
		run := func() error {
			var row [96]byte
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				node, ok, err := cursor.Next()
				if err != nil {
					return err
				}
				if !ok {
					break
				}
				if node.Kind == 4 {
					d.tags++
					continue
				}
				if node.Kind < 1 || node.Kind > 3 || node.Size < 0 {
					return fmt.Errorf("ordered source metadata kind or size")
				}
				d.selected++
				owned, err := d.accept(node.OID, node.Kind, node.Size)
				if err != nil {
					return err
				}
				if owned {
					continue
				}
				b := hex.AppendEncode(row[:0], node.OID[:])
				switch node.Kind {
				case 1:
					b = append(b, " commit "...)
					d.commits++
				case 2:
					b = append(b, " tree "...)
				case 3:
					b = append(b, " blob "...)
				}
				b = strconv.AppendInt(b, node.Size, 10)
				b = append(b, '\n')
				if _, err = out.Write(b); err != nil {
					return err
				}
			}
			if err := d.trees.finish(); err != nil {
				return err
			}
			if err := d.Main.Seal(ctx); err != nil {
				return err
			}
			if err := d.Blobs.Seal(ctx); err != nil {
				return err
			}
			return out.Flush()
		}
		d.err = run()
		writer.CloseWithError(d.err)
		streamingTrace("ordered_dispatch_done", d.stats.Objects+d.trees.worker.stats.Objects)
	}()
	return &orderedArchiveInventory{d: d}, nil
}

func (d *orderedArchiveDispatch) accept(id [20]byte, kind byte, size int64) (bool, error) {
	if kind == 1 {
		return false, nil
	}
	if kind == 3 && (size == 0 || size > ChunkSize) {
		d.blobBypass++
		return false, nil
	}
	oid := hex.EncodeToString(id[:])
	recipe, err := d.blobs.archive.planner.Recipe(oid, d.blobs.archive.id)
	if errors.Is(err, archivewire.ErrLimit) || errors.Is(err, gitdelta.ErrLimit) {
		if kind == 3 {
			d.blobs.limits++
		} else {
			d.treeRejected++
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(recipe.Frames) == 0 || int64(recipe.Frames[len(recipe.Frames)-1].Size) != size {
		return false, fmt.Errorf("ordered archive target size mismatch")
	}
	if kind == 2 {
		return d.trees.stageRecipe(oid, recipe)
	}
	encoded, err := archivewire.Encode(recipe)
	if err != nil {
		return false, err
	}
	part := &storagev1.DirectBlobPart{Oid: id[:], Size: uint64(size), Chunk: &storagev1.ChunkRecord{Hash: "git-sha1:" + oid, ArchiveRecipe: encoded}}
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(part)
	if err != nil {
		return false, err
	}
	var key [28]byte
	copy(key[:20], id[:])
	binary.BigEndian.PutUint64(key[20:], 0)
	if err = d.Blobs.Add(d.ctx, key[:], value); err != nil {
		return false, err
	}
	value, err = marshal(object{Kind: "blob", Size: size})
	if err != nil {
		return false, err
	}
	if err = d.Main.Add(d.ctx, []byte("o/"+oid), value); err != nil {
		return false, err
	}
	d.stats.Objects++
	d.stats.Blobs++
	d.stats.Bytes += size
	d.stats.Chunks++
	if len(recipe.Frames) > 1 {
		d.stats.DeltaChunks++
		d.stats.MaxDepth = max(d.stats.MaxDepth, len(recipe.Frames)-1)
	}
	d.blobs.admitted++
	d.blobs.rawBytes += size
	d.blobs.recipeBytes += int64(len(encoded))
	d.blobs.frames += int64(len(recipe.Frames))
	return true, nil
}

func (d *orderedArchiveDispatch) stop() {
	d.stopOnce.Do(func() { d.cancel(); d.reader.Close(); <-d.done })
}
func (d *orderedArchiveDispatch) close() error {
	if d == nil {
		return nil
	}
	d.closeOnce.Do(func() { d.stop(); d.closeErr = errors.Join(d.err, d.Main.Close(), d.Blobs.Close()) })
	return d.closeErr
}
func (d *orderedArchiveDispatch) finish(stats *Stats) error {
	<-d.done
	if d.err != nil {
		return d.err
	}
	if stats != nil && !d.counted {
		for _, s := range []Stats{d.stats, d.trees.worker.stats} {
			stats.Objects += s.Objects
			stats.Blobs += s.Blobs
			stats.Bytes += s.Bytes
			stats.UploadedBytes += s.UploadedBytes
			stats.Chunks += s.Chunks
			stats.DeltaChunks += s.DeltaChunks
			stats.MaxDepth = max(stats.MaxDepth, s.MaxDepth)
		}
		d.counted = true
		for _, v := range []struct {
			name  string
			value int64
		}{
			{"ordered_selected_objects", d.selected}, {"ordered_commit_rows", d.commits}, {"ordered_tag_rows_skipped", d.tags},
			{"ordered_main_rows", d.stats.Objects + d.trees.worker.stats.Objects}, {"ordered_direct_rows", d.stats.Blobs},
			{"ordered_blob_bypasses", d.blobBypass}, {"ordered_tree_rejections", d.treeRejected},
		} {
			streamingTrace(v.name, v.value)
		}
		for _, stream := range []struct {
			name  string
			spool *orderedrows.Spool
		}{{"main", d.Main}, {"direct", d.Blobs}} {
			m := stream.spool.Metrics()
			streamingTrace("ordered_"+stream.name+"_key_bytes", int64(m.KeyBytes))
			streamingTrace("ordered_"+stream.name+"_value_bytes", int64(m.ValueBytes))
			streamingTrace("ordered_"+stream.name+"_framed_bytes", int64(m.FramedBytes))
		}
	}
	return nil
}
