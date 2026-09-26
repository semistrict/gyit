package repo

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync/atomic"

	importerv1 "gyit/internal/gen/gyit/importer/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const blobJobBatchBytes = 2 << 20

// blobJobSpool decouples a single producer from a single worker using disposable
// protobuf batches. Only metadata goes to disk. Producer memory holds at most
// 128 jobs or 32 KiB plus one bounded path; the consumer reads one batch at a time.
// add/finish belong to the producer, batch to the consumer. close requires both
// to have stopped. ready publishes complete writes; closed publishes sealErr.
type blobJobSpool struct {
	file     *os.File
	ready    atomic.Int64
	closed   atomic.Bool
	changed  chan struct{}
	buffer   []byte
	count    int
	readAt   int64
	sealErr  error
	disposed bool
	closeErr error
}

func newBlobJobSpool(tmp string) (*blobJobSpool, error) {
	f, e := os.CreateTemp(tmp, "blob-jobs-*")
	if e != nil {
		return nil, e
	}
	return &blobJobSpool{file: f, changed: make(chan struct{}, 1)}, nil
}
func (q *blobJobSpool) signal() {
	select {
	case q.changed <- struct{}{}:
	default:
	}
}
func (q *blobJobSpool) flush() error {
	if len(q.buffer) == 0 {
		return nil
	}
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(q.buffer)))
	at := q.ready.Load()
	if _, e := q.file.WriteAt(header[:], at); e != nil {
		return e
	}
	if _, e := q.file.WriteAt(q.buffer, at+4); e != nil {
		return e
	}
	q.ready.Store(at + 4 + int64(len(q.buffer)))
	q.buffer = q.buffer[:0]
	q.count = 0
	q.signal()
	return nil
}
func (q *blobJobSpool) add(j blobImportJob, raw bool) error {
	if q.closed.Load() || q.disposed {
		return os.ErrClosed
	}
	if (len(j.oid) != 40 && len(j.oid) != 64) || len(j.hint)+len(j.group) > 1<<20 || j.size < 0 || j.size > ChunkSize || j.part < 0 {
		return fmt.Errorf("invalid blob work record")
	}
	// The wire layout is defined in gyit/importer/v1/importer.proto. Avoid one
	// generated allocation per queued field; tests use the generated decoder.
	b := make([]byte, 0, len(j.oid)+len(j.hint)+len(j.group)+48)
	for _, f := range []struct {
		number protowire.Number
		value  string
	}{{1, j.oid}, {2, j.hint}, {4, j.group}} {
		if f.value != "" {
			b = protowire.AppendTag(b, f.number, protowire.BytesType)
			b = protowire.AppendString(b, f.value)
		}
	}
	for _, f := range []struct {
		number protowire.Number
		value  int64
	}{{3, j.size}, {5, j.part}} {
		if f.value != 0 {
			b = protowire.AppendTag(b, f.number, protowire.VarintType)
			b = protowire.AppendVarint(b, uint64(f.value))
		}
	}
	if raw {
		b = protowire.AppendTag(b, 6, protowire.VarintType)
		b = protowire.AppendVarint(b, 1)
	}
	q.buffer = protowire.AppendTag(q.buffer, 1, protowire.BytesType)
	q.buffer = protowire.AppendBytes(q.buffer, b)
	q.count++
	// Publish a raw-body rendezvous before the producer can block sending it.
	if raw || len(q.buffer) >= 32<<10 || q.count == 128 {
		return q.flush()
	}
	return nil
}
func (q *blobJobSpool) finish() error {
	if q.closed.Load() {
		return q.sealErr
	}
	q.sealErr = q.flush()
	q.closed.Store(true)
	q.signal()
	return q.sealErr
}
func (q *blobJobSpool) close() error {
	if q.disposed {
		return q.closeErr
	}
	q.disposed = true
	q.closeErr = errors.Join(q.file.Close(), os.Remove(q.file.Name()))
	q.buffer = nil
	return q.closeErr
}
func (q *blobJobSpool) batch(ctx context.Context) ([]blobImportJob, error) {
	for {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		if q.closed.Load() && q.sealErr != nil {
			return nil, q.sealErr
		}
		if q.readAt < q.ready.Load() {
			break
		}
		if q.closed.Load() {
			// A final flush can happen between the first offset load and closed load.
			if q.readAt == q.ready.Load() {
				if q.sealErr != nil {
					return nil, q.sealErr
				}
				return nil, io.EOF
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.changed:
		}
	}
	var header [4]byte
	if _, e := q.file.ReadAt(header[:], q.readAt); e != nil {
		return nil, e
	}
	n := int(binary.LittleEndian.Uint32(header[:]))
	if n < 1 || n > blobJobBatchBytes || int64(n)+4 > q.ready.Load()-q.readAt {
		return nil, fmt.Errorf("invalid blob work batch size")
	}
	b := make([]byte, n)
	if _, e := q.file.ReadAt(b, q.readAt+4); e != nil {
		return nil, e
	}
	// Bound repeated-message allocation before invoking the generated decoder.
	count := 0
	for rest := b; len(rest) > 0; {
		number, kind, k := protowire.ConsumeTag(rest)
		if k < 0 || number != 1 || kind != protowire.BytesType {
			return nil, fmt.Errorf("invalid blob work batch wire")
		}
		_, length := protowire.ConsumeBytes(rest[k:])
		if length < 0 {
			return nil, fmt.Errorf("truncated blob work record")
		}
		rest = rest[k+length:]
		count++
		if count > 128 {
			return nil, fmt.Errorf("too many blob work records")
		}
	}
	var batch importerv1.BlobImportBatch
	if e := proto.Unmarshal(b, &batch); e != nil {
		return nil, e
	}
	jobs := make([]blobImportJob, 0, len(batch.Jobs))
	for _, j := range batch.Jobs {
		if (len(j.Oid) != 40 && len(j.Oid) != 64) || j.Size > ChunkSize || j.Part > math.MaxInt64 || len(j.Hint)+len(j.Group) > 1<<20 {
			return nil, fmt.Errorf("invalid queued blob work")
		}
		jobs = append(jobs, blobImportJob{oid: j.Oid, hint: string(j.Hint), size: int64(j.Size), group: string(j.Group), part: int64(j.Part), waitRaw: j.WaitForRaw})
	}
	q.readAt += int64(n) + 4
	return jobs, nil
}
