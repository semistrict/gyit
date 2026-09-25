package repo

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	importerv1 "gat/internal/gen/gat/importer/v1"
	"google.golang.org/protobuf/proto"
)

func TestBlobJobSpoolIndependentProducer(t *testing.T) {
	dir := t.TempDir()
	q, e := newBlobJobSpool(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer q.close()
	// The producer must finish a large backlog without a running consumer.
	const count = 20000
	for i := 0; i < count; i++ {
		if e = q.add(blobImportJob{oid: fmt.Sprintf("%040x", i), hint: fmt.Sprintf("path %d\xff\n", i), size: int64(i)}, false); e != nil {
			t.Fatal(e)
		}
	}
	if e = q.finish(); e != nil {
		t.Fatal(e)
	}
	if e = q.finish(); e != nil {
		t.Fatal(e)
	}
	at := 0
	for {
		batch, e := q.batch(t.Context())
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		for _, j := range batch {
			if j.oid != fmt.Sprintf("%040x", at) || j.hint != fmt.Sprintf("path %d\xff\n", at) || j.size != int64(at) || j.waitRaw {
				t.Fatalf("job %d differs: %+v", at, j)
			}
			at++
		}
	}
	if at != count {
		t.Fatalf("lost work: got %d, want %d", at, count)
	}
	if e = q.add(blobImportJob{oid: strings.Repeat("a", 40)}, false); e == nil {
		t.Fatal("accepted work after sealing")
	}
	if e = q.close(); e != nil {
		t.Fatal(e)
	}
	files, e := os.ReadDir(dir)
	if e != nil || len(files) != 0 {
		t.Fatalf("queue cleanup: %v %v", files, e)
	}
}
func TestBlobJobSpoolFinalBatchConcurrent(t *testing.T) {
	for iteration := 0; iteration < 30; iteration++ {
		q, e := newBlobJobSpool(t.TempDir())
		if e != nil {
			t.Fatal(e)
		}
		done := make(chan error, 1)
		go func() {
			for i := 0; i < 513; i++ {
				if e := q.add(blobImportJob{oid: fmt.Sprintf("%040x", i), size: int64(i)}, false); e != nil {
					done <- e
					return
				}
			}
			done <- q.finish()
		}()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		at := 0
		for {
			jobs, e := q.batch(ctx)
			if e == io.EOF {
				break
			}
			if e != nil {
				t.Fatal(e)
			}
			for _, j := range jobs {
				if j.size != int64(at) {
					t.Fatalf("order: %d %+v", at, j)
				}
				at++
			}
		}
		cancel()
		if e := <-done; e != nil {
			t.Fatal(e)
		}
		if at != 513 {
			t.Fatalf("lost final batch: %d", at)
		}
		if e = q.close(); e != nil {
			t.Fatal(e)
		}
	}
}
func TestBlobJobSpoolWireAndRawMarker(t *testing.T) {
	dir := t.TempDir()
	q, e := newBlobJobSpool(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer q.close()
	opaque := "name\xff\x00\n"
	oid := strings.Repeat("a", 64)
	if e = q.add(blobImportJob{oid: oid, hint: opaque, size: 123}, false); e != nil {
		t.Fatal(e)
	}
	payload := bytes.Repeat([]byte("BODY_MUST_NOT_BE_SPOOLED"), 400)
	if e = q.add(blobImportJob{oid: oid, group: opaque, part: 9, raw: payload}, true); e != nil {
		t.Fatal(e)
	}
	// Raw rendezvous markers publish their batch without waiting for Finish.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	jobs, e := q.batch(ctx)
	if e != nil || len(jobs) != 2 || !jobs[1].waitRaw || jobs[1].part != 9 || jobs[1].group != opaque {
		t.Fatalf("marker: %+v %v", jobs, e)
	}
	paths, e := filepath.Glob(filepath.Join(dir, "blob-jobs-*"))
	if e != nil || len(paths) != 1 {
		t.Fatal(paths, e)
	}
	encoded, e := os.ReadFile(paths[0])
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Contains(encoded, []byte("BODY_MUST_NOT_BE_SPOOLED")) {
		t.Fatal("file body was spooled")
	}
	if len(encoded) < 4 || int(binary.LittleEndian.Uint32(encoded)) != len(encoded)-4 {
		t.Fatal("invalid batch frame")
	}
	var batch importerv1.BlobImportBatch
	if e = proto.Unmarshal(encoded[4:], &batch); e != nil {
		t.Fatal(e)
	}
	if len(batch.Jobs) != 2 || batch.Jobs[0].Oid != oid || !bytes.Equal(batch.Jobs[0].Hint, []byte(opaque)) || batch.Jobs[0].Size != 123 || !batch.Jobs[1].WaitForRaw || batch.Jobs[1].Part != 9 {
		t.Fatalf("protobuf interoperability: %v", &batch)
	}
}
func TestBlobJobSpoolCancellationAndFailure(t *testing.T) {
	t.Run("empty cancellation", func(t *testing.T) {
		q, e := newBlobJobSpool(t.TempDir())
		if e != nil {
			t.Fatal(e)
		}
		defer q.close()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, e = q.batch(ctx); !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	})
	t.Run("write failure", func(t *testing.T) {
		q, e := newBlobJobSpool(t.TempDir())
		if e != nil {
			t.Fatal(e)
		}
		defer q.close()
		if e = q.add(blobImportJob{oid: strings.Repeat("a", 40)}, false); e != nil {
			t.Fatal(e)
		}
		q.file.Close()
		if e = q.finish(); e == nil {
			t.Fatal("lost seal failure")
		}
		if _, e = q.batch(t.Context()); e == nil || e == io.EOF {
			t.Fatalf("failed queue looked complete: %v", e)
		}
	})
	t.Run("truncated batch", func(t *testing.T) {
		q, e := newBlobJobSpool(t.TempDir())
		if e != nil {
			t.Fatal(e)
		}
		defer q.close()
		if e = q.add(blobImportJob{oid: strings.Repeat("a", 40)}, true); e != nil {
			t.Fatal(e)
		}
		if e = q.file.Truncate(5); e != nil {
			t.Fatal(e)
		}
		if _, e = q.batch(t.Context()); e == nil {
			t.Fatal("accepted truncated batch")
		}
	})
	t.Run("oversized job", func(t *testing.T) {
		q, e := newBlobJobSpool(t.TempDir())
		if e != nil {
			t.Fatal(e)
		}
		defer q.close()
		if e = q.add(blobImportJob{oid: strings.Repeat("a", 40), hint: strings.Repeat("x", 3<<20)}, false); e == nil {
			t.Fatal("accepted unbounded path")
		}
	})
}
