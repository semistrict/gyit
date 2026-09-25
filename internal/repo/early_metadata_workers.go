package repo

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"gat/internal/store"
	"github.com/klauspost/compress/zstd"
	"hash"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type earlyWorker struct {
	treeBodyRequests                                                  int64
	queue                                                             *blobJobSpool
	stats                                                             Stats
	err                                                               error
	treeVerified, treeBytes, treeStored, treeEntries, treeBlobLookups int64
	commitVerified, commitBytes, commitStored, commitInfo             int64
	commitParents                                                     int64
}
type earlyMetadata struct {
	globalTableBytes     int64
	ctx                  context.Context
	cancel               context.CancelFunc
	workers              []*earlyWorker
	next                 int
	wg                   sync.WaitGroup
	once                 sync.Once
	sealOnce             sync.Once
	sealErr, errorResult error
	counted              bool
}

func startEarlyMetadata(ctx context.Context, cancel context.CancelFunc, source, format, tmp string, workers int, sizes *blobSizes, st *stage, backend store.Store) (*earlyMetadata, error) {
	p := &earlyMetadata{ctx: ctx, cancel: cancel}
	for i := 0; i < workers; i++ {
		q, e := newBlobJobSpool(tmp)
		if e != nil {
			for _, w := range p.workers {
				w.queue.close()
			}
			return nil, e
		}
		p.workers = append(p.workers, &earlyWorker{queue: q})
	}
	var random [16]byte
	if _, e := rand.Read(random[:]); e != nil {
		for _, w := range p.workers {
			w.queue.close()
		}
		return nil, e
	}
	prefix := "dirs-" + hex.EncodeToString(random[:])
	var pending atomic.Int32
	pending.Store(int32(len(p.workers)))
	for i, w := range p.workers {
		p.wg.Add(1)
		go func(i int, w *earlyWorker) {
			defer p.wg.Done()
			defer func() {
				if pending.Add(-1) == 0 {
					var objects, rawBytes, trees, treeBytes, treeStored, treeEntries, treeBlobLookups int64
					var commits, commitBytes, commitStored, commitInfo, commitParents int64
					for _, worker := range p.workers {
						objects += worker.stats.Objects
						rawBytes += worker.stats.Bytes
						trees += worker.treeVerified
						treeBytes += worker.treeBytes
						treeStored += worker.treeStored
						treeEntries += worker.treeEntries
						treeBlobLookups += worker.treeBlobLookups
						commits += worker.commitVerified
						commitBytes += worker.commitBytes
						commitStored += worker.commitStored
						commitInfo += worker.commitInfo
						commitParents += worker.commitParents
					}
					var ordered *orderedArchiveDispatch
					if sizes.sourceInfo != nil {
						ordered = sizes.sourceInfo.ordered
					}
					if ordered != nil {
						<-ordered.done
						treeStored += ordered.trees.worker.treeStored
					}
					traceImportedTrees(p.workers, ordered)
					streamingTrace("metadata_workers_exited", objects)
					streamingTrace("metadata_raw_bytes", rawBytes)
					streamingTrace("tree_source_verified", trees)
					streamingTrace("tree_source_bytes", treeBytes)
					streamingTrace("tree_identities_staged", treeStored)
					streamingTrace("tree_entries_validated", treeEntries)
					streamingTrace("tree_blob_lookups", treeBlobLookups)
					streamingTrace("commit_source_verified", commits)
					streamingTrace("commit_source_bytes", commitBytes)
					streamingTrace("commit_identities_staged", commitStored)
					streamingTrace("commit_info_staged", commitInfo)
					if archiveImportEnabled(ctx) {
						streamingTrace("all_local_parent_records_staged", commitParents)
					}
				}
			}()
			w.err = w.run(ctx, source, format, tmp, prefix, i, workers, sizes, st, backend)
			if w.err != nil {
				if ctx.Err() != nil {
					w.err = ctx.Err()
				}
				cancel()
			}
		}(i, w)
	}
	return p, nil
}
func (p *earlyMetadata) add(oid, kind string) error {
	if e := p.ctx.Err(); e != nil {
		return e
	}
	if kind != "tree" && kind != "commit" {
		return fmt.Errorf("unexpected early kind %s", kind)
	}
	q := p.workers[p.next%len(p.workers)].queue
	p.next++
	return q.add(blobImportJob{oid: oid, hint: kind}, false)
}
func (p *earlyMetadata) seal() error {
	p.sealOnce.Do(func() {
		for _, w := range p.workers {
			p.sealErr = errors.Join(p.sealErr, w.queue.finish())
		}
		if p.sealErr != nil {
			p.cancel()
		}
	})
	return p.sealErr
}
func (p *earlyMetadata) finish(stats *Stats) error {
	p.once.Do(func() {
		p.seal()
		p.wg.Wait()
		streamingTrace("metadata_workers_joined", -1)
		p.errorResult = p.sealErr
		for _, w := range p.workers {
			if w.err != nil && !errors.Is(w.err, context.Canceled) && !errors.Is(w.err, context.DeadlineExceeded) {
				p.errorResult = errors.Join(p.errorResult, w.err)
			}
			p.errorResult = errors.Join(p.errorResult, w.queue.close())
		}
		if p.errorResult == nil {
			p.errorResult = p.ctx.Err()
		}
	})
	if p.errorResult == nil && stats != nil && !p.counted {
		for _, w := range p.workers {
			stats.Objects += w.stats.Objects
			stats.Bytes += w.stats.Bytes
			stats.UploadedBytes += w.stats.UploadedBytes
		}
		stats.UploadedBytes += p.globalTableBytes
		p.counted = true
	}
	return p.errorResult
}
func (w *earlyWorker) run(ctx context.Context, source, format, tmp, prefix string, ordinal, workers int, sizes *blobSizes, st *stage, backend store.Store) error {
	allLocal := archiveImportEnabled(ctx)
	own, e := os.MkdirTemp(tmp, "early-worker-*")
	if e != nil {
		return e
	}
	defer os.RemoveAll(own)
	discardTrees := false
	validator := ceilingTreeValidator{tmp: own}
	encoder, e := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithZeroFrames(true))
	if e != nil {
		return e
	}
	defer encoder.Close()
	pages := &indexWriter{ctx: ctx, store: backend, prefix: prefix, number: ordinal, step: workers}
	writer := &directoryWriter{pages: pages, encoder: encoder, stage: st, sizes: sizes, old: &index{store: backend, cache: newCache(DefaultCacheBytes)}, tmp: own}
	cmd := git(ctx, source, "cat-file", "--batch-command", "--buffer")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	input, e := cmd.StdinPipe()
	if e != nil {
		return e
	}
	defer input.Close()
	output, e := cmd.StdoutPipe()
	if e != nil {
		return e
	}
	if e = cmd.Start(); e != nil {
		return e
	}
	waited := false
	defer func() {
		if !waited {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	requests := bufio.NewWriterSize(input, 65536)
	reader := bufio.NewReaderSize(output, 65536)
	commitReader := bufio.NewReaderSize(nil, 32768)
	oidBytes := 20
	if format == "sha256" {
		oidBytes = 32
	}
	var checksum hash.Hash = sha1.New()
	if format == "sha256" {
		checksum = sha256.New()
	}
	for {
		jobs, e := w.queue.batch(ctx)
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		if len(jobs) == 0 {
			continue
		}
		for _, j := range jobs {
			if j.hint == "tree" {
				w.treeBodyRequests++
			}
			if _, e = fmt.Fprintf(requests, "contents %s\n", j.oid); e != nil {
				return e
			}
		}
		if _, e = requests.WriteString("flush\n"); e != nil {
			return e
		}
		if e = requests.Flush(); e != nil {
			return e
		}
		for _, j := range jobs {
			line, e := reader.ReadString('\n')
			if e != nil {
				return e
			}
			f := strings.Fields(line)
			if len(f) != 3 || f[0] != j.oid || f[1] != j.hint {
				return fmt.Errorf("early source header")
			}
			size, e := strconv.ParseInt(f[2], 10, 64)
			if e != nil || size < 0 {
				return fmt.Errorf("early source size")
			}
			checksum.Reset()
			fmt.Fprintf(checksum, "%s %d\x00", f[1], size)
			limited := &io.LimitedReader{R: reader, N: size}
			body := io.TeeReader(limited, checksum)
			var rawParents []string
			o := object{Kind: f[1], Size: size}
			if o.Kind == "tree" {
				if discardTrees {
					e = validator.validate(ctx, body, oidBytes, writer.fileSize)
				} else {
					o.Directory, e = writer.readTree(body, oidBytes)
				}
				if e != nil {
					return e
				}
			} else {
				commitReader.Reset(body)
				var sink *[]string
				if allLocal {
					sink = &rawParents
				}
				tree, info, e := parseBufferedCommitParents(commitReader, sink, oidBytes)
				if e != nil {
					return e
				}
				if len(tree) != oidBytes*2 {
					return fmt.Errorf("early commit tree")
				}
				o.Tree = tree
				if e = st.put("c/"+j.oid, info); e != nil {
					return e
				}
				w.commitInfo++
			}
			if limited.N != 0 {
				return io.ErrUnexpectedEOF
			}
			if hex.EncodeToString(checksum.Sum(nil)) != j.oid {
				return fmt.Errorf("source object checksum mismatch: %s", j.oid)
			}
			if o.Kind == "tree" {
				w.treeVerified++
				w.treeBytes += size
				counts := writer.ceilingCounts
				if discardTrees {
					counts = validator.counts
				}
				w.treeEntries += counts.entries
				w.treeBlobLookups += counts.blobLookups
			} else {
				w.commitVerified++
				w.commitBytes += size
			}
			if b, e := reader.ReadByte(); e != nil || b != '\n' {
				return fmt.Errorf("early separator")
			}
			if allLocal && o.Kind == "commit" {
				if e = st.put("p/"+j.oid, parents{Parents: rawParents}); e != nil {
					return e
				}
				w.commitParents++
			}
			if o.Kind != "tree" || !discardTrees {
				if e = st.put("o/"+j.oid, o); e != nil {
					return e
				}
				if o.Kind == "tree" {
					w.treeStored++
				} else {
					w.commitStored++
				}
			}
			w.stats.Objects++
			w.stats.Bytes += size
		}
	}
	input.Close()
	e = cmd.Wait()
	waited = true
	if e != nil {
		return fmt.Errorf("early source: %w: %s", e, stderr.String())
	}
	if e = pages.flush(); e != nil {
		return e
	}
	return nil
}
