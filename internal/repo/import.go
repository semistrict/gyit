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
	"hash"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
	bolt "go.etcd.io/bbolt"
	"gyit/internal/spill"
	"gyit/internal/store"
)

type ImportOptions struct {
	Repo    string
	TempDir string
	// Revision imports this commit's history; empty imports all local refs and HEAD.
	Revision string
	Progress func(Stats)
	// CompressionWorkers defaults to at most four available Go execution threads.
	CompressionWorkers int
	// DisableDeltas preserves independent frames for comparison or low-latency reads.
	DisableDeltas bool
	// DeltaDepth zero retains bounded native deltas when supported, or uses one
	// for chunk conversion. Explicit values select chunk conversion (1..MaxDeltaDepth).
	DeltaDepth int
	// DeltaCandidates zero selects automatically; chunk conversion uses four.
	// Explicit values select chunk conversion with at most eight per path/chunk hint.
	DeltaCandidates int
}
type Stats struct {
	retainedPackSource, retainedPackPrefix string
	retainedPackBytes                      int64
	Objects, Blobs, Bytes, UploadedBytes   int64
	Chunks, DeltaChunks                    int64
	MaxDepth                               int
	Generation                             string
	// ImportMode is archive for retained native packs, or reachable for converted objects.
	ImportMode, FallbackReason string
	Phase                      string
}

// RetainedGitPack identifies the immutable source pack copied by this import.
// Adapters can expose the same bytes without uploading a duplicate Git pack.
// Empty values mean the source was converted rather than retained.
func (s Stats) RetainedGitPack() (source, segmentPrefix string, size int64) {
	return s.retainedPackSource, s.retainedPackPrefix, s.retainedPackBytes
}

func git(ctx context.Context, dir string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_NO_REPLACE_OBJECTS=1", "GIT_NO_LAZY_FETCH=1")

	return cmd
}

type packWriter struct {
	ctx     context.Context
	store   store.Store
	prefix  string
	number  int
	data    []byte
	stats   *Stats
	onFlush func(int64)
}

func (p *packWriter) key() string { return fmt.Sprintf("packs/%s/%08x", p.prefix, p.number) }
func (p *packWriter) flush() error {
	if len(p.data) == 0 {
		return nil
	}
	if err := p.store.Put(p.ctx, p.key(), p.data, ""); err != nil {
		return err
	}
	p.stats.UploadedBytes += int64(len(p.data))
	if p.onFlush != nil {
		p.onFlush(int64(len(p.data)))
	}
	p.number++
	p.data = p.data[:0]
	return nil
}
func (p *packWriter) add(compressed []byte, hash string) (chunk, error) {
	if len(p.data)+len(compressed) > PackSize {
		if err := p.flush(); err != nil {
			return chunk{}, err
		}
	}
	c := chunk{Pack: p.key(), Offset: int64(len(p.data)), Length: int64(len(compressed)), Hash: hash}
	p.data = append(p.data, compressed...)
	return c, nil
}

// Import publishes only after all data and index pages are durable. Readers never
// follow mutable state after opening a snapshot. CAS protects against accidental
// competing writers; a CAS conflict leaves the winning HEAD unchanged. An I/O
// failure during publication can have an ambiguous outcome; readers stay pinned.
func Import(ctx context.Context, s store.Store, opt ImportOptions) (Stats, error) {
	return importWithMetadataThreshold(ctx, s, opt, 1_000_000)
}

func importSelected(ctx context.Context, s store.Store, opt ImportOptions, metadataThreshold int64) (stats Stats, retErr error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := opt.CompressionWorkers
	if workers < 0 {
		return stats, fmt.Errorf("compression workers cannot be negative")
	}
	if workers == 0 {
		workers = min(4, runtime.GOMAXPROCS(0))
	}
	depth, candidates := opt.DeltaDepth, opt.DeltaCandidates
	if depth == 0 {
		depth = 1
	}
	if candidates == 0 {
		candidates = 4
	}
	if depth < 1 || depth > MaxDeltaDepth {
		return stats, fmt.Errorf("delta depth must be 1..%d", MaxDeltaDepth)
	}
	if candidates < 1 || candidates > maxDeltaCandidates {
		return stats, fmt.Errorf("delta candidates must be 1..%d", maxDeltaCandidates)
	}
	var blobs *blobImporter
	progress := func() {
		if opt.Progress != nil {
			current := stats
			if blobs != nil {
				current.UploadedBytes += blobs.uploaded.Load()
			}
			opt.Progress(current)
		}
	}
	phase := func(name string) {
		stats.Phase = name
		progress()
	}
	stats.ImportMode = "reachable"
	stats.FallbackReason = importConfig(ctx).fallbackReason
	if archiveImportEnabled(ctx) {
		stats.ImportMode = "archive"
	}
	phase("enumerate")
	base, token, err := readHead(ctx, s)
	if errors.Is(err, store.ErrNotFound) {
		token = "*"
		base = manifest{Version: formatVersion}
	} else if err != nil {
		return stats, err
	} else if token == "" || token == "*" {
		// Empty means unconditional Put; '*' means create-only. Neither is a
		// version token, so never allow a broken backend to weaken publication.
		return stats, fmt.Errorf("cannot publish: object store returned no usable CAS token for HEAD")
	}
	if base.Root != (pageRef{}) && (archiveImportEnabled(ctx) || base.Blobs != (pageRef{}) || manifestHasGlobalSizes(base.Version)) {
		// Rebuild every immutable index and payload for a replacement generation.
		// Keep the real HEAD token for CAS and the source-format compatibility
		// check; no old logical roots may seed this complete rebuild.
		base = manifest{Version: formatVersion, Format: base.Format}
	}
	out, err := git(ctx, opt.Repo, "rev-parse", "--show-object-format").Output()
	if err != nil {
		return stats, fmt.Errorf("read source repository: %w", err)
	}
	format := strings.TrimSpace(string(out))
	oidBytes := 20
	if format == "sha256" {
		oidBytes = 32
	} else if format != "sha1" {
		return stats, fmt.Errorf("unsupported object format %q", format)
	}
	if base.Format != "" && base.Format != format {
		return stats, fmt.Errorf("source object format differs from destination")
	}
	var tips []string
	if opt.Revision != "" {
		out, err = git(ctx, opt.Repo, "rev-parse", "--verify", "--end-of-options", opt.Revision+"^{commit}").Output()
	} else {
		out, err = git(ctx, opt.Repo, "rev-list", "--all", "--no-walk").Output()
	}
	if err != nil {
		return stats, fmt.Errorf("resolve source commits: %w", err)
	}
	tips = strings.Fields(string(out))
	if len(tips) == 0 {
		return stats, fmt.Errorf("source has no commits")
	}
	input := strings.Join(tips, "\n") + "\n"
	if len(base.Tips) > 0 {
		check := git(ctx, opt.Repo, "cat-file", "--batch-check=%(objectname) %(objecttype)")
		check.Stdin = strings.NewReader(strings.Join(base.Tips, "\n") + "\n")
		out, err = check.Output()
		if err != nil {
			return stats, err
		}
		for _, line := range strings.Split(string(out), "\n") {
			f := strings.Fields(line)
			if len(f) == 2 && f[1] == "commit" {
				input += "^" + f[0] + "\n"
			}
		}
	}
	tmp, err := os.MkdirTemp(opt.TempDir, "gyit-import-*")
	if err != nil {
		return stats, err
	}
	defer os.RemoveAll(tmp)
	if archiveImportEnabled(ctx) {
		prefix, err := prepareArchivePack(ctx, nativeImportPack(ctx), tmp)
		if err != nil {
			return stats, err
		}
		config := importConfig(ctx)
		config.pack = prefix
		ctx = context.WithValue(ctx, importConfigurationKey{}, config)
	}
	// History depends only on the captured tips and the previous publication,
	// so build it alongside object conversion. Each task owns its staging data.
	historyInput := input
	if base.History == (pageRef{}) {
		historyInput = strings.Join(tips, "\n") + "\n"
	}
	var history pageRef
	var historyCount uint64
	var historyErr error
	historyDone := make(chan struct{})
	go func() {
		defer close(historyDone)
		history, historyCount, historyErr = importHistory(ctx, opt.Repo, historyInput, tmp, &index{store: s, cache: newCache(DefaultCacheBytes), root: base.History}, base)
		if historyErr == nil {
			streamingTrace("history_completed", int64(historyCount))
		}
		if historyErr != nil {
			// A canceled Git subprocess may report a broken stream or SIGKILL.
			// Preserve a genuine history failure, but normalize cancellation so
			// cleanup cannot overwrite the foreground task's original error.
			if ctx.Err() != nil {
				historyErr = ctx.Err()
			}
			cancel()
		}
	}()
	defer func() {
		cancel()
		<-historyDone
		if historyErr != nil && !errors.Is(historyErr, context.Canceled) && !errors.Is(historyErr, context.DeadlineExceeded) {
			retErr = historyErr
		}
	}()
	ids, err := os.Create(filepath.Join(tmp, "objects"))
	if err != nil {
		return stats, err
	}
	defer ids.Close()
	records, err := spill.New(tmp, 32<<20)
	if err != nil {
		return stats, err
	}
	defer records.Close()
	backfillGraph := base.Root != (pageRef{}) && (!base.RevisionGraph || !base.CommitMetadata)

	var direct *directBlobStage
	if archiveImportEnabled(ctx) {
		direct, err = newDirectBlobStage(tmp)
		if err != nil {
			return stats, err
		}
		defer direct.records.Close()
	}
	st := &stage{tmp: tmp, records: records, trackCommits: backfillGraph, direct: direct}
	defer st.close()
	// Collect Git's traversable parents with the object walk. In particular,
	// shallow boundaries must not regain parents from their raw commit headers.
	var sizes *blobSizes
	var early *earlyMetadata
	var native *earlyNative
	var stderr bytes.Buffer
	if base.Root == (pageRef{}) && !opt.DisableDeltas && workers > 1 && opt.Revision == "" && (archiveImportEnabled(ctx) || sourceSupportsMetadataPreload(ctx, opt.Repo, metadataThreshold)) {
		sizes, early, native, err = prepareEarly(ctx, cancel, opt.Repo, format, input, tmp, ids, workers, oidBytes, st, s, func() { phase("objects") })
	}
	if errors.Is(err, errMetadataInventoryUnavailable) && ctx.Err() == nil {
		err = nil
	}
	if err == nil && sizes == nil {
		walk := git(ctx, opt.Repo, "rev-list", "--objects", "--parents", "--stdin")
		walk.Stdin = strings.NewReader(input)
		walk.Stdout = ids
		walk.Stderr = &stderr
		if err = walk.Run(); err != nil {
			return stats, fmt.Errorf("enumerate source: %w: %s", err, stderr.String())
		}
		if _, err = ids.Seek(0, io.SeekStart); err != nil {
			return stats, err
		}
		sizes, err = prepareImportObjects(ctx, ids, tmp, opt.Repo, !opt.DisableDeltas, workers, func(id string, p []string) error { return st.put("p/"+id, parents{Parents: p}) })
	}
	if err != nil {
		return stats, err
	}
	defer sizes.Close()
	if archiveImportEnabled(ctx) && early == nil {
		return stats, fmt.Errorf("global size import requires actual metadata inventory path")
	}
	if native != nil {
		defer func() {
			cancel()
			if e := native.finish(nil); e != nil && !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
				retErr = e
			}
		}()
	}
	if early != nil {
		defer func() {
			cancel()
			if e := early.finish(nil); e != nil && !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
				retErr = e
			}
		}()
	}
	db, err := bolt.Open(filepath.Join(tmp, "stage.db"), 0600, &bolt.Options{NoSync: true})
	if err != nil {
		return stats, err
	}
	defer db.Close()
	idx := &index{store: s, cache: newCache(DefaultCacheBytes), root: base.Root}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return stats, err
	}
	packs := &packWriter{ctx: ctx, store: s, prefix: hex.EncodeToString(random), stats: &stats, data: make([]byte, 0, PackSize)}
	directoryEncoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithZeroFrames(true))
	if err != nil {
		return stats, err
	}
	defer directoryEncoder.Close()
	directories := &directoryWriter{pages: &indexWriter{ctx: ctx, store: s, prefix: "dirs-" + hex.EncodeToString(random)}, encoder: directoryEncoder, stage: st, sizes: sizes, old: idx, tmp: tmp}

	var seed func(string) ([]*deltaBase, error)
	if !opt.DisableDeltas && base.Root != (pageRef{}) {
		seed = func(hint string) ([]*deltaBase, error) {
			var record anchorRecord
			err := idx.get(ctx, anchorKey(hint), &record)
			if errors.Is(err, store.ErrNotFound) {
				// Version 4 stores published a single full anchor under the old prefix.
				var old chunk
				if err = idx.get(ctx, legacyAnchorKey(hint), &old); err == nil {
					record.Candidates = []chunkBase{chunkLocation(old)}
				}
			}
			if errors.Is(err, store.ErrNotFound) {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			var result []*deltaBase
			for _, loc := range record.Candidates {
				d, err := loc.depth()
				if err != nil {
					return nil, err
				}
				if d >= depth {
					continue
				}
				raw, err := (&Snapshot{idx: idx}).readChunk(ctx, loc.chunk())
				if err != nil {
					return nil, err
				}
				b := newDeltaBase(raw)
				b.location = loc
				b.depth = d
				result = append(result, b)
				if len(result) == candidates {
					break
				}
			}
			return result, nil
		}
	}
	var encoder *chunkEncoder
	// The direct pipeline pays for additional Git processes only on a large
	// fresh import. Updates retain the same existing-generation seed behavior.
	if base.Root == (pageRef{}) && !opt.DisableDeltas && workers > 1 && sizes.records >= 1024 {
		blobs, err = startBlobImporterMode(ctx, cancel, opt.Repo, format, workers, depth, candidates, st, packs, native != nil)
		if err != nil {
			return stats, err
		}
	} else {
		encoder, err = newChunkEncoder(ctx, workers, depth, candidates, seed, func(slot *encodeSlot) error { return writeEncodedChunk(slot, packs, st, &stats) })
		if err != nil {
			return stats, err
		}
	}
	defer func() {
		cancel()
		if encoder != nil {
			encoder.close()
		}
		if blobs != nil {
			_ = blobs.finish(nil)
			if blobs.failed != nil {
				retErr = blobs.failed
			}
		}
	}()
	directBlob := func(oid string) bool {
		if blobs == nil {
			return false
		}
		id, e := hex.DecodeString(oid)
		if e != nil {
			return false
		}
		size, found, e := sizes.get(id)
		return e == nil && found && size <= ChunkSize
	}

	var trees *treeImporter
	if early == nil && base.Root == (pageRef{}) && workers > 1 {
		trees = startTreeImporter(ctx, cancel, min(workers, 8), directories, oidBytes)
		defer func() {
			cancel()
			if e := trees.finish(); e != nil && !errors.Is(e, context.Canceled) {
				retErr = e
			}
		}()
	}
	cat := git(ctx, opt.Repo, "cat-file", "--batch-command", "--buffer")
	cat.Stderr = &stderr
	stdin, err := cat.StdinPipe()
	if err != nil {
		return stats, err
	}
	stdout, err := cat.StdoutPipe()
	if err != nil {
		stdin.Close()
		return stats, err
	}
	if err := cat.Start(); err != nil {
		return stats, err
	}
	waited := false
	defer func() {
		stdin.Close()
		if !waited {
			_ = cat.Process.Kill()
			_ = cat.Wait()
		}
	}()
	r := bufio.NewReaderSize(stdout, 64<<10)
	scan := bufio.NewScanner(ids)
	scan.Buffer(make([]byte, 64<<10), 1<<20)
	requests := &objectBatch{scan: scan, input: bufio.NewWriter(stdin), oidSize: oidBytes * 2, include: func(oid string) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if base.Root == (pageRef{}) {
			return true, nil
		}
		var existing object
		err := idx.get(ctx, "o/"+oid, &existing)
		if errors.Is(err, store.ErrNotFound) {
			return true, nil
		}
		return false, err
	}}
	requests.direct = directBlob
	buf := make([]byte, ChunkSize)
	commitReader := bufio.NewReaderSize(nil, 32<<10)
	phase("objects")
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if encoder != nil {
			if err := encoder.err(); err != nil {
				return stats, err
			}
		}
		oid, hint, err := requests.take()
		if err == io.EOF {
			break
		}
		if err != nil {
			return stats, err
		}
		if directBlob(oid) {
			id, _ := hex.DecodeString(oid)
			size, _, err := sizes.get(id)
			if err != nil {
				return stats, err
			}
			if err = blobs.add(oid, hint, size); err != nil {
				return stats, err
			}
			if err = st.put("o/"+oid, object{Kind: "blob", Size: size}); err != nil {
				return stats, err
			}
			stats.Objects++
			stats.Blobs++
			stats.Bytes += size
			progress()
			continue
		}
		header, err := r.ReadString('\n')
		if err != nil {
			return stats, err
		}
		f := strings.Fields(header)
		if len(f) != 3 || f[0] != oid {
			return stats, fmt.Errorf("invalid cat-file response: %q", header)
		}
		size, err := strconv.ParseInt(f[2], 10, 64)
		if err != nil || size < 0 {
			return stats, fmt.Errorf("invalid object size")
		}
		var h hash.Hash = sha1.New()
		if format == "sha256" {
			h = sha256.New()
		}
		fmt.Fprintf(h, "%s %d\x00", f[1], size)
		limited := &io.LimitedReader{R: r, N: size}
		body := io.TeeReader(limited, h)
		o := object{Kind: f[1], Size: size}
		deferredTree := false
		switch o.Kind {
		case "blob":
			for part := int64(0); ; part++ {
				n, err := io.ReadFull(body, buf)
				if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
					return stats, err
				}
				if n > 0 {
					group := ""
					if !opt.DisableDeltas && hint != "" {
						group = fmt.Sprintf("%s/%016x", hint, part)
					}
					var encodeErr error
					if blobs != nil {
						encodeErr = blobs.addRaw(oid, group, part, buf[:n])
					} else {
						encodeErr = encoder.addHint(chunkKey(oid, part), group, buf[:n])
					}
					if err := encodeErr; err != nil {
						return stats, err
					}
				}
				if err != nil {
					break
				}
			}
			stats.Blobs++
		case "tree":
			if trees != nil && size <= ChunkSize {
				if err := trees.add(oid, size, body); err != nil {
					return stats, err
				}
				deferredTree = true
				break
			}
			o.Directory, err = directories.readTree(body, oidBytes)
			if err != nil {
				return stats, err
			}
		case "commit":
			commitReader.Reset(body)
			tree, info, err := parseBufferedCommit(commitReader)
			if err != nil {
				return stats, err
			}
			if len(tree) != oidBytes*2 {
				return stats, fmt.Errorf("invalid commit tree")
			}
			o.Tree = tree
			if err := st.put("c/"+oid, info); err != nil {
				return stats, err
			}
		default:
			return stats, fmt.Errorf("unexpected reachable object kind %q", o.Kind)
		}
		if limited.N != 0 {
			return stats, io.ErrUnexpectedEOF
		}
		if hex.EncodeToString(h.Sum(nil)) != oid {
			return stats, fmt.Errorf("source object checksum mismatch: %s", oid)
		}
		sep, err := r.ReadByte()
		if err != nil || sep != '\n' {
			return stats, fmt.Errorf("invalid object separator")
		}
		if !deferredTree {
			if err := st.put("o/"+oid, o); err != nil {
				return stats, err
			}
		}
		stats.Objects++
		stats.Bytes += size
		progress()
	}
	stdin.Close()
	err = cat.Wait()
	waited = true
	if err != nil {
		return stats, fmt.Errorf("read source objects: %w: %s", err, stderr.String())
	}
	if blobs != nil {
		if err := blobs.finish(&stats); err != nil {
			return stats, err
		}
	} else if err := encoder.finish(); err != nil {
		return stats, err
	}
	if err := packs.flush(); err != nil {
		return stats, err
	}
	streamingTrace("fallback_done", stats.Blobs)
	streamingTrace("fallback_raw_bytes", stats.Bytes)
	// Older publications may lack records for commits excluded by this import's
	// reachability walk. Only that upgrade needs another complete graph walk.
	if backfillGraph {
		graphInput := strings.Join(tips, "\n") + "\n"
		if err := stageParents(ctx, opt.Repo, graphInput, format, st); err != nil {
			return stats, err
		}
	}
	if trees != nil {
		if err := trees.finish(); err != nil {
			return stats, err
		}
	}
	if early != nil {
		if e := early.finish(&stats); e != nil {
			return stats, e
		}
	}
	if native != nil {
		if e := native.finish(&stats); e != nil {
			return stats, e
		}
	}

	var ordered *orderedArchiveDispatch
	if sizes != nil && sizes.sourceInfo != nil {
		ordered = sizes.sourceInfo.ordered
	}
	if ordered != nil {
		if err := ordered.finish(&stats); err != nil {
			return stats, err
		}
	}
	if sizes.sourceInfo != nil && sizes.sourceInfo.archive != nil {
		if err := sizes.sourceInfo.archive.finish(&stats); err != nil {
			return stats, err
		}
	}
	if err := st.close(); err != nil {
		return stats, err
	}
	// Directory pages already contain stat sizes from the size prepass.
	if err := directories.pages.flush(); err != nil {
		return stats, err
	}

	streamingTrace("index_start", -1)
	phase("index")

	var blobRoot pageRef
	if direct != nil {
		if ordered != nil {
			blobRoot, err = direct.buildOrdered(ctx, s, ordered.Blobs.Next)
		} else {
			blobRoot, err = direct.build(ctx, s)
		}
		if err != nil {
			return stats, err
		}
	}

	var root pageRef
	if ordered != nil {
		root, err = idx.updateSortedOrdered(ctx, records, ordered.Main.Next)
	} else {
		root, err = idx.updateSorted(ctx, records)
	}
	if err != nil {
		return stats, err
	}
	refs, refsHash, err := importRefs(ctx, opt.Repo, db, &index{store: s, cache: idx.cache, root: root}, base)
	if err != nil {
		return stats, err
	}
	streamingTrace("index_end", -1)
	phase("history")
	<-historyDone
	if historyErr != nil {
		return stats, historyErr
	}
	phase("publish")
	m := manifest{Version: legacyFormatVersion, Format: format, Root: root, Tips: tips, Refs: refs, RevisionGraph: true, RefsHash: refsHash, CommitMetadata: true, History: history, HistoryCount: historyCount, Blobs: blobRoot}
	if archiveImportEnabled(ctx) {
		m.Version = globalSizesFormat
	}
	data, err := marshal(m)
	if err != nil {
		return stats, err
	}
	stats.Generation = fmt.Sprintf("%x", sha256.Sum256(data))
	if err := s.Put(ctx, "generations/"+stats.Generation, data, ""); err != nil {
		return stats, err
	}
	if err := s.Put(ctx, "HEAD", data, token); err != nil {
		if errors.Is(err, store.ErrConflict) {
			return stats, fmt.Errorf("%w; re-run import to retry against the latest HEAD", err)
		}
		return stats, err
	}
	streamingTrace("publication_done", -1)
	phase("done")
	return stats, nil
}
