package repo

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	archivecopy "gyit/internal/archive"
	archiveplan "gyit/internal/archive/planner"
	archivewire "gyit/internal/archive/wire"
	"gyit/internal/gitdelta"
	orderedrows "gyit/internal/orderedrows"
	"gyit/internal/packfile"
	"gyit/internal/store"
)

// Shared immutable physical planning and one durable copy belong to an import,
// not to a conversion worker. Readers never need the source repository.
type sourceArchiveImport struct {
	ordered              *orderedArchiveDispatch
	planner              sourceRecipePlanner
	id                   [16]byte
	source, prefix       string
	ctx                  context.Context
	cancel               context.CancelFunc
	done                 chan struct{}
	copied               archivecopy.Result
	copyErr              error
	countOnce, closeOnce sync.Once
	closeErr             error
}

func startSourceArchive(ctx context.Context, source, tmp string, info *sourceInfo, backend store.Store) (*sourceArchiveImport, error) {
	ctx, cancel := context.WithCancel(ctx)
	a := &sourceArchiveImport{ctx: ctx, cancel: cancel, source: source, prefix: nativeImportPack(ctx), done: make(chan struct{})}
	if _, err := rand.Read(a.id[:]); err != nil {
		cancel()
		return nil, err
	}
	f, err := os.Open(a.prefix + ".pack")
	if err != nil {
		cancel()
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		cancel()
		return nil, err
	}
	go func() {
		defer close(a.done)
		defer f.Close()
		streamingTrace("archive_copy_start", fi.Size())
		a.copied, a.copyErr = archivecopy.Copy(ctx, backend, f, fi.Size(), a.id)
		streamingTrace("archive_copy_end", int64(a.copied.Bytes))
	}()
	if info.metadata != nil {
		a.planner = metadataRecipeAdapter{info.metadata}
		streamingTrace("archive_plan_reused", 1)
		return a, nil
	}
	streamingTrace("archive_plan_start", -1)
	planner, err := archiveplan.Open(ctx, a.prefix, func(id [20]byte) (byte, int64, bool, error) { return info.lookup(id[:]) }, tmp)
	if err == nil {
		a.planner = planner
	}
	if err != nil {
		a.close()
		return nil, err
	}
	streamingTrace("archive_plan_end", -1)
	return a, nil
}
func (a *sourceArchiveImport) close() error {
	a.closeOnce.Do(func() {
		a.cancel()
		<-a.done
		a.closeErr = a.copyErr
		if a.planner != nil {
			a.closeErr = errors.Join(a.closeErr, a.planner.Close())
		}
	})
	return a.closeErr
}
func (a *sourceArchiveImport) finish(stats *Stats) error {
	<-a.done
	if a.copyErr != nil {
		return a.copyErr
	}
	if err := a.ctx.Err(); err != nil {
		return err
	}
	retainedSource, err := filepath.EvalSymlinks(a.prefix + ".pack")
	if err != nil {
		return err
	}
	a.countOnce.Do(func() {
		stats.UploadedBytes += int64(a.copied.Bytes)
		stats.retainedPackSource = retainedSource
		stats.retainedPackPrefix = fmt.Sprintf("packs/archive-%x", a.id)
		stats.retainedPackBytes = a.copied.Bytes
		streamingTrace("archive_source_bytes", int64(a.copied.Bytes))
		streamingTrace("archive_segments", int64(a.copied.Segments))
		p := a.planner.Stats()
		for _, v := range []struct {
			name  string
			value uint64
		}{
			{"archive_plan_objects", p.SourceObjects}, {"archive_plan_inventory_lookups", p.InventoryLookups},
			{"archive_plan_headers", p.SourceHeaders}, {"archive_plan_header_bytes", p.SourceHeaderBytes},
			{"archive_plan_dp_nodes", p.DPNodeVisits}, {"archive_plan_dp_edges", p.DPEdgeVisits},
			{"archive_plan_limit_nodes", p.LimitNodes}, {"archive_plan_malformed_nodes", p.MalformedNodes},
			{"archive_plan_source_mapped_bytes", p.SourceMappedBytes}, {"archive_plan_scratch_mapped_bytes", p.ScratchMappedBytes},
			{"archive_plan_peak_scratch_mapped_bytes", p.BuildPeakScratchMappedBytes},
			{"archive_recipe_calls", p.RecipeCalls}, {"archive_recipe_accepted", p.RecipeAccepted},
			{"archive_recipe_limit_fallbacks", p.RecipeLimitFallbacks}, {"archive_recipe_malformed_failures", p.RecipeMalformedFailures},
			{"archive_recipe_parent_visits", p.RecipeParentVisits}, {"archive_recipe_bytes", p.RecipeBytes},
			{"archive_recipe_fetched_bytes", p.RecipeFetchedBytes}, {"archive_recipe_work_bytes", p.RecipeWork},
		} {
			streamingTrace(v.name, int64(v.value))
		}
	})
	return nil
}

type archiveNativeReader struct {
	archive                                         *sourceArchiveImport
	fallback                                        *packrecipe.Reader
	admitted, rawBytes, recipeBytes, frames, limits int64
}

func startArchiveNative(ctx context.Context, cancel context.CancelFunc, source, format, tmp string, workers int, st *stage, backend store.Store, a *sourceArchiveImport) (*earlyNative, error) {
	if a == nil {
		return startEarlyNative(ctx, cancel, source, format, tmp, workers, st, backend)
	}
	return startEarlyNativeReaders(ctx, cancel, tmp, workers, st, backend, func(int) (earlyNativeReader, error) { return &archiveNativeReader{archive: a}, nil })
}
func (r *archiveNativeReader) Has(oid string) bool { return r.archive.planner.Has(oid) }
func (r *archiveNativeReader) Close() {
	if r.fallback != nil {
		r.fallback.Close()
	}
}
func (r *archiveNativeReader) Convert(oid string, body []byte) (packrecipe.Output, error) {
	if r.fallback == nil {
		var err error
		r.fallback, err = packrecipe.OpenDeferred(r.archive.ctx, r.archive.prefix, r.archive.source)
		if err != nil {
			return packrecipe.Output{}, err
		}
	}
	return r.fallback.Convert(oid, body)
}
func archiveFallbackReader(r earlyNativeReader) *packrecipe.Reader {
	switch r := r.(type) {
	case *packrecipe.Reader:
		return r
	case *archiveNativeReader:
		return r.fallback
	}
	return nil
}
func (r *archiveNativeReader) stage(job blobImportJob, st *stage, stats *Stats) (bool, error) {
	// Ordered dispatch already classified every eligible payload once.
	if r.archive.ordered != nil {
		return false, nil
	}
	recipe, err := r.archive.planner.Recipe(job.oid, r.archive.id)
	if errors.Is(err, archivewire.ErrLimit) || errors.Is(err, gitdelta.ErrLimit) {
		r.limits++
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(recipe.Frames) == 0 || int64(recipe.Frames[len(recipe.Frames)-1].Size) != job.size {
		return false, fmt.Errorf("archive blob size mismatch")
	}
	encoded, err := archivewire.Encode(recipe)
	if err != nil {
		return false, err
	}
	c := chunk{Hash: "git-sha1:" + job.oid, ArchiveRecipe: string(encoded)}
	if err = st.put(chunkKey(job.oid, 0), c); err != nil {
		return false, err
	}
	if err = st.put("o/"+job.oid, object{Kind: "blob", Size: job.size}); err != nil {
		return false, err
	}
	stats.Objects++
	stats.Blobs++
	stats.Bytes += job.size
	stats.Chunks++
	if len(recipe.Frames) > 1 {
		stats.DeltaChunks++
		stats.MaxDepth = max(stats.MaxDepth, len(recipe.Frames)-1)
	}
	r.admitted++
	r.rawBytes += job.size
	r.recipeBytes += int64(len(encoded))
	r.frames += int64(len(recipe.Frames))
	return true, nil
}
func traceArchiveBlobs(workers []*earlyNativeWorker) {
	var sum archiveNativeReader
	for _, w := range workers {
		if r, ok := w.reader.(*archiveNativeReader); ok && r.archive.ordered != nil {
			<-r.archive.ordered.done
			sum = r.archive.ordered.blobs
			break
		}
	}
	for _, w := range workers {
		if r, ok := w.reader.(*archiveNativeReader); ok {
			sum.admitted += r.admitted
			sum.rawBytes += r.rawBytes
			sum.recipeBytes += r.recipeBytes
			sum.frames += r.frames
			sum.limits += r.limits
		}
	}
	for _, v := range []struct {
		name  string
		value int64
	}{{"archive_blob_admitted", sum.admitted}, {"archive_blob_raw_bytes", sum.rawBytes}, {"archive_blob_recipe_bytes", sum.recipeBytes}, {"archive_blob_frames", sum.frames}, {"archive_blob_limits", sum.limits}} {
		streamingTrace(v.name, v.value)
	}
}

type importedNativeTree struct {
	ordered                                     *orderedrows.Spool
	pages                                       *indexWriter
	worker                                      *earlyWorker
	admitted, rawBytes, descriptorBytes, frames int64
}

func (n *importedNativeTree) finish() error {
	if n == nil {
		return nil
	}
	return n.pages.flush()
}
func (n *importedNativeTree) stageRecipe(oid string, recipe archivewire.Recipe) (bool, error) {
	size := int64(recipe.Frames[len(recipe.Frames)-1].Size)
	if size > 64<<10 {
		return false, nil
	}
	encoded, err := archivewire.Encode(recipe)
	if err != nil {
		return false, err
	}
	ref, err := n.pages.saveBytes(encoded)
	if err != nil {
		return false, err
	}
	obj := object{Kind: "tree", Size: size, Directory: ref}
	value, err := marshal(obj)
	if err != nil {
		return false, err
	}
	err = n.ordered.Add(n.pages.ctx, []byte("o/"+oid), value)
	if err != nil {
		return false, err
	}
	n.admitted++
	n.rawBytes += size
	n.descriptorBytes += int64(len(encoded))
	n.frames += int64(len(recipe.Frames))
	n.worker.treeStored++
	n.worker.stats.Objects++
	n.worker.stats.Bytes += size
	return true, nil
}
func traceImportedTrees(workers []*earlyWorker, ordered *orderedArchiveDispatch) {
	var sum importedNativeTree
	if ordered != nil {
		<-ordered.done
		sum = *ordered.trees
	}
	var fallback, fallbackBytes, requests int64
	for _, w := range workers {
		fallback += w.treeVerified
		fallbackBytes += w.treeBytes
		requests += w.treeBodyRequests

	}
	for _, v := range []struct {
		name  string
		value int64
	}{{"tree_total", fallback + sum.admitted}, {"tree_total_bytes", fallbackBytes + sum.rawBytes}, {"tree_native_admitted", sum.admitted}, {"tree_native_raw_bytes", sum.rawBytes}, {"tree_native_fallback", fallback}, {"tree_native_fallback_bytes", fallbackBytes}, {"tree_body_requests", requests}, {"tree_native_descriptor_bytes", sum.descriptorBytes}, {"archive_tree_frames", sum.frames}} {
		streamingTrace(v.name, v.value)
	}
}
