package repo

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	metaplan "gyit/internal/packmeta"
)

// Retaining a native pack requires it to be the complete local object database.
// Unsupported sources retain ordinary metadata acquisition.
func qualifyPackSource(ctx context.Context, source string, oidBytes int) (string, bool, error) {
	if oidBytes != 20 {
		return "", false, nil
	}
	for _, name := range []string{"GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_SHALLOW_FILE"} {
		if os.Getenv(name) != "" {
			return "", false, nil
		}
	}
	path := func(name string) (string, error) {
		b, e := git(ctx, source, "rev-parse", "--git-path", name).Output()
		if e != nil {
			return "", e
		}
		p := strings.TrimSpace(string(b))
		if !filepath.IsAbs(p) {
			p = filepath.Join(source, p)
		}
		return filepath.Abs(p)
	}
	objects, e := path("objects")
	if e != nil {
		return "", false, e
	}
	shallow, e := path("shallow")
	if e != nil {
		return "", false, e
	}
	for _, p := range []string{shallow, filepath.Join(objects, "info", "alternates")} {
		if _, e = os.Stat(p); e == nil {
			return "", false, nil
		} else if !errors.Is(e, os.ErrNotExist) {
			return "", false, e
		}
	}
	entries, e := os.ReadDir(objects)
	if e != nil {
		return "", false, e
	}
	for _, entry := range entries {
		if entry.Name() != "info" && entry.Name() != "pack" {
			return "", false, nil
		}
	}
	packs, e := filepath.Glob(filepath.Join(objects, "pack", "*.pack"))
	if e != nil {
		return "", false, e
	}
	if len(packs) != 1 {
		return "", false, nil
	}
	actual, e := filepath.EvalSymlinks(packs[0])
	if e != nil {
		return "", false, e
	}
	actual, e = filepath.Abs(actual)
	if e != nil {
		return "", false, e
	}
	prefix := strings.TrimSuffix(actual, ".pack")
	for _, suffix := range []string{".idx"} {
		st, e := os.Stat(prefix + suffix)
		if errors.Is(e, os.ErrNotExist) {
			return "", false, nil
		}
		if e != nil {
			return "", false, e
		}
		if !st.Mode().IsRegular() {
			return "", false, nil
		}
	}
	promos, e := filepath.Glob(filepath.Join(objects, "pack", "*.promisor"))
	if e != nil {
		return "", false, e
	}
	if len(promos) != 0 {
		return "", false, nil
	}
	return prefix, true, nil
}

func loadPackSourceInfo(ctx context.Context, tmp, source string, oidBytes int) (result *sourceInfo, used bool, retErr error) {
	if archiveImportEnabled(ctx) {
		ok, err := qualifyRawCommitParents(ctx, source)
		if err != nil {
			return nil, true, err
		}
		if !ok {
			return nil, true, fmt.Errorf("all-local commits require unmodified complete source history")
		}
	}
	prefix, ok, e := qualifyPackSource(ctx, source, oidBytes)
	if archiveImportEnabled(ctx) && e == nil && !ok {
		return nil, true, fmt.Errorf("all-local commits require one complete supported source pack")
	}
	if e != nil || !ok {
		return nil, false, e
	}
	if configured := nativeImportPack(ctx); configured != "" {
		prefix = configured
	}
	streamingTrace("pack_metadata_start", -1)
	p, e := metaplan.OpenSource(ctx, prefix, tmp)
	if errors.Is(e, metaplan.ErrUnsupported) {
		streamingTrace("pack_metadata_unsupported", 1)
		if archiveImportEnabled(ctx) {
			return nil, true, fmt.Errorf("all-local commit source metadata unsupported: %w", e)
		}
		return nil, false, nil
	}
	if e != nil {
		return nil, true, e
	}
	s := &sourceInfo{metadata: p}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, s.close())
		}
	}()
	v := p.Stats()
	for _, row := range []struct {
		name  string
		value uint64
	}{
		{"pack_metadata_objects", v.MetadataObjects}, {"pack_metadata_blobs", v.MetadataBlobs},
		{"pack_metadata_trees", v.MetadataTrees}, {"pack_metadata_commits", v.MetadataCommits}, {"pack_metadata_tags", v.MetadataTags},
		{"pack_metadata_prefix_calls", v.PrefixInflations}, {"pack_metadata_prefix_input_bytes", v.PrefixInputBytes}, {"pack_metadata_prefix_output_bytes", v.PrefixOutputBytes},
		{"pack_metadata_type_nodes", v.TypeDPNodeVisits}, {"pack_metadata_type_edges", v.TypeDPEdgeVisits},
		{"pack_metadata_retained_bytes", v.ScratchMappedBytes}, {"pack_metadata_build_peak_bytes", v.BuildPeakScratchMappedBytes},
	} {
		streamingTrace(row.name, int64(row.value))
	}
	streamingTrace("pack_metadata_end", int64(v.MetadataObjects))
	streamingTrace("pack_global_records_start", -1)
	if e = p.ForEachBlob(ctx, func(id [20]byte, size int64) error { return s.collectGlobalSize(id[:], size) }); e != nil {
		return nil, true, e
	}
	streamingTrace("pack_global_records_end", int64(len(s.globalRecords)))
	return s, true, nil
}

// A bounded in-memory producer supplies the existing selector without a disk
// spool or a second metadata array. Close joins it before mappings are released.
type packInventory struct {
	*io.PipeReader
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
	err    error
}

func newPackInventory(ctx context.Context, p *metaplan.Planner) *packInventory {
	includeCommits := archiveImportEnabled(ctx)
	ctx, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	s := &packInventory{PipeReader: reader, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		stop := context.AfterFunc(ctx, func() { writer.CloseWithError(ctx.Err()) })
		defer stop()
		out := bufio.NewWriterSize(writer, 64<<10)
		var row [96]byte
		e := p.ForEachObject(ctx, func(id [20]byte, kind byte, size int64) error {
			if kind != 2 && kind != 3 && !(includeCommits && kind == 1) {
				return nil
			}
			b := row[:0]
			b = hex.AppendEncode(b, id[:])
			if kind == 1 {
				b = append(b, " commit "...)
			} else if kind == 2 {
				b = append(b, " tree "...)
			} else {
				b = append(b, " blob "...)
			}
			b = strconv.AppendInt(b, size, 10)
			b = append(b, '\n')
			_, e := out.Write(b)
			return e
		})
		if e == nil {
			e = out.Flush()
		}
		writer.CloseWithError(e)
	}()
	return s
}
func (s *packInventory) Close() error {
	s.once.Do(func() { s.cancel(); s.err = s.PipeReader.Close(); <-s.done })
	return s.err
}
