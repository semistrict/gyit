package repo

import (
	"context"
	"errors"
	"fmt"
	"gyit/internal/store"
	"io"
	"os"
	"strconv"
	"strings"
)

var errMetadataInventoryUnavailable = errors.New("complete source inventory unavailable")

func prepareEarly(ctx context.Context, cancel context.CancelFunc, source, format, input, tmp string, ids *os.File, workers, oidBytes int, st *stage, backend store.Store, beforeConvert func()) (sizes *blobSizes, early *earlyMetadata, native *earlyNative, retErr error) {
	wholeDatabase := archiveImportEnabled(ctx)
	allLocal := archiveImportEnabled(ctx)
	var walk io.ReadCloser
	var e error
	ownedWalk := false
	if allLocal {
		streamingTrace("whole_database_object_walk_selections", 0)
		streamingTrace("all_local_commit_walk_selections", 0)
	} else {
		args := []string{"rev-list", "--objects", "--parents", "--stdin"}
		if wholeDatabase {
			args = []string{"rev-list", "--parents", "--stdin"}
			streamingTrace("whole_database_object_walk_selections", 0)
		}
		walk, e = streamGitOutput(ctx, tmp, source, input, args...)
		if e != nil {
			return nil, nil, nil, e
		}
		ownedWalk = true
	}
	defer func() {
		if ownedWalk {
			walk.Close()
		}
	}()
	info, e := loadSourceInfo(ctx, tmp, source, oidBytes)
	if e != nil {
		if archiveImportEnabled(ctx) {
			return nil, nil, nil, e
		}
		// An unreadable unreachable object must not make the reachable import fail.
		// No conversion or foreground staging has started yet, so retrying the
		// normal reachable-only preparation is safe after this child is joined.
		if ctx.Err() != nil {
			return nil, nil, nil, ctx.Err()
		}
		return nil, nil, nil, fmt.Errorf("%w: %w", errMetadataInventoryUnavailable, e)
	}
	ownedInfo := true
	defer func() {
		if ownedInfo {
			info.close()
		}
	}()
	if wholeDatabase && info.metadata == nil {
		return nil, nil, nil, fmt.Errorf("pack database selection requires supported native metadata")
	}
	var globalBytes int64
	if archiveImportEnabled(ctx) {
		globalBytes, e = writeGlobalSizes(ctx, info, st, backend)
		if e != nil {
			return nil, nil, nil, e
		}
	}
	if archiveImportEnabled(ctx) {
		info.archive, e = startSourceArchive(ctx, source, tmp, info, backend)
		if e != nil {
			return nil, nil, nil, e
		}
	}
	var orderedInventory io.ReadCloser
	if archiveImportEnabled(ctx) {
		orderedInventory, e = newOrderedArchiveInventory(ctx, tmp, info, backend)
		if e != nil {
			return nil, nil, nil, e
		}
	}
	sizes = &blobSizes{parent: tmp, sourceInfo: info}
	beforeConvert()
	early, e = startEarlyMetadata(ctx, cancel, source, format, tmp, min(workers, 8), sizes, st, backend)
	if e != nil {
		return nil, nil, nil, e
	}
	early.globalTableBytes = globalBytes
	workerOwner := early
	defer func() {
		if retErr != nil {
			cancel()
			if e := workerOwner.finish(nil); e != nil && !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
				retErr = e
			}
		}
	}()

	native, e = startArchiveNative(ctx, cancel, source, format, tmp, workers, st, backend, info.archive)
	if e != nil {
		return nil, nil, nil, e
	}
	nativeOwner := native
	defer func() {
		if retErr != nil {
			cancel()
			if e := nativeOwner.finish(nil); e != nil && !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
				retErr = e
			}
		}
	}()
	var typed io.ReadCloser
	var selection *databaseSelection
	if allLocal {
		inventory := orderedInventory
		if inventory == nil {
			inventory = newPackInventory(ctx, info.metadata)
		}
		selection = newAllLocalSelection(inventory, info, oidBytes)
		typed = selection
	} else if wholeDatabase {
		selection = newDatabaseSelection(walk, newPackInventory(ctx, info.metadata), info, oidBytes)
		typed = selection
	} else {
		typed = newTypedWalk(walk, info, oidBytes)
	}
	ownedWalk = false
	var parentSink func(string, []string) error
	if !allLocal {
		parentSink = func(id string, p []string) error { return st.put("p/"+id, parents{Parents: p}) }
	}
	result, e := prepareImportMetadataWithNative(ctx, ids, tmp, true, workers, parentSink, typed, sizes, early.add, native.tryAdd, func(emit func(string, string, int64) error) error {
		if e := early.seal(); e != nil {
			return e
		}
		return native.replayRejected(emit)
	})
	if e != nil {
		return nil, nil, nil, e
	}
	if e = early.seal(); e != nil {
		return nil, nil, nil, e
	}
	if selection != nil {
		if info.ordered != nil {
			if e = info.ordered.finish(nil); e != nil {
				return nil, nil, nil, e
			}
			selection.count.Trees += info.ordered.trees.admitted
			selection.count.Blobs += info.ordered.blobs.admitted
		}
		selection.traceCounts()
	}
	ownedInfo = false
	return result, early, native, nil
}

// Count only the local object database. A restricted revision or alternate
// may reach very few objects from a much larger database; the caller retains
// the reachable-only preparation path for those imports.
func sourceSupportsMetadataPreload(ctx context.Context, source string, threshold int64) bool {
	output, e := git(ctx, source, "count-objects", "-v").Output()
	if e != nil {
		return false
	}
	var count, packed int64
	haveCount, havePacked := false, false
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(line, "alternate:") {
			return false
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		switch fields[0] {
		case "count:":
			count, e = strconv.ParseInt(fields[1], 10, 64)
			haveCount = e == nil && count >= 0
		case "in-pack:":
			packed, e = strconv.ParseInt(fields[1], 10, 64)
			havePacked = e == nil && packed >= 0
		}
	}
	return haveCount && havePacked && (count >= threshold || packed >= threshold-count)
}
