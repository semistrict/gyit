package repo

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gat/internal/spill"
)

// Path names from rev-list are hints only, never authoritative tree metadata.
// A temporary disk-backed sort places related blobs together, without retaining
// an O(history) candidate map in memory or adding an external sort dependency.
func orderObjectHints(ctx context.Context, ids *os.File, tmp, source string) error {
	return prepareObjectHints(ctx, ids, tmp, source, true, 1)
}

func prepareObjectHints(ctx context.Context, ids *os.File, tmp, source string, reorder bool, workers int) error {
	sizes, err := prepareImportObjects(ctx, ids, tmp, source, reorder, workers, nil)
	if err != nil {
		return err
	}
	return sizes.Close()
}

// With parentSink, commit rows come from rev-list --objects --parents. The
// batch-check type separates parent lists from arbitrary object path hints.
func prepareImportObjects(ctx context.Context, ids *os.File, tmp, source string, reorder bool, workers int, parentSink func(string, []string) error) (_ *blobSizes, retErr error) {
	// Git supplies sizes without decompressing blob contents. Keep this pass
	// disk-backed, just like reachability enumeration and the subsequent sort.
	info, err := objectMetadata(ctx, ids, tmp, source)
	if err != nil {
		return nil, err
	}
	return prepareImportMetadata(ctx, ids, tmp, reorder, workers, parentSink, info, nil, nil)
}
func prepareImportMetadata(ctx context.Context, ids *os.File, tmp string, reorder bool, workers int, parentSink func(string, []string) error, info io.ReadCloser, preloaded *blobSizes, early func(string, string) error) (_ *blobSizes, retErr error) {
	return prepareImportMetadataWithNative(ctx, ids, tmp, reorder, workers, parentSink, info, preloaded, early, nil, nil)
}

func prepareImportMetadataWithNative(ctx context.Context, ids *os.File, tmp string, reorder bool, workers int, parentSink func(string, []string) error, info io.ReadCloser, preloaded *blobSizes, early func(string, string) error, native func(string, string, int64) (bool, error), replay func(func(string, string, int64) error) error) (_ *blobSizes, retErr error) {
	defer info.Close()
	if native != nil && (preloaded == nil || !reorder || replay == nil) {
		return nil, fmt.Errorf("streaming native requires immutable sizes and ordered fallback replay")
	}
	ordered, err := spill.New(tmp, 32<<20)
	if err != nil {
		return nil, err
	}
	defer ordered.Close()

	sizes := preloaded
	if sizes == nil {
		sizes = &blobSizes{parent: tmp}
	}
	defer func() {
		if retErr != nil && preloaded == nil {
			sizes.Close()
		}
	}()
	if preloaded != nil && reorder && workers > 1 {
		sizes.laneBytes = make([]uint64, workers)
	}
	addOrdered := func(oid, hint, kind string, size uint64) error {
		objectID, err := hex.DecodeString(oid)
		if err != nil {
			return err
		}
		worker := chunkWorker(hint+"/0000000000000000", workers)
		if kind == "blob" {
			sizes.noteBlobWork(hint, size, worker)
		}
		digest := sha256.Sum256([]byte(hint))
		key := binary.BigEndian.AppendUint64(nil, uint64(worker))
		key = append(key, digest[:]...)
		key = binary.BigEndian.AppendUint64(key, ^size)
		key = append(key, objectID...)
		return ordered.Add(key, []byte(oid+" "+hint))
	}
	scan := bufio.NewScanner(info)
	scan.Buffer(make([]byte, 64<<10), 1<<20)
	var scanned int64
	for scan.Scan() {
		scanned++
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fields := strings.SplitN(scan.Text(), " ", 4)
		if len(fields) != 4 {
			return nil, fmt.Errorf("invalid object size response")
		}
		size, err := strconv.ParseUint(fields[2], 10, 64)
		if err != nil {
			return nil, err
		}
		oid, hint := fields[0], fields[3]
		objectID, err := hex.DecodeString(oid)
		if err != nil {
			return nil, err
		}
		if fields[1] == "blob" {
			if preloaded != nil {
				sizes.records++
			} else if err := sizes.put(objectID, size); err != nil {
				return nil, err
			}
		}
		if fields[1] == "commit" && parentSink != nil {
			parentIDs := strings.Fields(hint)
			for _, parent := range parentIDs {
				decoded, err := hex.DecodeString(parent)
				if err != nil || len(decoded) != len(objectID) {
					return nil, fmt.Errorf("invalid enumerated parent ID")
				}
			}
			if err := parentSink(oid, parentIDs); err != nil {
				return nil, err
			}
			// Parent IDs are metadata, not paths. Keep the original empty
			// commit hint so ordering and worker interleaving do not change.
			hint = ""
		}
		if native != nil && fields[1] == "blob" {
			if size > uint64(^uint64(0)>>1) {
				return nil, fmt.Errorf("invalid blob size")
			}
			owned, err := native(oid, hint, int64(size))
			if err != nil {
				return nil, err
			}
			if owned {
				// The immutable size map retains this identity for tree conversion.
				// Count only fallback work for the late codec, restoring rejections below.
				sizes.records--
				continue
			}
		}
		if early != nil && fields[1] != "blob" {
			if err := early(oid, fields[1]); err != nil {
				return nil, err
			}
			continue
		}
		if !reorder {
			continue
		}
		// Preserve the existing worker/path/size/OID ordering for every fallback.
		if err := addOrdered(oid, hint, fields[1], size); err != nil {
			return nil, err
		}
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	streamingTrace("typed_scan_eof", scanned)
	if replay != nil {
		if err := replay(func(oid, hint string, size int64) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if size < 0 {
				return fmt.Errorf("invalid rejected blob size")
			}
			sizes.records++
			return addOrdered(oid, hint, "blob", uint64(size))
		}); err != nil {
			return nil, err
		}
	}
	if !reorder {
		if _, err := ids.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
	} else if err := interleaveOrderedHints(ctx, ordered, ids, tmp, workers); err != nil {
		return nil, err
	}
	return sizes, nil
}

func interleaveOrderedHints(ctx context.Context, ordered *spill.Sorter, ids *os.File, tmp string, workers int) error {
	dir, err := os.MkdirTemp(tmp, "order-lanes-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	lanePath := func(worker int) string { return filepath.Join(dir, fmt.Sprintf("%08x", worker)) }
	var file *os.File
	var output *bufio.Writer
	lane := -1
	closeLane := func() error {
		if file == nil {
			return nil
		}
		err := output.Flush()
		closeErr := file.Close()
		file = nil
		if err != nil {
			return err
		}
		return closeErr
	}
	defer closeLane()
	if err := ordered.Walk(ctx, func(key, value []byte) error {
		if len(key) < 8 || binary.BigEndian.Uint64(key[:8]) >= uint64(workers) {
			return fmt.Errorf("invalid sorted worker lane")
		}
		worker := int(binary.BigEndian.Uint64(key[:8]))
		if worker != lane {
			if err := closeLane(); err != nil {
				return err
			}
			file, err = os.Create(lanePath(worker))
			if err != nil {
				return err
			}
			output, lane = bufio.NewWriterSize(file, 64<<10), worker
		}
		if _, err := output.Write(value); err != nil {
			return err
		}
		return output.WriteByte('\n')
	}); err != nil {
		return err
	}
	if err := closeLane(); err != nil {
		return err
	}
	if err := ids.Truncate(0); err != nil {
		return err
	}
	if _, err := ids.Seek(0, io.SeekStart); err != nil {
		return err
	}
	inputs := make([]*bufio.Reader, workers)
	for i := range inputs {
		file, err := os.Open(lanePath(i))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		defer file.Close()
		inputs[i] = bufio.NewReaderSize(file, 64<<10)
	}
	w := bufio.NewWriterSize(ids, 64<<10)
	for {
		active := false
		for i, r := range inputs {
			if err := ctx.Err(); err != nil {
				return err
			}
			if r == nil {
				continue
			}
			line, err := r.ReadString('\n')
			if err == io.EOF && len(line) == 0 {
				inputs[i] = nil
				continue
			}
			if err != nil {
				return err
			}
			active = true
			if _, err := w.WriteString(line); err != nil {
				return err
			}
		}
		if !active {
			break
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	_, err = ids.Seek(0, io.SeekStart)
	return err
}
