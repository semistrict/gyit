//go:build !js

package repo

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/spill"
	"gyit/internal/store"
	"io"
	"os"
	"path/filepath"
	"runtime/trace"
	"strings"
)

// ImportPacks copies only new packs and incrementally updates the object index.
// Metadata comes from pack headers (delta result lengths from their prefixes),
// not a full reconstruction/verification pass over historical file bodies.
func (p *Progressive) ImportPacks(ctx context.Context, gitdir string) error {
	records, err := spill.New(p.temp, 1<<20)
	if err != nil {
		return err
	}
	defer records.Close()
	changed, err := p.stagePackRecords(ctx, gitdir, records.Add)
	if err != nil || !changed {
		return err
	}
	metadata := newPublicationUploads(ctx, p.store)
	defer metadata.close()
	p.writer.Lock()
	defer p.writer.Unlock()
	return p.publishIndex(ctx, metadata, func(idx *index) (pageRef, error) { return idx.updateSorted(ctx, records) })
}

// Stream recipes directly to bounded staging, without constructing a second
// temporary database. Successful return joins all immutable uploads; the caller
// can then publish these records atomically, optionally with prepared history.
func (p *Progressive) stagePackRecords(ctx context.Context, gitdir string, add func([]byte, []byte) error) (bool, error) {
	region := trace.StartRegion(ctx, "pack-stage-records")
	defer region.End()
	files, err := filepath.Glob(filepath.Join(gitdir, "objects", "pack", "pack-*.idx"))
	if err != nil {
		return false, err
	}
	uploads := newPublicationUploads(ctx, p.store)
	defer uploads.close()
	changed := false
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		id := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(file), "pack-"), ".idx")
		if !validProgressiveOID(id) {
			return false, fmt.Errorf("invalid pack identity")
		}
		var known pb.ProgressiveObject
		if err := p.get(ctx, "pack/"+id, &known); err == nil {
			continue
		} else if !errors.Is(err, store.ErrNotFound) {
			return false, err
		}
		trace.WithRegion(ctx, "pack-scan-recipes", func() { err = p.importPack(ctx, uploads, file, id, add) })
		if err != nil {
			return false, err
		}
		changed = true
	}
	trace.WithRegion(ctx, "pack-upload-wait", func() { err = uploads.wait() })
	return changed, err
}
func (p *Progressive) importPack(ctx context.Context, backend store.Store, indexPath, id string, add func([]byte, []byte) error) error {
	put := func(key string, value *pb.ProgressiveObject) error {
		raw, err := proto.Marshal(value)
		if err != nil {
			return err
		}
		return add([]byte(key), raw)
	}
	idx, err := os.ReadFile(indexPath)
	if err != nil {
		return err
	}
	if len(idx) < 1072 || !bytes.Equal(idx[:8], []byte{255, 't', 'O', 'c', 0, 0, 0, 2}) {
		return fmt.Errorf("unsupported pack index")
	}
	count := int(binary.BigEndian.Uint32(idx[1028:1032]))
	if count > (len(idx)-1072)/28 {
		return fmt.Errorf("pack index bounds")
	}
	file, err := os.Open(strings.TrimSuffix(indexPath, ".idx") + ".pack")
	if err != nil {
		return err
	}
	defer file.Close()
	st, err := file.Stat()
	if err != nil {
		return err
	}
	if st.Size() < 32 || st.Size() > int64(^uint(0)>>1) {
		return fmt.Errorf("pack size bounds")
	}
	data, err := unix.Mmap(int(file.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_PRIVATE)
	if err != nil {
		return err
	}
	defer unix.Munmap(data)
	if string(data[:4]) != "PACK" || binary.BigEndian.Uint32(data[4:8]) != 2 || int(binary.BigEndian.Uint32(data[8:12])) != count || hex.EncodeToString(data[len(data)-20:]) != id {
		return fmt.Errorf("pack header mismatch")
	}
	for off := int64(0); off < st.Size(); off += progressiveSegment {
		if err = backend.Put(ctx, progressivePackKey(id, off/progressiveSegment), data[off:min(st.Size(), off+progressiveSegment)], ""); err != nil {
			return err
		}
	}
	var inflater io.ReadCloser
	defer func() {
		if inflater != nil {
			inflater.Close()
		}
	}()
	for i := 0; i < count; i++ {
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		off := uint64(binary.BigEndian.Uint32(idx[1032+24*count+4*i:]))
		if off&0x80000000 != 0 {
			pos := uint64(1032+28*count) + 8*(off&0x7fffffff)
			if pos+8 > uint64(len(idx)-40) {
				return fmt.Errorf("large offset bounds")
			}
			off = binary.BigEndian.Uint64(idx[pos:])
		}
		if off < 12 || off >= uint64(len(data)-20) {
			return fmt.Errorf("pack offset bounds")
		}
		in := bytes.NewReader(data[off : len(data)-20])
		kind, size, _, _, err := progressiveHeader(in, int64(off))
		if err != nil {
			return err
		}
		if kind == 6 || kind == 7 {
			if inflater == nil {
				inflater, err = zlib.NewReader(in)
			} else {
				err = inflater.(zlib.Resetter).Reset(in, nil)
			}
			if err != nil {
				return err
			}
			br := bufio.NewReaderSize(inflater, 32)
			if _, err = binary.ReadUvarint(br); err != nil {
				return err
			}
			n, e := binary.ReadUvarint(br)
			if e != nil || n > 1<<62 {
				return fmt.Errorf("delta result size")
			}
			size = int64(n)
		}
		oid := hex.EncodeToString(idx[1032+20*i : 1032+20*(i+1)])
		// Small foreground history packs are already mapped here. Cache only
		// independently encoded, bounded commits; never retain compressed data.
		// Cache admission is optional and cannot turn an import into verification.
		if st.Size() <= 1<<20 && kind == 1 && size <= 64<<10 {
			p.cacheCommit(ctx, oid, in, size)
		}
		if err = put("g/"+oid, &pb.ProgressiveObject{Pack: id, PackSize: st.Size(), Offset: int64(off), Size: size}); err != nil {
			return err
		}
	}
	return put("pack/"+id, &pb.ProgressiveObject{Pack: id, PackSize: st.Size()})
}

func (p *Progressive) cacheCommit(ctx context.Context, oid string, in io.Reader, size int64) {
	if ctx.Err() != nil || (p.cache.disk == nil && p.cache.max == 0) {
		return
	}
	z, err := zlib.NewReader(in)
	if err != nil {
		return
	}
	raw, err := io.ReadAll(io.LimitReader(z, size+1))
	z.Close()
	if err != nil || int64(len(raw)) != size {
		return
	}
	h := sha1.New()
	fmt.Fprintf(h, "commit %d%c", len(raw), 0)
	h.Write(raw)
	if hex.EncodeToString(h.Sum(nil)) != oid {
		return
	}
	p.cache.put("progressive-object/"+oid, append([]byte{1}, raw...))
}
