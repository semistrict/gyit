package packrecipe

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	wirecodec "gyit/internal/packcodec"
	"gyit/internal/gitdelta"
	"io"
	"os"
	"sort"

	"golang.org/x/sys/unix"
)

// Experimental import-only reader. Never used for stored/mounted data.
const maxObject = 16 << 20

type object struct {
	kind byte
	data []byte
}
type cacheItem struct {
	offset int
	value  object
}
type frameData struct{ raw, packed []byte }
type packReader struct {
	work               sourceWorkCounters
	frames             bounded[frameData]
	native             bool
	idx, pack          []byte
	count              int
	z                  io.ReadCloser
	input              bytes.Reader
	cache              map[int]*list.Element
	lru                list.List
	used, budget       int
	decoded, hits, raw int64
}

func mapped(path string) ([]byte, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil {
		return nil, e
	}
	if st.Size() < 1 || int64(int(st.Size())) != st.Size() {
		return nil, fmt.Errorf("mapping size")
	}
	return unix.Mmap(int(f.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_PRIVATE)
}
func openPack(path string, budget int) (*packReader, error) {
	p := &packReader{frames: bounded[frameData]{limit: 16 << 20}, cache: make(map[int]*list.Element), budget: budget}
	var e error
	p.idx, e = mapped(path + ".idx")
	if e != nil {
		return nil, e
	}
	p.pack, e = mapped(path + ".pack")
	if e != nil {
		p.close()
		return nil, e
	}
	if len(p.idx) < 1072 || !bytes.Equal(p.idx[:8], []byte{255, 't', 'O', 'c', 0, 0, 0, 2}) || len(p.pack) < 32 || string(p.pack[:4]) != "PACK" {
		p.close()
		return nil, fmt.Errorf("format")
	}
	p.count = int(binary.BigEndian.Uint32(p.idx[1028:1032]))
	if p.count > (len(p.idx)-1072)/28 || int(binary.BigEndian.Uint32(p.pack[8:12])) != p.count || !bytes.Equal(p.pack[len(p.pack)-20:], p.idx[len(p.idx)-40:len(p.idx)-20]) {
		p.close()
		return nil, fmt.Errorf("index bounds/checksum identity")
	}
	return p, nil
}
func (p *packReader) close() {
	if p.z != nil {
		p.z.Close()
	}
	if p.idx != nil {
		unix.Munmap(p.idx)
	}
	if p.pack != nil {
		unix.Munmap(p.pack)
	}
}
func (p *packReader) offset(oid []byte) (int, error) {
	if len(oid) != 20 {
		return 0, fmt.Errorf("SHA1 only")
	}
	lo := 0
	if oid[0] > 0 {
		lo = int(binary.BigEndian.Uint32(p.idx[8+4*(int(oid[0])-1):]))
	}
	hi := int(binary.BigEndian.Uint32(p.idx[8+4*int(oid[0]):]))
	if lo > hi || hi > p.count {
		return 0, fmt.Errorf("fanout")
	}
	i := lo + sort.Search(hi-lo, func(i int) bool { return bytes.Compare(p.idx[1032+20*(lo+i):1032+20*(lo+i+1)], oid) >= 0 })
	if i == hi || !bytes.Equal(p.idx[1032+20*i:1032+20*(i+1)], oid) {
		return 0, os.ErrNotExist
	}
	off := uint64(binary.BigEndian.Uint32(p.idx[1032+24*p.count+4*i:]))
	if off&0x80000000 != 0 {
		pos := uint64(1032+28*p.count) + 8*(off&0x7fffffff)
		if pos+8 > uint64(len(p.idx)-40) {
			return 0, fmt.Errorf("offset table")
		}
		off = binary.BigEndian.Uint64(p.idx[pos:])
	}
	if off < 12 || off >= uint64(len(p.pack)-20) {
		return 0, fmt.Errorf("offset bounds")
	}
	return int(off), nil
}

type frame struct {
	offset, body, size int
	kind               byte
}

func (p *packReader) header(off int) (frame, int, error) {
	f := frame{offset: off}
	end := len(p.pack) - 20
	if off < 12 || off >= end {
		return f, 0, fmt.Errorf("header bounds")
	}
	b := p.pack[off]
	off++
	f.kind = (b >> 4) & 7
	size := uint64(b & 15)
	for shift := uint(4); b&128 != 0; shift += 7 {
		if off >= end || shift > 60 {
			return f, 0, fmt.Errorf("size overflow")
		}
		b = p.pack[off]
		off++
		size |= uint64(b&127) << shift
	}
	if size > maxObject {
		return f, 0, fmt.Errorf("object limit")
	}
	f.size = int(size)
	base := 0
	if f.kind == 6 {
		if off >= end {
			return f, 0, io.ErrUnexpectedEOF
		}
		b = p.pack[off]
		off++
		dist := uint64(b & 127)
		for b&128 != 0 {
			if off >= end || dist > uint64(f.offset)>>7 {
				return f, 0, fmt.Errorf("base offset")
			}
			b = p.pack[off]
			off++
			dist = ((dist + 1) << 7) | uint64(b&127)
		}
		if dist == 0 || dist > uint64(f.offset-12) {
			return f, 0, fmt.Errorf("base offset")
		}
		base = f.offset - int(dist)
	} else if f.kind == 7 {
		if off+20 > end {
			return f, 0, io.ErrUnexpectedEOF
		}
		var e error
		base, e = p.offset(p.pack[off : off+20])
		if e != nil {
			return f, 0, e
		}
		off += 20
	} else if f.kind < 1 || f.kind > 4 {
		return f, 0, fmt.Errorf("object type")
	}
	f.body = off
	return f, base, nil
}
func (p *packReader) frameData(f frame) (frameData, error) {
	if d, ok := p.frames.get(f.offset); ok {
		return d, nil
	}
	if f.size > 2<<20 {
		return frameData{}, gitdelta.ErrLimit
	}
	raw := make([]byte, f.size)
	p.work.FullInflations++
	n, e := wirecodec.InflatePrefix(raw, p.pack[f.body:len(p.pack)-20])
	if e != nil {
		return frameData{}, e
	}
	p.work.FullInflatedBytes += int64(len(raw))
	d := frameData{raw, p.pack[f.body : f.body+n]}
	p.frames.put(f.offset, d, len(raw)+128)
	return d, nil
}
func (p *packReader) inflate(f frame) ([]byte, error) { d, e := p.frameData(f); return d.raw, e }
func (p *packReader) rawFrame(f frame) ([]byte, []byte, error) {
	d, e := p.frameData(f)
	return d.raw, d.packed, e
}

func number(data []byte, pos *int) (uint64, error) {
	var n uint64
	for shift := uint(0); shift < 63; shift += 7 {
		if *pos >= len(data) {
			return 0, io.ErrUnexpectedEOF
		}
		b := data[*pos]
		*pos++
		n |= uint64(b&127) << shift
		if b&128 == 0 {
			return n, nil
		}
	}
	return 0, fmt.Errorf("varint overflow")
}
func apply(base, program []byte) ([]byte, error) {
	pos := 0
	n, e := number(program, &pos)
	if e != nil || n != uint64(len(base)) {
		return nil, fmt.Errorf("delta base size")
	}
	n, e = number(program, &pos)
	if e != nil || n > 1<<20 {
		return nil, gitdelta.ErrLimit
	}
	out := make([]byte, 0, int(n))
	for pos < len(program) {
		op := program[pos]
		pos++
		if op == 0 {
			return nil, fmt.Errorf("zero opcode")
		}
		if op&128 == 0 {
			size := int(op)
			if pos+size > len(program) || size > cap(out)-len(out) {
				return nil, fmt.Errorf("literal bounds")
			}
			out = append(out, program[pos:pos+size]...)
			pos += size
			continue
		}
		var off, size uint64
		for j := uint(0); j < 7; j++ {
			if op&(1<<j) == 0 {
				continue
			}
			if pos >= len(program) {
				return nil, io.ErrUnexpectedEOF
			}
			if j < 4 {
				off |= uint64(program[pos]) << (8 * j)
			} else {
				size |= uint64(program[pos]) << (8 * (j - 4))
			}
			pos++
		}
		if size == 0 {
			size = 65536
		}
		if off+size > uint64(len(base)) || size > uint64(cap(out)-len(out)) {
			return nil, fmt.Errorf("copy bounds")
		}
		out = append(out, base[off:off+size]...)
	}
	if len(out) != cap(out) {
		return nil, fmt.Errorf("result size")
	}
	return out, nil
}
func (p *packReader) remember(off int, v object) {
	if len(v.data) > p.budget || p.budget == 0 {
		return
	}
	if _, ok := p.cache[off]; ok {
		return
	}
	for p.used+len(v.data) > p.budget || len(p.cache) >= 32768 {
		e := p.lru.Back()
		c := e.Value.(cacheItem)
		delete(p.cache, c.offset)
		p.used -= len(c.value.data)
		p.lru.Remove(e)
	}
	p.cache[off] = p.lru.PushFront(cacheItem{off, v})
	p.used += len(v.data)
}
func (p *packReader) get(oid string) (object, error) {
	p.work.GetCalls++
	raw, e := hex.DecodeString(oid)
	if e != nil {
		return object{}, e
	}
	off, e := p.offset(raw)
	if e != nil {
		return object{}, e
	}
	var chain [64]frame
	n := 0
	var result object
	for {
		if c := p.cache[off]; c != nil {
			p.hits++
			p.lru.MoveToFront(c)
			result = c.Value.(cacheItem).value
			break
		}
		if n == len(chain) {
			return object{}, fmt.Errorf("chain limit")
		}
		f, base, e := p.header(off)
		if e != nil {
			return object{}, e
		}
		chain[n] = f
		n++
		if base == 0 {
			body, e := p.inflate(f)
			if e != nil {
				return object{}, e
			}
			result = object{f.kind, body}
			p.remember(f.offset, result)
			n--
			break
		}
		off = base
	}
	for n > 0 {
		n--
		f := chain[n]
		program, e := p.inflate(f)
		if e != nil {
			return object{}, e
		}
		body, e := apply(result.data, program)
		if e != nil {
			return object{}, e
		}
		p.work.Reconstructions++
		p.work.ReconstructedBytes += int64(len(body))
		result = object{result.kind, body}
		p.remember(f.offset, result)
	}
	return result, nil
}
