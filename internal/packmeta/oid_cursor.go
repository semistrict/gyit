package planner

import (
	"bytes"
	"context"
	"fmt"
	"sync"
)

// SourceObject is an owned metadata row. Ordinal is the zero-based slot in the
// source's lexicographically sorted OID index, not its physical pack position.
// Kind and Size have the same deferred-authentication contract as Lookup.
type SourceObject struct {
	OID     [20]byte
	Kind    byte
	Size    int64
	Ordinal uint32
}

// OIDCursor retains only a source pointer, context, lock, and next ordinal.
// It owns no mappings, buffers, goroutines, or files and needs no Close call.
// Planner.Close remains authoritative and makes subsequent Next calls fail.
type OIDCursor struct {
	mu      sync.Mutex
	planner *Planner
	ctx     context.Context
	next    uint32
}

func (p *Planner) NewOIDCursor(ctx context.Context) (*OIDCursor, error) {
	if ctx == nil {
		return nil, fmt.Errorf("nil object cursor context")
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := p.ctx.Err(); err != nil {
		return nil, err
	}
	return &OIDCursor{planner: p, ctx: ctx}, nil
}

// Next copies one row and releases every source lock before returning. Calling
// Recipe, Lookup, or Close between steps cannot reenter a held lifecycle lock.
// Concurrent Next calls are serialized; each successful row is returned once.
// Source/cursor cancellation and source closure are reported even after EOF.
func (c *OIDCursor) Next() (SourceObject, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.planner
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return SourceObject{}, false, ErrClosed
	}
	if err := c.ctx.Err(); err != nil {
		return SourceObject{}, false, err
	}
	if err := p.ctx.Err(); err != nil {
		return SourceObject{}, false, err
	}
	count := uint32(len(p.records))
	if c.next == count {
		return SourceObject{}, false, nil
	}
	row := SourceObject{Ordinal: c.next}
	pos := uint64(1032) + 20*uint64(c.next)
	copy(row.OID[:], p.idx[pos:pos+20])
	offset, err := p.indexOffset(c.next, count)
	if err != nil {
		return SourceObject{}, false, err
	}
	physical, found, _ := p.atOffset(offset)
	if !found || !bytes.Equal(row.OID[:], p.oid(physical)) {
		return SourceObject{}, false, fmt.Errorf("%w: cursor index/physical identity mismatch", ErrMalformed)
	}
	record := p.records[physical]
	row.Kind, row.Size = record.finalKind(), int64(record.size)
	c.next++
	return row, true, nil
}
