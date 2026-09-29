//go:build !js

package repo

import (
	"context"
	"encoding/binary"
	"fmt"

	"gyit/internal/spill"
)

const historyChangeMemory = 1 << 20

// Small commits avoid a temporary file. Large commits spill path/parent pairs;
// recording the parent in the key preserves OR semantics across spill runs.
type historyChanges struct {
	temp          string
	parents, used int
	paths         map[string][]byte
	sorted        *spill.Sorter
}

func newHistoryChanges(temp string, parents int) *historyChanges {
	return &historyChanges{temp: temp, parents: parents, paths: make(map[string][]byte)}
}

func (c *historyChanges) add(path string, parent int) error {
	mask := c.paths[path]
	if mask == nil {
		cost := len(path) + (c.parents+7)/8 + 64
		if cost > historyChangeMemory {
			return fmt.Errorf("history change exceeds staging limit")
		}
		if c.used+cost > historyChangeMemory {
			if err := c.flush(); err != nil {
				return err
			}
		}
		mask = make([]byte, (c.parents+7)/8)
		c.paths[path] = mask
		c.used += cost
	}
	mask[parent/8] |= 1 << uint(parent%8)
	return nil
}

func (c *historyChanges) flush() error {
	if c.sorted == nil {
		var err error
		c.sorted, err = spill.New(c.temp, historyChangeMemory)
		if err != nil {
			return err
		}
	}
	for path, mask := range c.paths {
		key := make([]byte, len(path)+5)
		copy(key, path)
		for parent := range c.parents {
			if mask[parent/8]&(1<<uint(parent%8)) == 0 {
				continue
			}
			binary.BigEndian.PutUint32(key[len(path)+1:], uint32(parent))
			if err := c.sorted.Add(key, nil); err != nil {
				return err
			}
		}
	}
	clear(c.paths)
	c.used = 0
	return nil
}

func (c *historyChanges) walk(ctx context.Context, emit func(string, []byte) error) error {
	if c.sorted == nil {
		for path, mask := range c.paths {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := emit(path, mask); err != nil {
				return err
			}
		}
		return nil
	}
	if err := c.flush(); err != nil {
		return err
	}
	var path string
	var mask []byte
	err := c.sorted.Walk(ctx, func(key, _ []byte) error {
		name := string(key[:len(key)-5])
		if mask == nil || name != path {
			if mask != nil {
				if err := emit(path, mask); err != nil {
					return err
				}
			}
			path = name
			mask = make([]byte, (c.parents+7)/8)
		}
		parent := binary.BigEndian.Uint32(key[len(key)-4:])
		mask[parent/8] |= 1 << uint(parent%8)
		return nil
	})
	if err != nil {
		return err
	}
	if mask != nil {
		return emit(path, mask)
	}
	return nil
}

func (c *historyChanges) close() {
	if c.sorted != nil {
		_ = c.sorted.Close()
	}
	c.paths = nil
}
