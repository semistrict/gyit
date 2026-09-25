// Package scratchmap stores fixed-width import lookup tables in disposable
// mapped files. It avoids a transaction and B-tree rewrite for every batch.
// Tables belong to one importer; they are neither published nor used by mounts.
package scratchmap

import (
	"bytes"
	"errors"
	"fmt"
	"hash/maphash"
	"os"

	"golang.org/x/sys/unix"
)

// Map is a temporary hash table. Mutations and Close require exclusive access;
// concurrent Lookups are safe once construction has finished. The OS can reclaim
// mapped pages under memory pressure. Lookup copies into caller-owned storage,
// so no pointer into a mapping survives growth or Close. No durability is promised.
type Map struct {
	dir                   string
	file                  *os.File
	data                  []byte
	keySize, valueSize    int
	slotSize, count, used int
	seed                  maphash.Seed
	closed                bool
}

func New(parent string, keySize, valueSize int) (*Map, error) {
	if keySize < 1 || keySize > 64 || valueSize < 1 || valueSize > 4096 {
		return nil, fmt.Errorf("invalid scratch map widths")
	}
	dir, err := os.MkdirTemp(parent, "lookup-*")
	if err != nil {
		return nil, err
	}
	m := &Map{dir: dir, keySize: keySize, valueSize: valueSize,
		slotSize: (1 + keySize + valueSize + 7) &^ 7, seed: maphash.MakeSeed()}
	if err := m.grow(1024); err != nil {
		_ = m.Close()
		return nil, err
	}
	return m, nil
}

func (m *Map) slot(data []byte, count int, key []byte) []byte {
	i := int(maphash.Bytes(m.seed, key) & uint64(count-1))
	for {
		entry := data[i*m.slotSize : (i+1)*m.slotSize]
		if entry[0] == 0 || bytes.Equal(entry[1:1+m.keySize], key) {
			return entry
		}
		i = (i + 1) & (count - 1)
	}
}

func (m *Map) grow(count int) error {
	if count <= 0 || count > int(^uint(0)>>1)/m.slotSize {
		return fmt.Errorf("scratch map exceeds addressable size")
	}
	file, err := os.CreateTemp(m.dir, "table-*")
	if err != nil {
		return err
	}
	length := count * m.slotSize
	err = file.Truncate(int64(length))
	var mapped []byte
	if err == nil {
		mapped, err = unix.Mmap(int(file.Fd()), 0, length, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	}
	if err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return err
	}
	for i := 0; i < len(m.data); i += m.slotSize {
		entry := m.data[i : i+m.slotSize]
		if entry[0] != 0 {
			copy(m.slot(mapped, count, entry[1:1+m.keySize]), entry)
		}
	}
	oldData, oldFile := m.data, m.file
	m.data, m.file, m.count = mapped, file, count
	if oldFile != nil {
		return errors.Join(unix.Munmap(oldData), oldFile.Close(), os.Remove(oldFile.Name()))
	}
	return nil
}

// Put copies exactly one key and value, replacing an existing value if present.
func (m *Map) Put(key, value []byte) error {
	if m.closed {
		return os.ErrClosed
	}
	if len(key) != m.keySize || len(value) != m.valueSize {
		return fmt.Errorf("scratch map key or value width mismatch")
	}
	entry := m.slot(m.data, m.count, key)
	if entry[0] != 0 {
		copy(entry[1+m.keySize:], value)
		return nil
	}
	if m.used+1 > m.count*3/4 {
		if m.count > int(^uint(0)>>1)/2 {
			return fmt.Errorf("scratch map exceeds addressable size")
		}
		if err := m.grow(m.count * 2); err != nil {
			return err
		}
		entry = m.slot(m.data, m.count, key)
	}
	copy(entry[1:], key)
	copy(entry[1+m.keySize:], value)
	entry[0] = 1
	m.used++
	return nil
}

// Lookup fills dst only when the key exists. Its width must equal valueSize.
func (m *Map) Lookup(key, dst []byte) (bool, error) {
	if m.closed {
		return false, os.ErrClosed
	}
	if len(key) != m.keySize || len(dst) != m.valueSize {
		return false, fmt.Errorf("scratch map key or output width mismatch")
	}
	entry := m.slot(m.data, m.count, key)
	if entry[0] == 0 {
		return false, nil
	}
	copy(dst, entry[1+m.keySize:1+m.keySize+m.valueSize])
	return true, nil
}

func (m *Map) Close() error {
	if m.closed {
		return nil
	}
	m.closed = true
	var err error
	if m.data != nil {
		err = unix.Munmap(m.data)
		m.data = nil
	}
	if m.file != nil {
		err = errors.Join(err, m.file.Close())
	}
	return errors.Join(err, os.RemoveAll(m.dir))
}
