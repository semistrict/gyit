package scratchmap_test

import (
	"encoding/binary"
	"os"
	"testing"

	"gat/internal/scratchmap"
)

func TestGrowthUpdatesAndOwnedInputs(t *testing.T) {
	parent := t.TempDir()
	m, err := scratchmap.New(parent, 4, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	var key [4]byte
	var value [8]byte
	for i := 0; i < 10000; i++ {
		id := i * 8191 % 10000
		binary.BigEndian.PutUint32(key[:], uint32(id))
		binary.LittleEndian.PutUint64(value[:], uint64(id*11))
		if err := m.Put(key[:], value[:]); err != nil {
			t.Fatal(err)
		}
	}
	clear(key[:])
	clear(value[:])
	if err := m.Put(key[:], value[:]); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		binary.BigEndian.PutUint32(key[:], uint32(i))
		found, err := m.Lookup(key[:], value[:])
		if err != nil || !found || binary.LittleEndian.Uint64(value[:]) != uint64(i*11) {
			t.Fatalf("key %d: found=%v value=%d err=%v", i, found, binary.LittleEndian.Uint64(value[:]), err)
		}
		clear(value[:]) // The result belongs to the caller.
	}
	binary.BigEndian.PutUint32(key[:], 10000)
	if found, err := m.Lookup(key[:], value[:]); err != nil || found {
		t.Fatalf("missing key: %v %v", found, err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(parent)
	if err != nil || len(files) != 0 {
		t.Fatalf("scratch files remain: %v %v", files, err)
	}
	if err := m.Put(key[:], value[:]); err == nil {
		t.Fatal("accepted write after close")
	}
}

func TestUpdatesDoNotLoseOtherKeysAndWidthsAreValidated(t *testing.T) {
	m, err := scratchmap.New(t.TempDir(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for i := 0; i < 256; i++ {
		if err := m.Put([]byte{byte(i)}, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 256; i++ {
		if err := m.Put([]byte{byte(i)}, []byte{byte(255 - i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 256; i++ {
		var dst [1]byte
		if found, err := m.Lookup([]byte{byte(i)}, dst[:]); err != nil || !found || dst[0] != byte(255-i) {
			t.Fatalf("updated key %d: %v %v %v", i, found, dst, err)
		}
	}
	if err := m.Put(nil, []byte{0}); err == nil {
		t.Fatal("accepted wrong key width")
	}
	if err := m.Put([]byte{0}, nil); err == nil {
		t.Fatal("accepted wrong value width")
	}
	if _, err := m.Lookup([]byte{0}, nil); err == nil {
		t.Fatal("accepted wrong output width")
	}
}
