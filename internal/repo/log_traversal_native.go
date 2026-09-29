//go:build !js

package repo

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
)

// The mapped B-tree is disposable query state, outside the repository store.
// Short transactions bound dirty pages; no fsync or durability is needed.
type diskLogTraversal struct {
	db        *bolt.DB
	tx        *bolt.Tx
	name      string
	mutations int
}

func newLogTraversalDisk(parent string) (_ logTraversalDisk, resultErr error) {
	file, err := os.CreateTemp(parent, "history-walk-*.db")
	if err != nil {
		return nil, err
	}
	d := &diskLogTraversal{name: file.Name()}
	if err := file.Close(); err != nil {
		_ = os.Remove(d.name)
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			_ = d.close()
		}
	}()
	d.db, err = bolt.Open(d.name, 0600, &bolt.Options{NoSync: true, NoFreelistSync: true, FreelistType: bolt.FreelistMapType})
	if err != nil {
		return nil, err
	}
	d.tx, err = d.db.Begin(true)
	if err != nil {
		return nil, err
	}
	for _, name := range []string{"seen", "queue"} {
		if _, err := d.tx.CreateBucket([]byte(name)); err != nil {
			return nil, err
		}
	}
	return d, nil
}
func (d *diskLogTraversal) checkpoint() error {
	d.mutations++
	if d.mutations < 256 {
		return nil
	}
	if err := d.tx.Commit(); err != nil {
		return err
	}
	d.tx = nil
	var err error
	d.tx, err = d.db.Begin(true)
	d.mutations = 0
	return err
}
func (d *diskLogTraversal) has(key string) (bool, error) {
	return d.tx.Bucket([]byte("seen")).Get([]byte(key)) != nil, nil
}
func (d *diskLogTraversal) remember(key string) error {
	if err := d.tx.Bucket([]byte("seen")).Put([]byte(key), []byte{1}); err != nil {
		return err
	}
	return d.checkpoint()
}
func (d *diskLogTraversal) clearSeen() error {
	if err := d.tx.DeleteBucket([]byte("seen")); err != nil {
		return err
	}
	if _, err := d.tx.CreateBucket([]byte("seen")); err != nil {
		return err
	}
	return d.checkpoint()
}
func (d *diskLogTraversal) push(c logCandidate) error {
	oid, err := hex.DecodeString(c.sha)
	if err != nil || len(oid) != 20 {
		return fmt.Errorf("invalid history candidate identity")
	}
	data, err := proto.Marshal(&pb.LogTraversalCandidate{CommitOid: oid, Path: []byte(c.path), HistoryLocation: c.location})
	if err != nil {
		return err
	}
	var key [16]byte
	// Signed time descending; insertion order ascending, including equal dates.
	binary.BigEndian.PutUint64(key[:8], ^(uint64(c.time) ^ (uint64(1) << 63)))
	binary.BigEndian.PutUint64(key[8:], uint64(c.order))
	if err := d.tx.Bucket([]byte("queue")).Put(key[:], data); err != nil {
		return err
	}
	return d.checkpoint()
}
func (d *diskLogTraversal) pop() (logCandidate, bool, error) {
	bucket := d.tx.Bucket([]byte("queue"))
	key, data := bucket.Cursor().First()
	if key == nil {
		return logCandidate{}, false, nil
	}
	var record pb.LogTraversalCandidate
	if err := proto.Unmarshal(data, &record); err != nil {
		return logCandidate{}, false, err
	}
	if len(key) != 16 || len(record.CommitOid) != 20 {
		return logCandidate{}, false, fmt.Errorf("invalid history traversal scratch record")
	}
	c := logCandidate{location: record.HistoryLocation, sha: hex.EncodeToString(record.CommitOid), path: string(record.Path), time: int64(^binary.BigEndian.Uint64(key[:8]) ^ (uint64(1) << 63)), order: int(binary.BigEndian.Uint64(key[8:]))}
	if err := bucket.Delete(key); err != nil {
		return logCandidate{}, false, err
	}
	if err := d.checkpoint(); err != nil {
		return logCandidate{}, false, err
	}
	return c, true, nil
}
func (d *diskLogTraversal) close() error {
	var result error
	if d.tx != nil {
		result = d.tx.Rollback()
		d.tx = nil
	}
	if d.db != nil {
		result = errors.Join(result, d.db.Close())
		d.db = nil
	}
	if d.name != "" {
		result = errors.Join(result, os.Remove(d.name))
		d.name = ""
	}
	return result
}
