package repo

import (
	"bytes"
	"fmt"
	"sort"
	"unicode/utf8"

	storagev1 "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// lookupIndexPage searches the immutable protobuf in place. Stack-resident
// offsets replace decoding all siblings into heap objects. Only the selected
// value (or routing reference) is decoded, so no mmap view escapes its lease.
func lookupIndexPage(data []byte, key string, out any) (pageRef, error) {
	var keys, records [fanout][]byte
	count := 0
	var kind protowire.Number
	bad := func() (pageRef, error) { return pageRef{}, fmt.Errorf("invalid index page") }
	for len(data) > 0 {
		field, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return bad()
		}
		data = data[n:]
		if field != 1 && field != 2 {
			n = protowire.ConsumeFieldValue(field, typ, data)
			if n < 0 {
				return bad()
			}
			data = data[n:]
			continue
		}
		if typ != protowire.BytesType || count == fanout || (kind != 0 && kind != field) {
			return bad()
		}
		kind = field
		raw, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return bad()
		}
		data = data[n:]
		k, err := indexRecordKey(raw)
		if err != nil {
			return pageRef{}, err
		}
		if count > 0 && bytes.Compare(keys[count-1], k) >= 0 {
			return bad()
		}
		keys[count], records[count] = k, raw
		count++
	}
	i := sort.Search(count, func(i int) bool { return bytes.Compare(keys[i], []byte(key)) >= 0 })
	if i == count {
		return pageRef{}, store.ErrNotFound
	}
	if kind == 2 {
		var child storagev1.IndexChild
		if err := proto.Unmarshal(records[i], &child); err != nil {
			return pageRef{}, err
		}
		ref := decodePageRef(child.Page)
		if ref == (pageRef{}) {
			return bad()
		}
		return ref, nil
	}
	if !bytes.Equal(keys[i], []byte(key)) {
		return pageRef{}, store.ErrNotFound
	}
	var item storagev1.IndexItem
	if err := proto.Unmarshal(records[i], &item); err != nil {
		return pageRef{}, err
	}
	return pageRef{}, unmarshal(item.Value, out)
}

func indexRecordKey(data []byte) ([]byte, error) {
	var key []byte
	for len(data) > 0 {
		field, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if field == 1 {
			if typ != protowire.BytesType {
				return nil, fmt.Errorf("invalid index key")
			}
			key, n = protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			if !utf8.Valid(key) {
				return nil, fmt.Errorf("invalid index key UTF-8")
			}
		} else {
			n = protowire.ConsumeFieldValue(field, typ, data)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
		}
		data = data[n:]
	}
	return key, nil
}
