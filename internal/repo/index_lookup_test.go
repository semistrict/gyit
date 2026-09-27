package repo

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	storagev1 "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
)

func TestIndexPointLookupDoesNotDecodeSiblings(t *testing.T) {
	ctx := t.Context()
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w := &indexWriter{ctx: ctx, store: backend, prefix: "point-lookup"}
	p := page{}
	for i := 0; i < fanout; i++ {
		value, err := marshal(object{Kind: "blob", Size: int64(i)})
		if err != nil {
			t.Fatal(err)
		}
		p.Items = append(p.Items, item{fmt.Sprintf("o/%04d", i), value})
	}
	e, err := w.save(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	idx := &index{store: backend, cache: newCache(1 << 20), root: e.ID}
	var got object
	if err := idx.get(ctx, "o/0064", &got); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(100, func() {
		if err := idx.get(ctx, "o/0064", &got); err != nil {
			t.Fatal(err)
		}
	})
	if got.Kind != "blob" || got.Size != 64 {
		t.Fatalf("wrong record: %+v", got)
	}
	// A point lookup may decode its result, but not allocate one object per
	// sibling. This is independent of the size of an index record's payload.
	if allocs > 30 {
		t.Fatalf("point lookup allocated %.0f objects; decoded siblings", allocs)
	}
}

func TestIndexPointLookupWire(t *testing.T) {
	value, err := marshal(object{Kind: "blob", Size: 71})
	if err != nil {
		t.Fatal(err)
	}
	ref := pageRef{Pack: "index/test/1", Offset: 17, Length: 23, Hash: "hash"}
	for _, routing := range []bool{false, true} {
		p := page{}
		for i := 0; i < fanout; i++ {
			key := fmt.Sprintf("o/%04d", i*2)
			if routing {
				p.Children = append(p.Children, edge{key, ref})
			} else {
				p.Items = append(p.Items, item{key, value})
			}
		}
		wire, err := marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		// Unknown fields must remain forward compatible with protobuf readers.
		wire = protowire.AppendVarint(protowire.AppendTag(wire, 31, protowire.VarintType), 42)
		for i := -1; i <= fanout*2; i++ {
			key := fmt.Sprintf("o/%04d", i)
			var got object
			next, err := lookupIndexPage(wire, key, &got)
			found := i >= 0 && i <= 2*(fanout-1) && (routing || i%2 == 0)
			// A key before the first child still routes to that child.
			if routing && i < 0 {
				found = true
			}
			if !found {
				if !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("routing=%v key=%q: %v", routing, key, err)
				}
			} else if err != nil || (routing && next != ref) || (!routing && (next != (pageRef{}) || got.Size != 71)) {
				t.Fatalf("routing=%v key=%q: next=%+v got=%+v err=%v", routing, key, next, got, err)
			}
		}
	}
	// Protobuf permits reordered fields and repeated singular keys (last wins).
	raw := protowire.AppendBytes(protowire.AppendTag(nil, 2, protowire.BytesType), value)
	raw = protowire.AppendString(protowire.AppendTag(raw, 1, protowire.BytesType), "old")
	raw = protowire.AppendString(protowire.AppendTag(raw, 1, protowire.BytesType), "new")
	wire := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), raw)
	var got object
	if _, err := lookupIndexPage(wire, "new", &got); err != nil || got.Size != 71 {
		t.Fatalf("reordered fields: %+v %v", got, err)
	}
	for _, bad := range [][]byte{{0xff}, {0x0a, 0xff}, {0x08, 0x01}, append(append([]byte{}, wire...), wire...)} {
		if _, err := lookupIndexPage(bad, "new", &got); err == nil {
			t.Fatalf("accepted malformed page %x", bad)
		}
	}
}

func FuzzIndexPointLookup(f *testing.F) {
	p := &storagev1.IndexPage{Items: []*storagev1.IndexItem{{Key: "test", Value: []byte{0x08, 0x03}}}}
	wire, _ := proto.Marshal(p)
	f.Add(wire, "test")
	f.Fuzz(func(t *testing.T, b []byte, key string) {
		if len(b) > indexPackSize {
			t.Skip()
		}
		var got object
		_, _ = lookupIndexPage(b, key, &got)
	})
}
