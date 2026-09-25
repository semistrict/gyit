package repo

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"gat/internal/gitdelta"
)

type nativeSampleNode struct {
	base    int64
	data    []byte
	program *gitdelta.Program
}
type nativeSampleCase struct {
	node                        *nativeSampleNode
	parent                      *gitdelta.Program
	root, target                []byte
	base                        *deltaBase
	compressedRoot              []byte
	heuristic, native           []byte
	heuristicDelta, nativeDelta bool
}

func loadNativeSamples(tb testing.TB) []nativeSampleCase {
	tb.Helper()
	if os.Getenv("GAT_NATIVE_DELTA_BENCH") != "1" {
		tb.Skip("set GAT_NATIVE_DELTA_BENCH=1 with the bounded native-delta sample")
	}
	dir := "../../.testdata/native-delta-sample"
	read := func(name string) [][]string {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			tb.Fatal(err)
		}
		defer f.Close()
		r := csv.NewReader(f)
		r.Comma = '\t'
		rows, err := r.ReadAll()
		if err != nil {
			tb.Fatal(err)
		}
		return rows[1:]
	}
	number := func(text string) int64 {
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			tb.Fatal(err)
		}
		return n
	}
	nodes := map[int64]*nativeSampleNode{}
	total := 0
	for _, row := range read("nodes.tsv") {
		if len(row) != 2 || len(nodes) >= 10000 {
			tb.Fatal("invalid or oversized node fixture")
		}
		offset, base := number(row[0]), number(row[1])
		data, err := os.ReadFile(filepath.Join(dir, row[0]+".bin"))
		if err != nil {
			tb.Fatal(err)
		}
		total += len(data)
		if total > 64<<20 {
			tb.Fatal("fixture exceeds 64 MiB")
		}
		node := &nativeSampleNode{base: base, data: data}
		if base < 0 {
			node.program, err = gitdelta.From(uint32(len(data)))
		} else {
			parent, ok := nodes[base]
			if !ok {
				tb.Fatal("fixture is not parent-before-child")
			}
			node.program, err = parent.program.Compose(data)
		}
		if err != nil {
			tb.Fatal(err)
		}
		nodes[offset] = node
	}
	encoder, err := newCompressor()
	if err != nil {
		tb.Fatal(err)
	}
	defer encoder.Close()
	var cases []nativeSampleCase
	for _, row := range read("cases.tsv") {
		if len(row) != 3 || len(cases) >= 1024 {
			tb.Fatal("invalid or oversized case fixture")
		}
		node, root := nodes[number(row[0])], nodes[number(row[1])]
		if node == nil || root == nil || root.base >= 0 || nodes[node.base] == nil {
			tb.Fatal("invalid fixture references")
		}
		target, err := node.program.Materialize(root.data)
		if err != nil {
			tb.Fatal(err)
		}
		h := sha1.New()
		fmt.Fprintf(h, "blob %d\x00", len(target))
		h.Write(target)
		if hex.EncodeToString(h.Sum(nil)) != row[2] {
			tb.Fatal("flattened output differs from Git object ID")
		}
		c := nativeSampleCase{node: node, parent: nodes[node.base].program, root: root.data, target: target, base: newDeltaBase(root.data), compressedRoot: encoder.EncodeAll(root.data, nil)}
		full := encoder.EncodeAll(target, nil)
		old := encoder.EncodeAll(c.base.delta(target), nil)
		wire, err := node.program.Encode()
		if err != nil {
			tb.Fatal(err)
		}
		next := encoder.EncodeAll(wire, nil)
		c.heuristic, c.native = full, full
		if len(old)+200 < len(full)*4/5 {
			c.heuristic, c.heuristicDelta = old, true
		}
		if len(next)+200 < len(full)*4/5 {
			c.native, c.nativeDelta = next, true
		}
		cases = append(cases, c)
	}
	return cases
}

func TestNativeDeltaSampleWireMatchesGit(t *testing.T) {
	cases := loadNativeSamples(t)
	var oldBytes, newBytes, oldCold, newCold, oldDeltas, newDeltas int
	wireHash := sha256.New()
	for _, c := range cases {
		for name, choice := range map[string]struct {
			data  []byte
			delta bool
		}{"heuristic": {c.heuristic, c.heuristicDelta}, "native": {c.native, c.nativeDelta}} {
			decoded, err := decodeFrame(choice.data, 2*ChunkSize)
			if err != nil {
				t.Fatal(err)
			}
			if choice.delta {
				decoded, err = applyDelta(c.root, decoded)
			}
			if err != nil || !bytes.Equal(decoded, c.target) {
				t.Fatalf("%s data differs: %v", name, err)
			}
		}
		wireHash.Write(c.base.delta(c.target))
		oldBytes += len(c.heuristic)
		newBytes += len(c.native)
		oldCold += len(c.heuristic)
		newCold += len(c.native)
		if c.heuristicDelta {
			oldCold += len(c.compressedRoot)
			oldDeltas++
		}
		if c.nativeDelta {
			newCold += len(c.compressedRoot)
			newDeltas++
		}
	}
	t.Logf("heuristic protobuf SHA256=%x", wireHash.Sum(nil))
	t.Logf("cases=%d published bytes heuristic=%d native=%d; cold payload bytes heuristic=%d native=%d; deltas heuristic=%d native=%d", len(cases), oldBytes, newBytes, oldCold, newCold, oldDeltas, newDeltas)
}

func BenchmarkNativeDeltaSample(b *testing.B) {
	cases := loadNativeSamples(b)
	var total int64
	for _, c := range cases {
		total += int64(len(c.target))
	}
	for _, mode := range []string{"encode-heuristic", "encode-native", "read-heuristic", "read-native"} {
		b.Run(mode, func(b *testing.B) {
			encoder, err := newCompressor()
			if err != nil {
				b.Fatal(err)
			}
			defer encoder.Close()
			b.SetBytes(total)
			b.ReportAllocs()
			var fullBuffer, compressedBuffer, wireBuffer, rawBuffer []byte
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, c := range cases {
					switch mode {
					case "encode-heuristic":
						fullBuffer = encoder.EncodeAll(c.target, fullBuffer[:0])
						wireBuffer = c.base.deltaInto(wireBuffer[:0], c.target)
						compressedBuffer = encoder.EncodeAll(wireBuffer, compressedBuffer[:0])
						sha256.Sum256(c.target)
					case "encode-native":
						program, err := c.parent.Compose(c.node.data)
						if err != nil {
							b.Fatal(err)
						}
						data, err := program.MaterializeInto(rawBuffer, c.root)
						if err != nil {
							b.Fatal(err)
						}
						rawBuffer = data
						wire, err := program.AppendTo(wireBuffer[:0])
						if err != nil {
							b.Fatal(err)
						}
						wireBuffer = wire
						fullBuffer = encoder.EncodeAll(data, fullBuffer[:0])
						compressedBuffer = encoder.EncodeAll(wire, compressedBuffer[:0])
						sha256.Sum256(data)
					default:
						data, isDelta := c.heuristic, c.heuristicDelta
						if mode == "read-native" {
							data, isDelta = c.native, c.nativeDelta
						}
						decoded, err := decodeFrame(data, 2*ChunkSize)
						if err != nil {
							b.Fatal(err)
						}
						if isDelta {
							decoded, err = applyDelta(c.root, decoded)
							if err != nil {
								b.Fatal(err)
							}
						}
						sha256.Sum256(decoded)
					}
				}
			}
		})
	}
}
