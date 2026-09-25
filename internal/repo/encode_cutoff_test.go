package repo

import (
	"bytes"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"math/rand/v2"
	"testing"
)

func TestCutoffCodecParity(t *testing.T) {
	for _, depth := range []int{1, 2, 4} {
		for _, candidates := range []int{1, 4} {
			t.Run(fmt.Sprintf("depth%d-candidates%d", depth, candidates), func(t *testing.T) {
				oldEnc, _ := newCompressor()
				defer oldEnc.Close()
				newEnc, _ := newCompressor()
				defer newEnc.Close()
				old := &chunkCodec{encoder: oldEnc, bases: newBaseCache(candidates), depth: depth}
				updated := &chunkCodec{encoder: newEnc, bases: newBaseCache(candidates), depth: depth}
				r := rand.New(rand.NewPCG(71, 83))
				dec, _ := zstd.NewReader(nil)
				defer dec.Close()
				for _, size := range []int{0, 255, 1024, 4095, 4096, 4097, 65536, 65537, 131072, 262144, 1048576} {
					for _, entropy := range []int{1, 7, 256} {
						raw := make([]byte, size)
						for i := range raw {
							raw[i] = byte(r.IntN(entropy))
						}
						for version := 0; version < 12; version++ {
							if size > 0 {
								for j := 0; j < version*10; j++ {
									raw[r.IntN(size)] = byte(r.IntN(entropy))
								}
							}
							hint := fmt.Sprintf("%d/%d", size, entropy)
							a, b := encodeSlot{raw: raw, hint: hint}, encodeSlot{raw: raw, hint: hint}
							old.encodeFull(&a)
							updated.encode(&b)
							if !bytes.Equal(a.compressed, b.compressed) || a.hash != b.hash || (a.base == nil) != (b.base == nil) || (a.anchor == nil) != (b.anchor == nil) {
								t.Fatalf("selection differs size%d entropy%d version%d", size, entropy, version)
							}
							if a.base != nil && (!bytes.Equal(a.base.raw, b.base.raw) || a.base.depth != b.base.depth) {
								t.Fatal("base differs")
							}
							got, err := dec.DecodeAll(b.compressed, nil)
							if err != nil {
								t.Fatal(err)
							}
							if b.base != nil {
								got, err = applyDelta(b.base.raw, got)
								if err != nil {
									t.Fatal(err)
								}
							}
							if !bytes.Equal(got, raw) {
								t.Fatal("read bytes differ")
							}
						}
					}
				}
			})
		}
	}
}
