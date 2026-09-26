package zstd

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
)

// A cutoff may decline to stop, but must never discard a frame that could win.
// Completed frames and subsequent encoder uses must match the normal encoder.
func TestBoundedEncoderContract(t *testing.T) {
	r := rand.New(rand.NewPCG(32, 51))
	for _, level := range []EncoderLevel{SpeedFastest, SpeedDefault, SpeedBetterCompression} {
		for _, window := range []int{1024, 1 << 20} {
			t.Run(fmt.Sprintf("level%d-window%d", level, window), func(t *testing.T) {
				opts := []EOption{WithEncoderConcurrency(1), WithEncoderLevel(level), WithWindowSize(window), WithZeroFrames(true)}
				normal, err := NewWriter(nil, opts...)
				if err != nil {
					t.Fatal(err)
				}
				defer normal.Close()
				bounded, err := NewWriter(nil, opts...)
				if err != nil {
					t.Fatal(err)
				}
				defer bounded.Close()
				stops := 0
				for _, size := range []int{0, 1, 255, 4096, 65536, 131073, 1 << 20} {
					for _, alphabet := range []int{1, 7, 256} {
						raw := make([]byte, size)
						for i := range raw {
							raw[i] = byte(r.IntN(alphabet))
						}
						// Add repetitions as well as uniform/incompressible inputs.
						if alphabet == 7 && size > 4096 {
							copy(raw[size/2:], raw[:size/2])
						}
						prefix := []byte("existing output")
						want := normal.EncodeAll(raw, bytes.Clone(prefix))
						for _, limit := range []int{-1, 0, 1, 256, len(want) - 1, len(want), len(want) + 1, int(^uint(0) >> 1)} {
							got, stopped := bounded.EncodeAllBelow(raw, bytes.Clone(prefix), limit)
							if stopped {
								stops++
								if len(want) < limit {
									t.Fatalf("false cutoff: size=%d alphabet=%d encoded=%d limit=%d", size, alphabet, len(want), limit)
								}
							} else if !bytes.Equal(got, want) {
								t.Fatalf("frame differs: size=%d alphabet=%d limit=%d", size, alphabet, limit)
							}
							if got := bounded.EncodeAll(raw, bytes.Clone(prefix)); !bytes.Equal(got, want) {
								t.Fatalf("reuse after cutoff differs: size=%d limit=%d", size, limit)
							}
						}
					}
				}
				if stops == 0 {
					t.Fatal("cutoff never exercised")
				}
			})
		}
	}
}

func TestBoundedEncoderEpochWrap(t *testing.T) {
	opts := []EOption{WithEncoderLevel(SpeedFastest), WithEncoderConcurrency(1)}
	normal, err := NewWriter(nil, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer normal.Close()
	bounded, err := NewWriter(nil, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer bounded.Close()
	r := rand.New(rand.NewPCG(13, 41))
	raw := make([]byte, 1<<20)
	for i := range raw {
		raw[i] = byte(r.Uint32())
	}
	want := normal.EncodeAll(raw, nil)
	bounded.init.Do(bounded.initialize)
	for _, offset := range []int32{-1, 0, 1} {
		enc := <-bounded.encoders
		fast := enc.(*fastEncoder)
		fast.cur = fast.bufferReset + offset
		bounded.encoders <- enc
		if _, stopped := bounded.EncodeAllBelow(raw, nil, 256); !stopped {
			t.Fatal("expected incompressible cutoff")
		}
		if got := bounded.EncodeAll(raw, nil); !bytes.Equal(got, want) {
			t.Fatal("epoch reset after cutoff changed frame")
		}
	}
}

func TestBoundedEncoderDictionaryFallback(t *testing.T) {
	dict := bytes.Repeat([]byte("a dictionary with repeated words"), 100)
	opts := []EOption{WithEncoderLevel(SpeedFastest), WithEncoderConcurrency(1), WithEncoderDictRaw(123, dict)}
	e, err := NewWriter(nil, opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	raw := bytes.Repeat(dict, 10)
	want := e.EncodeAll(raw, nil)
	got, stopped := e.EncodeAllBelow(raw, nil, len(want)+1)
	if stopped || !bytes.Equal(got, want) {
		t.Fatal("dictionary fallback changed frame")
	}
}
