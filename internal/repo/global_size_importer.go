package repo

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"unsafe"

	sizetable "gyit/internal/globalsizes"
	sizewire "gyit/internal/globalsizes/wire"
	"gyit/internal/store"
)

const globalSizesFormat = formatVersion

// Collection runs within the existing inventory pass. Explicit growth accounts
// for both old and new backing arrays while copying, without a source prepass.
func (s *sourceInfo) collectGlobalSize(key []byte, size int64) error {
	if len(key) != 20 || size < 0 || len(s.globalRecords) == sizetable.MaxRecords {
		return fmt.Errorf("global size input bound")
	}
	if len(s.globalRecords) == cap(s.globalRecords) {
		next := min(sizetable.MaxRecords, max(1024, cap(s.globalRecords)*2))
		live := uint64(cap(s.globalRecords)+next) * uint64(unsafe.Sizeof(sizetable.Record{}))
		if live > sizetable.ScratchLimit {
			return fmt.Errorf("global size collection memory bound")
		}
		s.globalCollectPeak = max(s.globalCollectPeak, live)
		grown := make([]sizetable.Record, len(s.globalRecords), next)
		copy(grown, s.globalRecords)
		s.globalRecords = grown
	}
	var record sizetable.Record
	copy(record.OID[:], key)
	record.Size = size
	s.globalRecords = append(s.globalRecords, record)
	return nil
}

func writeGlobalSizes(ctx context.Context, info *sourceInfo, st *stage, backend store.Store) (int64, error) {
	defer func() { info.globalRecords = nil }()
	streamingTrace("global_sizes_build_start", int64(len(info.globalRecords)))
	payload, counts, err := sizetable.Build(ctx, info.globalRecords)
	if err != nil {
		return 0, err
	}
	encoded, err := sizewire.Encode(payload)
	if err != nil {
		return 0, err
	}
	// The borrowed inventory array and neutral/encoded payload coexist here.
	live := counts.InputBytes + counts.RetainedBytes + uint64(cap(encoded)) + counts.MetadataAllowanceBytes
	if live > sizetable.ScratchLimit {
		return 0, fmt.Errorf("global size encoding memory bound")
	}
	streamingTrace("global_sizes_build_end", int64(counts.Records))
	streamingTrace("global_sizes_records", int64(counts.Records))
	streamingTrace("global_sizes_encoded_bytes", int64(len(encoded)))
	streamingTrace("global_sizes_scratch_peak", int64(max(info.globalCollectPeak, counts.PeakScratchBytes, live)))
	streamingTrace("global_sizes_hash_visits", int64(counts.HashVisits))
	hash := fmt.Sprintf("%x", sha256.Sum256(encoded))
	key := "index/global-sizes-" + rand.Text()
	if err = backend.Put(ctx, key, encoded, "*"); err != nil {
		return 0, err
	}
	if err = st.put("x/global-sizes", chunk{Pack: key, Length: int64(len(encoded)), Hash: hash}); err != nil {
		return 0, err
	}
	streamingTrace("global_sizes_durable", int64(len(encoded)))
	return int64(len(encoded)), nil
}
