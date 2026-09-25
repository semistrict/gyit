package repo

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"

	"gat/internal/packmeta"
	"gat/internal/store"
)

// Import configuration belongs to one invocation. In particular, concurrent
// imports must never select a source pack through process-wide environment state.
type importConfiguration struct {
	pack           string
	fallbackReason string
}
type importConfigurationKey struct{}

func importConfig(ctx context.Context) importConfiguration {
	config, _ := ctx.Value(importConfigurationKey{}).(importConfiguration)
	return config
}

func archiveImportEnabled(ctx context.Context) bool { return importConfig(ctx).pack != "" }
func nativeImportPack(ctx context.Context) string   { return importConfig(ctx).pack }

func selectImport(ctx context.Context, opt ImportOptions, workers int) (importConfiguration, error) {
	fallback := func(reason string) (importConfiguration, error) {
		return importConfiguration{fallbackReason: reason}, nil
	}
	if opt.Revision != "" {
		return fallback("a restricted revision requires reachable-object import")
	}
	if opt.DisableDeltas {
		return fallback("delta compression is disabled")
	}
	if opt.DeltaDepth != 0 || opt.DeltaCandidates != 0 {
		return fallback("explicit delta settings require chunk conversion")
	}
	if workers <= 1 {
		return fallback("archive import requires at least two workers")
	}
	format, err := git(ctx, opt.Repo, "rev-parse", "--show-object-format").Output()
	if err != nil {
		return importConfiguration{}, fmt.Errorf("read source repository: %w", err)
	}
	if strings.TrimSpace(string(format)) != "sha1" {
		return fallback("archive import currently requires SHA-1 objects")
	}
	ok, err := qualifyRawCommitParents(ctx, opt.Repo)
	if err != nil {
		return importConfiguration{}, err
	}
	if !ok {
		return fallback("the source has history or object-directory overrides")
	}
	pack, ok, err := qualifyPackSource(ctx, opt.Repo, 20)
	if err != nil {
		return importConfiguration{}, err
	}
	if !ok {
		return fallback("the source is not one complete pack with an index")
	}
	return importConfiguration{pack: pack}, nil
}

func importWithMetadataThreshold(ctx context.Context, backend store.Store, opt ImportOptions, metadataThreshold int64) (Stats, error) {
	if opt.CompressionWorkers < 0 {
		return Stats{}, fmt.Errorf("compression workers cannot be negative")
	}
	if opt.DeltaDepth < 0 || opt.DeltaDepth > MaxDeltaDepth {
		return Stats{}, fmt.Errorf("delta depth must be 1..%d", MaxDeltaDepth)
	}
	if opt.DeltaCandidates < 0 || opt.DeltaCandidates > maxDeltaCandidates {
		return Stats{}, fmt.Errorf("delta candidates must be 1..%d", maxDeltaCandidates)
	}
	workers := opt.CompressionWorkers
	if workers == 0 {
		workers = min(4, runtime.GOMAXPROCS(0))
	}
	config, err := selectImport(ctx, opt, workers)
	if err != nil {
		return Stats{}, err
	}
	selected := context.WithValue(ctx, importConfigurationKey{}, config)
	stats, err := importSelected(selected, backend, opt, metadataThreshold)
	if config.pack != "" && errors.Is(err, planner.ErrUnsupported) && ctx.Err() == nil {
		// Native planning rejects unsupported structures before payload dispatch.
		// Its children and staging files have been joined and removed on return.
		config = importConfiguration{fallbackReason: "source pack metadata requires reachable-object conversion"}
		return importSelected(context.WithValue(ctx, importConfigurationKey{}, config), backend, opt, metadataThreshold)
	}
	return stats, err
}
