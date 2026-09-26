package repo

import (
	metaplan "gyit/internal/packmeta"
	archiveplan "gyit/internal/archive/planner"
	archivewire "gyit/internal/archive/wire"
)

type sourceRecipePlanner interface {
	Has(string) bool
	Recipe(string, [16]byte) (archivewire.Recipe, error)
	Stats() archiveplan.Statistics
	Close() error
}
type metadataRecipeAdapter struct{ *metaplan.Planner }

func (p metadataRecipeAdapter) Stats() archiveplan.Statistics {
	s := p.Planner.Stats()
	return archiveplan.Statistics{SourceObjects: s.SourceObjects, InventoryLookups: s.InventoryLookups, SourceHeaders: s.SourceHeaders, SourceHeaderBytes: s.SourceHeaderBytes, Roots: s.Roots, OFSParents: s.OFSParents, REFParents: s.REFParents, ParentLookups: s.ParentLookups, OIDProbeSteps: s.OIDProbeSteps, OffsetProbeSteps: s.OffsetProbeSteps, DPNodeVisits: s.DPNodeVisits, DPEdgeVisits: s.DPEdgeVisits, LimitNodes: s.LimitNodes, MalformedNodes: s.MalformedNodes, SourceMappedBytes: s.SourceMappedBytes, ScratchMappedBytes: s.ScratchMappedBytes, BuildPeakScratchMappedBytes: s.BuildPeakScratchMappedBytes, RecordBytes: s.RecordBytes, HashSlotBytes: s.HashSlotBytes, BuildStackBytes: s.BuildStackBytes, RecipeCalls: s.RecipeCalls, RecipeAccepted: s.RecipeAccepted, RecipeLimitFallbacks: s.RecipeLimitFallbacks, RecipeMalformedFailures: s.RecipeMalformedFailures, BlobAccepted: s.BlobAccepted, TreeAccepted: s.TreeAccepted, BlobFallbacks: s.BlobFallbacks, TreeFallbacks: s.TreeFallbacks, RecipeFrames: s.RecipeFrames, RecipeParentVisits: s.RecipeParentVisits, RecipeBytes: s.RecipeBytes, RecipePackedBytes: s.RecipePackedBytes, RecipeFetchedBytes: s.RecipeFetchedBytes, RecipeWork: s.RecipeWork, PrefixInflations: s.PrefixInflations, FullInflations: s.FullInflations, FullInflatedBytes: s.FullInflatedBytes, Reconstructions: s.Reconstructions, ReconstructedBytes: s.ReconstructedBytes, PayloadCopiedBytes: s.PayloadCopiedBytes}
}
