---
id: TASK-10
title: Keep foreground reads responsive during background history acquisition
status: Done
assignee:
  - '@claude'
created_date: '2026-09-29 18:41'
updated_date: '2026-09-29 21:17'
labels: []
dependencies: []
references:
  - internal/githubfs/progressive.go
  - internal/githubfs/history_acquisition.go
priority: high
type: bug
ordinal: 10000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Review of 841b3f1 found that `ingestBackgroundHistory` in internal/githubfs/progressive.go holds `p.operations` (the lock also taken by `Demand` for foreground blob reads and by `prepareProgressive`) for the entire `git fetch --deepen` step, which now grows to 16384 generations (previously capped at 2048). A user opening files in a large repository can stall behind a long history fetch. The deepen also acquires the lock inline instead of through `p.lock`.

The same review found that when the speculative early ancestry fetch fails (`early.err != nil`), the full-acquisition goroutine returns that error instead of running its own fetch, so one transient network error aborts the whole background history run and ends shallow progress via `waitFull`.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A foreground file read that needs a blob completes while a background history deepen is in progress, covered by a test with a blocked/slow history fetch
- [x] #2 A failed early ancestry fetch falls back to the normal full history fetch; a test injects one early failure and history still completes
- [x] #3 All operations-lock acquisitions go through one helper
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Split acquisition.git into a blob lane and a shallow lane (Git allows one shallow-boundary fetch at a time via shallow.lock; blob fetches do not move it). Depth-one snapshot fetch and deepen batches use the shallow lane; Demand uses the blob lane.
2. Replace ad-hoc channel semaphores with a small `lane` type.
3. Extract full-ancestry acquisition; treat the early transfer as speculative and refetch when it failed.
4. Tests: file read from inside a deepen batch; injected early failure still reaches complete coverage.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Added TestFileReadDuringHistoryDeepen (reads dir/hello from the progress callback while the deepen batch owns its lane; failed with deadline exceeded before the split) and TestFailedEarlyAncestryFallsBackToFullFetch (mutation of the fallback reproduces "transient network failure"). Full `go test ./...` and `-race` on githubfs pass.

GCE nested-KVM (scripts/verify_nested_kvm_regressions.py, torvalds/linux, native GCS): reads that started during a deepen batch took at most 3.1 s after the fix; the 841b3f1 build took 12.8-15.4 s (they finished only when the batch ended).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Foreground blob reads no longer wait for background history: acquisition.git now has separate blob and shallow lanes (`lane` type replaces inline channel semaphores). A failed speculative early ancestry fetch now falls back to a normal full fetch via `acquireFullAncestry`. Verified by two new regression tests, the full suite, and race runs.
<!-- SECTION:FINAL_SUMMARY:END -->
