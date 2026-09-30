---
id: TASK-16
title: Keep history frontier complete after draining the ingestion queue
status: Done
assignee:
  - '@claude'
created_date: '2026-09-29 21:17'
updated_date: '2026-09-29 21:17'
labels: []
dependencies: []
references:
  - internal/repo/history_batch_writer.go
  - internal/repo/history_frontier_test.go
priority: high
type: bug
ordinal: 16000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Found while testing on GCE: the repo suite hung on Linux in TestIngestedHistoryClockSkewAndMerges (6/6 runs, also on committed 841b3f1). `writeHistoryFrontier` walks the `queue` and `missing` staging buckets backwards after deleting from them in the same bbolt transaction. bbolt v1.5.0 (latest) leaves emptied leaves until commit; its reverse cursor then stops at the first empty leaf (silently truncating the resumable frontier) or, for an emptied multi-page bucket, never returns from Last(). 4 KiB pages on Linux amd64 hit this in ordinary ingestion; 16 KiB macOS arm64 pages mostly hid it.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A frontier written after draining part of a multi-page queue contains every remaining commit in order
- [x] #2 Writing the frontier after draining the whole queue returns
- [x] #3 The repo suite passes on Linux amd64
<!-- AC:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
writeHistoryFrontier now checkpoints the staging transaction first; bbolt's commit rebalances emptied leaves away before the reverse walk. TestHistoryFrontierAfterDrainingQueue reproduced truncation on macOS (200 of 400 commits) and passes now; the full repo suite passes on the GCE Linux host, where it previously hung.
<!-- SECTION:FINAL_SUMMARY:END -->
