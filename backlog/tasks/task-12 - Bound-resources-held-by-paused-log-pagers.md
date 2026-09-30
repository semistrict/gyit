---
id: TASK-12
title: Bound resources held by paused log pagers
status: Done
assignee:
  - '@claude'
created_date: '2026-09-29 18:41'
updated_date: '2026-09-29 21:17'
labels: []
dependencies: []
references:
  - internal/control/server.go
  - internal/repo/history_read_view_native.go
  - internal/repo/history_batch_reader.go
priority: medium
type: bug
ordinal: 12000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`gyit log` requests intentionally have no deadline (internal/control/server.go `operationTimeout` returns 0) because a paused pager or coverage gap is not a stalled request. But a paused pager blocks the socket write, so the request keeps one of `maxConnections = 16` control slots indefinitely; 16 idle pagers make status and update unusable. Separately, the global 2-slot `historyReadViews` semaphore (internal/repo/history_read_view_native.go) is held for the whole `indexedFileLog`, including while `emit` blocks on the pager, so two paged logs on repos with incomplete history block every other such query on any mount.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Status and update requests succeed while more than 16 log pagers are open and paused
- [x] #2 A third file-history query proceeds while two other file-history pagers are paused
- [x] #3 Tests cover both cases
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Control server: admission with request slots (16) and stream slots (64). After reading an open-ended log request, the connection trades its request slot for a stream slot (keeps it if none free). Shared by socket and FUSE control-file transports.
2. History read views: release the process-wide view if the consumer (emit) blocks longer than 250ms; traversal continues through Store and reopens a view at the next object gap. Loaded commits own their bytes, so nothing stays mapped.
3. Tests for both.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
TestPausedLogStreamsDoNotBlockShortRequests: 64 paused log streams, status succeeds; fails ("not all admitted") when the slot trade is removed. It exercises status; update uses the same request-slot admission and was not separately exercised. Beyond 64 streams, further streams keep request slots, so 80 paused pagers would still exhaust the socket.
TestPausedFileLogsDoNotHoldReadViews: two paused file logs, a third completes and the paused ones resume with complete results; failed with deadline exceeded before. Race runs clean.

GCE nested-KVM, through the FUSE control file: `gyit status` beside 20 paused log streams returned in 0.11 s (841b3f1: timed out at 30 s, 4 streams got no slot). A file log beside two paused file logs took 0.02 s (841b3f1: timed out at 60 s; 0.03 s alone).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Paused pagers no longer starve other requests: log streams move to a separate 64-slot stream budget in the control server, and file-history queries release their read view while blocked on the consumer. Verified by two new regression tests (each shown to fail without the fix), the full suite and race runs.
<!-- SECTION:FINAL_SUMMARY:END -->
