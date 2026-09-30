---
id: TASK-13
title: Reduce redundant work in file-history reads
status: To Do
assignee: []
created_date: '2026-09-29 18:42'
labels: []
dependencies: []
references:
  - internal/repo/history_batch_reader.go
  - internal/repo/history_prefetch.go
  - internal/repo/index.go
  - internal/repo/progressive.go
priority: medium
type: enhancement
ordinal: 13000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Review of 841b3f1 found several read-path inefficiencies, not yet measured:
- internal/repo/history_batch_reader.go: while waiting at a coverage gap the poll loop builds a whole new `Progressive` every 100ms just to re-read HEAD (about 10 remote HEAD GETs per second per waiting query); `repo.go` already has a lighter root refresh.
- `readAhead.after` is only called for frames without links, and `writeLinkedHistoryFrames` rarely produces such frames, so read-ahead seldom starts from newly written data.
- internal/repo/index.go admits every page read from a container under its own `verified-index/...` key; with the disk cache this writes one extra small file per page and the per-entry budget can evict whole containers.
- `Progressive.State` and `HasHistoryIndex` bypass the `pageRanges` point-read path, loading whole containers for status queries.
- `indexedFileLog` allocates a new empty refs index per emitted entry (`log.go` reuses `emptyRefs`).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A waiting file log re-checks publication without constructing a new Progressive and with a bounded HEAD request rate
- [ ] #2 Read-ahead starts for linked history frames; covered by a test using writer-produced frames
- [ ] #3 Status queries use point reads; disk-cache write amplification from page admission is measured and removed or justified
<!-- AC:END -->
