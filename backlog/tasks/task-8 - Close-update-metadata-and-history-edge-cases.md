---
id: TASK-8
title: Close update metadata and history edge cases
status: To Do
assignee: []
created_date: '2026-09-28 02:22'
labels: []
dependencies: []
priority: medium
type: bug
ordinal: 8000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Source review after the virtio-fs scan optimization found follow-ups outside the measured scan criterion. Open file reads are pinned, but FUSE fstat still resolves the live path; the Go-FUSE bridge also fabricates a file handle for some path getattr calls, so simply using every supplied handle would make path attributes stale. The FSKit adapter likewise needs explicit per-open content identity. The regular-FUSE update test assumes immediate delivery of asynchronous notifications, despite the accepted one-second freshness window. Historical unprepared tree entries require name sorting before binary search. Shared container flights should not propagate one caller cancellation to unrelated live waiters. These are recorded for follow-up; they were not claimed fixed by the scan benchmark.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 After an update, old file descriptors retain original content and attributes while new path lookups return current attributes on FUSE and FSKit.
- [ ] #2 Cache coherence tests wait only through the permitted freshness bound and fail if the refreshed tree never appears.
- [ ] #3 Historical tree lookup and pagination handle directory foo adjacent to file foo.bar.
- [ ] #4 Canceling one container reader does not fail another live reader of the same container.
<!-- AC:END -->
