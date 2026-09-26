---
id: TASK-1
title: Make gyit log work inside GitHub mounts
status: To Do
assignee: []
created_date: '2026-09-26 18:39'
labels: []
dependencies: []
priority: high
ordinal: 1000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Mounted history commands currently use the older imported-store control interface, and GitHub setup imports shallow snapshots.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Discover the mounted repository and pinned revision without --socket.
- [ ] #2 Fetch history on demand without blocking ordinary browsing or silently truncating log output.
- [ ] #3 Verify supported output and paging against Git.
<!-- AC:END -->
