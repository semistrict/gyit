---
id: TASK-5
title: Back GitHub repository storage with S3
status: To Do
assignee: []
created_date: '2026-09-26 18:39'
labels: []
dependencies: []
priority: medium
ordinal: 5000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Imported snapshots currently use durable local storage, separate from disposable cache.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Publish imports atomically with safe concurrent readers.
- [ ] #2 Apply the shared 4 GiB size limit only to disposable cache; never automatically evict durable data.
- [ ] #3 Fetch directory and file data on demand.
<!-- AC:END -->
