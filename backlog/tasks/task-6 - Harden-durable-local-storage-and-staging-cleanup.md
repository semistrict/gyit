---
id: TASK-6
title: Harden durable local storage and staging cleanup
status: To Do
assignee: []
created_date: '2026-09-26 18:39'
labels: []
dependencies: []
priority: medium
ordinal: 6000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Linux durable storage defaults sit beside a cache directory, and crashes can leave setup staging behind.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Keep durable storage outside OS cache directories by default.
- [ ] #2 Reclaim abandoned staging without deleting published data or active setup work.
- [ ] #3 Deleting cache preserves imported repositories and local offline reads.
<!-- AC:END -->
