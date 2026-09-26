---
id: TASK-4
title: Improve native mount lifecycle feedback
status: To Do
assignee: []
created_date: '2026-09-26 18:39'
labels: []
dependencies: []
priority: medium
ordinal: 4000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Opaque busy errors, ignored Open repository failures and zero available bytes make a working mount appear broken.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Explain busy unmount failures and offer an explicit force-unmount action.
- [ ] #2 Surface Open repository failures.
- [ ] #3 Report meaningful volume statistics.
<!-- AC:END -->
