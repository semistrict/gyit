---
id: TASK-14
title: Make empty root commit visibility consistent across history paths
status: To Do
assignee: []
created_date: '2026-09-29 18:42'
labels: []
dependencies: []
references:
  - internal/repo/history_fallback.go
  - internal/repo/history_batch_writer.go
priority: low
type: bug
ordinal: 14000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Reported by review of 841b3f1, not yet reproduced: for path `""` and a root commit whose tree is empty, the unindexed fallback (internal/repo/history_fallback.go) reports the commit because the tree OID is non-empty, while the indexed batch writer (internal/repo/history_batch_writer.go) maps the empty tree to no change and hides it. `gyit log` output would then differ depending on whether history coverage exists yet.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A test with an empty root commit shows identical whole-repository log output from indexed and fallback paths, matching `git log`
<!-- AC:END -->
