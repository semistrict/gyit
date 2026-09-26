---
id: TASK-2
title: Connect remaining view commands to GitHub mounts
status: To Do
assignee: []
created_date: '2026-09-26 18:39'
labels: []
dependencies: []
priority: high
ordinal: 2000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Existing status, show, diff and blame commands use the legacy mount interface. Users need commands to operate on the version they are browsing.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Discover the repository from any mounted subdirectory.
- [ ] #2 Compare supported command output against Git using repeatable fixtures.
- [ ] #3 Keep mounted repository data immutable.
<!-- AC:END -->
