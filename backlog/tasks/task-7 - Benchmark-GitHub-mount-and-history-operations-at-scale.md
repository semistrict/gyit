---
id: TASK-7
title: Benchmark GitHub mount and history operations at scale
status: To Do
assignee: []
created_date: '2026-09-26 18:39'
labels: []
dependencies: []
priority: medium
ordinal: 7000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Earlier engine benchmarks do not measure the new GitHub namespace and background setup path.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Measure cold and warm listings, reads, setup and history; flag operations over one second.
- [ ] #2 Keep expensive runs opt-in, reuse fixtures and enforce explicit kill deadlines.
- [ ] #3 Verify directory navigation remains independent of history size.
<!-- AC:END -->
