---
id: TASK-3
title: Automate installed macOS mount and retry tests
status: To Do
assignee: []
created_date: '2026-09-26 18:39'
labels: []
dependencies: []
priority: high
ordinal: 3000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Callback tests and Linux tests do not cover every FSKit kernel interaction.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Cover public listing, NOTICE replacement, reads and unmount in an installed-app smoke test.
- [ ] #2 Touching a failed synthetic NOTICE retries setup; touching a real repository NOTICE is rejected.
- [ ] #3 Finder metadata probes do not import every listed repository.
<!-- AC:END -->
