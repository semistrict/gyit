---
id: TASK-9
title: Propagate terminal resizes into the interactive guest
status: To Do
assignee: []
created_date: '2026-09-28 02:22'
labels: []
dependencies: []
priority: low
type: bug
ordinal: 9000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The nested-KVM serial console did not receive a terminal size and reported 0 by 0. less rendered at fallback dimensions and produced broken mid-line paging in tmux. The current session was corrected to its actual 214 by 50 dimensions, and the retained launcher now passes the initial host size at boot. Runtime window-size changes still need propagation to the guest and SIGWINCH to the active pager.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A newly attached or resized tmux client gives the guest the actual pane dimensions.
- [ ] #2 Paging forward and backward remains clean after resizing while less is active.
<!-- AC:END -->
