---
id: TASK-1
title: Make gyit log work inside GitHub mounts
status: Done
assignee:
  - '@assistant'
created_date: '2026-09-26 18:39'
updated_date: '2026-09-26 20:03'
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
- [x] #1 Discover the mounted repository and pinned revision without --socket.
- [x] #2 Read complete history from the existing durable gyit store; do not perform a second history fetch or create a separate native Git store.
- [x] #3 Verify supported output and paging against Git.
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
Import full history before publication. Expose a protobuf endpoint attribute at each repository root in FSKit and Linux FUSE. Serve existing read-only commands over a private local Unix socket using the mounted repository and pinned snapshot. Reuse CLI discovery, history engine and pager; reject checkout on pinned GitHub paths.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
User rejected shallow snapshots and the separate native Git history store. Reverting that implementation; mounted log remains unfinished.

Full-history import restored, including all branch and tag refs. Existing shallow publications are bypassed. Initial reads wait up to three seconds before showing NOTICE. CLI mount transport wiring remains to do.

Native Swift callback to real CLI discovery/log test passes. Repository command output matches Git with the remote deleted. Race tests cover discovery, command formatting and existing pager. Installed build 9 exposes the command endpoint. Direct live macOS command verification is blocked by automation-process filesystem permissions; no permissions were broadened.

User reports installed command still fails. Reopened pending real terminal reproduction. Automation-created tmux is denied all mounted filesystem access, including ls; requested a user-terminal-owned tmux server to test without changing permissions.

Live user-owned tmux reproduction found two native failures: FSKit limited xattr list omitted the control attribute, then the sandbox denied creating the socket under global TMPDIR. The final signed app advertises the attribute and keeps the socket under its writable container. Verified live gyit log --oneline -n 3 emitted three commits and gyit log entered less; race tests and native bridge passed. Final review clean.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Mounted gyit log now discovers the native control endpoint and reads complete pinned history from gyit storage. Verified in user-owned tmux with commit output and pager, native bridge, Linux FUSE, race tests and review.
<!-- SECTION:FINAL_SUMMARY:END -->
