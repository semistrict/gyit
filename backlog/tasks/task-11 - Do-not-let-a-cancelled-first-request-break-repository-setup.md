---
id: TASK-11
title: Do not let a cancelled first request break repository setup
status: Done
assignee:
  - '@claude'
created_date: '2026-09-29 18:41'
updated_date: '2026-09-29 19:33'
labels: []
dependencies: []
references:
  - internal/githubfs/progressive.go
priority: medium
type: bug
ordinal: 11000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
In internal/githubfs/progressive.go the snapshot and ancestry `git init`/`config` steps run inside `p.once.Do` using the caller's `ctx`. If the first request that triggers setup is cancelled (Ctrl-C, a closed pager, a FUSE interrupt), `p.err` is set permanently by `sync.Once` and that repository stays broken until the mount restarts. One-time shared setup must not inherit a single caller's lifetime.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Cancelling the request that triggers first-time repository setup does not fail later requests for that repository; covered by a test
- [x] #2 Shared one-time setup is bounded by the filesystem lifetime, not the first caller
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Replace sync.Once with singleflight + ready flag so setup runs under the filesystem context (tracked by f.wg) and a failure is retried by the next caller.
2. Extract idempotent setupProgressive; keep an already opened reader across retries and close a store whose reader failed to open.
3. FS.Close returns store close errors.
4. Test: cancelled first request, then setup and prepare succeed.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
TestCancelledFirstRequestDoesNotBreakSetup failed before ("initialize acquisition: context canceled") and passes now. A failed setup is no longer permanent, which also makes `touch NOTICE` retry effective after setup errors.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Repository setup now runs once per filesystem under f.ctx via singleflight, with callers waiting under their own context and failures retried. Verified by a new regression test, the full suite and race runs.
<!-- SECTION:FINAL_SUMMARY:END -->
