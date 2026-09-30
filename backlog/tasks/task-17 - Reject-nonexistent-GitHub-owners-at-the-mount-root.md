---
id: TASK-17
title: Reject nonexistent GitHub owners at the mount root
status: Done
assignee:
  - '@claude'
created_date: '2026-09-29 22:29'
updated_date: '2026-09-29 22:37'
labels: []
dependencies: []
references:
  - internal/githubfs/fs.go
  - internal/githubfs/listing.go
priority: medium
type: bug
ordinal: 17000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Seen in the GCE interactive guest: at /mnt/gyit/github.com, `mkdir t` fails with EEXIST instead of EROFS, and every looked-up name (t, asdadsa, a shell probe `mailpath`) then appears in `ls`. `FS.Lookup` for a one-component path returns a directory for any syntactically valid name and records it in the root listing, without asking GitHub whether the owner exists. mkdir looks the name up first, so the kernel reports EEXIST.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Looking up an owner that does not exist on GitHub returns ENOENT, so mkdir at the root reports EROFS
- [x] #2 Names that cannot be GitHub owners (dot files, underscores) are rejected without an API request
- [x] #3 Owner checks are cached like repository checks; transient API failures are not cached as missing
- [x] #4 The root listing never shows a name only because it was looked up and rejected
<!-- AC:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Replace the repository-only check cache with one bounded, expiring check cache (`FS.check`) shared by repository and owner checks.
2. `publicOwner`: owners with jobs or a fresh listing need no request; otherwise GET /users/{owner} (users and orgs), cached with listingTTL (missing 5 min, transient 10 s).
3. Root Lookup calls publicOwner before recording the name for the root listing; `validOwner` in parse rejects non-login names (dots, underscores, leading hyphen, >39 chars) without a request.
4. Test first; verify in a GCE guest.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
TestOwnerLookupRequiresGitHubOwner failed before ("missing owner: <nil>"). GCE guest with the fix: `ls mailpath` -> No such file or directory; `mkdir asdadsa` -> Read-only file system; root `ls` shows only t and torvalds (both real users).
Also fixed a flaky test helper (cacheAllocation lstat of an entry evicted mid-walk) seen once in the full suite, and made the interactive harness stop QEMU itself after guest power-off (it logs the console and watches for "reboot: Power down").
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Root lookups now require the owner to exist on GitHub (`publicOwner`, cached via the new shared `FS.check`), so nonexistent names return ENOENT, mkdir reports EROFS, and rejected names never appear in the root listing. Verified by a new regression test, the full suite, and interactively in a GCE nested-KVM guest.
<!-- SECTION:FINAL_SUMMARY:END -->
