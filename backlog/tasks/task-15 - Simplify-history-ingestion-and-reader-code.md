---
id: TASK-15
title: Simplify history ingestion and reader code
status: To Do
assignee: []
created_date: '2026-09-29 18:42'
labels: []
dependencies: []
priority: low
type: chore
ordinal: 15000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Review of 841b3f1 left structural cleanups that make the new history code harder to change safely:
- `indexedFileLog` (history_batch_reader.go, ~270 lines of nested closures with an inline LRU) and `ingestHistoryInputBounded` (history_batch_writer.go, ~370 lines) are too long.
- Duplicates: the bolt queue push closure in history_batch_writer.go and history_compact.go; the parent-location lookup in history_batch_reader.go; two commit-header parsers (`historyBatchHeader` vs `historyBatchCommit`/`parseBufferedCommitParents`), and history_prepare.go fully parsing parents only for CommitTime; trailing-`/` path handling in log.go and history_fallback.go; the repository-check cache in listing.go copies the owner-listing cache; the published-pack identity check in progressive_directory_writer.go vs history_source_native.go.
- Silently dropped errors: history_changes.go, history_publication.go, history_batch_writer.go, history_prefetch.go, log_traversal*.go, `hex.DecodeString` in history_commit.go and history_compact.go, spill/sort.go temp-dir removal.
- Test-only production code: `RebuildHistoryPrefix`, `historyTreeDelta`; history_layout_probe.proto ships generated code in the production storage package.
- Misc: pager spool still named `gyit-log-*` with a "log output" error though it now serves `show`; `pageLog` boolean mode; `flag` shadowing in controlcli/log.go; go.mod marks grpc indirect though store/gcs.go imports it; control.proto does not define `max_count` together with `unlimited`; `repositoryRequest` returns a closed response alongside an error.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 No function in the history reader or writer exceeds ~100 lines
- [ ] #2 Each duplicated rule listed has one implementation
- [ ] #3 Every dropped error listed is returned or logged
- [ ] #4 `go mod tidy` produces no diff and staticcheck reports nothing new
<!-- AC:END -->
