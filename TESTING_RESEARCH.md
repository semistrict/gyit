# Test expansion research

Investigated 2026-09-26. Recommendations below are proposals unless an existing
test or implementation is linked explicitly. The [mutation closure](#mutation-closure)
section records the completed follow-up on all 26 initial gaps.

## Implemented first layer

These tests run in the ordinary suite without new dependencies or remote services:

- [Seeded Git histories](internal/repo/model_history_test.go): three fixed seeds,
  ten commits each, SHA-1 and SHA-256, branching/merge graphs, deletion, binary and
  empty files, executable bits, symlinks, and unusual names. An independent byte
  map checks every revision across loose objects, repacking, and disabled deltas.
  The test requires the expected importer, exercises one-entry directory pages
  and EOF reads, then removes the source and checks fresh readers again.
- [Publication fault schedules](internal/repo/publication_fault_schedule_test.go):
  sixteen schedules across both importers, failing before or losing the reply
  after a write to packs, indexes, generation manifests, or HEAD. The adapter
  wraps real local storage. Each schedule requires that its fault fired, checks
  the exact selected generation, retries against leftover staging objects, and
  verifies the old pinned snapshot and scratch cleanup.
- [Virtual-time cache simulation](internal/repo/cache_simulation_test.go): the
  real cache and singleflight implementation face an hour-long simulated fetch,
  a canceled waiter, another live waiter, and successful or failed dependency
  replies. Explicit admission gates plus `synctest.Wait` make these schedules
  repeatable. Failed reads must be retryable; successful reads must be cached.
- [Delta fuzzing](internal/gitdelta/program_fuzz_test.go): arbitrary native delta
  bytes are checked against an independent byte interpreter, including chained
  composition, ownership and protobuf readback. A second target generates valid
  edit chains so malformed headers cannot dominate the search. Fuzz seeds also
  run under ordinary `go test` and mutation testing.
- [Delta boundaries](internal/gitdelta/program_limits_test.go): exactly maximum
  chunks, caller-buffer reuse, large append prefixes, and truncated empty-base
  headers. These assertions were added after inspecting mutation survivors.

Run the bounded campaigns and replay one history seed:

```sh
go test ./internal/repo -run 'TestSeededGitHistoryAcrossStorageLayouts|TestPublicationFaultSchedule|TestCacheSimulationSharedFlight' -count=1
go test ./internal/repo -run 'TestSeededGitHistoryAcrossStorageLayouts/sha1/seed=7$' -count=1 -v
go test ./internal/gitdelta -run '^$' -fuzz '^FuzzComposeAgainstByteInterpreter$' -fuzztime=30s -parallel=2
go test ./internal/gitdelta -run '^$' -fuzz '^FuzzComposeEditSequence$' -fuzztime=30s -parallel=2
```

Limits: the history campaign checks snapshot contents, not every Git command or
ordered-parent query. Its seeds reproduce fixture generation, not Go scheduling.
Publication schedules select a semantic write class; they do not enumerate every
individual write or every worker interleaving. They model atomic storage with
failed requests/lost responses, not power loss, real S3, or torn filesystem writes.
Only the cache simulation uses virtual time; real Git and disk I/O stay outside
the simulation bubble. These are complementary layers, not a complete simulator.

## Start from real Git, then vary the environment

Small local Git repositories already underpin
[directory-history tests](internal/repo/directory_history_test.go), while the
[correctness oracle](internal/repo/correctness_oracle_test.go) compares published
views against Git. Keep that independent oracle. The normal suite should remain
local and bounded, as [CONTRIBUTING.md](CONTRIBUTING.md) requires.

Recommended layers:

| Layer | Concrete experiment | Required invariant |
| --- | --- | --- |
| Real Git differential tests | Generate small histories containing merges, deletes, renames, empty/binary files, executable bits, symlinks, unusual filenames, and tags | Imported trees, bytes, modes, refs, and ordered parents match Git |
| Representation changes | Repack the same repository; import with supported archive/conversion options; reopen with a fresh reader | Representation and cache state never change observable contents |
| Stateful campaign | Seeded sequence of commit, import, open, read, cancel, retry, and competing publication | Old snapshots retain their contents; current snapshots match the selected publication |
| Fault schedules | Fail, block, truncate, or corrupt selected store operations at explicit boundaries | No partial publication, unauthenticated bytes, or successful reader result after integrity failure |
| Parser fuzzing | Malformed delta programs, pack metadata, pathspecs, and bounded range arithmetic | No panic, unbounded allocation, invalid successful result, or input-buffer aliasing |

Git's `fast-import` supports explicit commit parents, merges, file modes, and
binary data lengths, making it useful for inexpensive generated histories. Use
Git commands with NUL-delimited output as the oracle for arbitrary legal names.
Seed replay must capture the operation stream and Git version; fix object format,
identity, timestamps, and relevant Git configuration.
[Git fast-import reference](https://git-scm.com/docs/git-fast-import)

Do not build a second importer as the model. Keep an independent map of expected
path/mode/content and publication identity; exercise the production importer,
store contract, and reader. Retain old snapshot handles through updates and
verify them again. Shrink a failure to the smallest operation sequence and save
that sequence as a normal regression asserting correct behavior.

## Transferable patterns from the local simulation project

The local reference is [butter's testing guide](/Users/ramon/src/butter/docs/testing.md).
Its useful pattern is one deployment harness, real production implementations,
simulated dependencies, a small command vocabulary, and invariant checks after
each transition. It records seeds and recent events; explicit schedules make
faults reproducible. Transfer these ideas incrementally at `store.Store` and
existing importer synchronization seams. A whole virtual operating system is
unnecessary for the first campaign.

Use gates such as “all object writes completed, HEAD CAS blocked” to release
competing publishers in both orders. Distinguish failure before a write from a
lost response after a successful write; verify the contract of each operation
instead of treating them as interchangeable failures. Check the actual selected
HEAD and require one complete corresponding snapshot, never a mixture.

Go 1.26.6 already provides `testing/synctest`: isolated goroutines, fake time,
and `Wait` for quiescence. It is useful for retry, cancellation, and worker
lifecycle tests with in-process dependencies. It does not explore every possible
schedule. Real sockets, subprocesses, and syscalls do not durably block its clock;
keep Git subprocesses outside the bubble and retain separate real-I/O tests.
[Versioned synctest documentation](https://pkg.go.dev/testing/synctest@go1.26.6)

Native Go fuzzing minimizes failures and stores replayable corpus inputs.
Use bounded byte inputs for fast parsers; seed ordinary tests with meaningful
valid and invalid cases. Run expensive Git-history generation as a small seeded
campaign first. Fuzz targets should assert semantics, not merely absence of
panics. [Go fuzzing guide](https://go.dev/doc/security/fuzz/)

Rapid adds structured generators, state-machine testing, shrinking, and replay.
Consider it when shrinking command sequences becomes cumbersome; no dependency
is needed to begin with fixed seeds and native fuzzing.
[Rapid API documentation](https://pkg.go.dev/pgregory.net/rapid)

## Gremlins: suitable, with an outcome-audit caveat

GitHub's latest release API returned **v0.6.0**, released 2025-12-06. Its module
requires Go 1.25 and its CI selects that module version; these facts do not
establish compatibility with this project's Go 1.26.6. Qualify it with a local
smoke run. The release includes a Darwin arm64 artifact.
[Release](https://github.com/go-gremlins/gremlins/releases/tag/v0.6.0),
[module](https://github.com/go-gremlins/gremlins/blob/v0.6.0/go.mod),
[CI](https://github.com/go-gremlins/gremlins/blob/v0.6.0/.github/workflows/ci.yml)

Install the pinned version independently of project dependencies:

```sh
go install github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0
```

[Official installation instructions](https://gremlins.dev/latest/install/)

`unleash` accepts a directory, **not a Go package pattern**. The tagged source
confirms that `internal/gitdelta` scopes coverage to that subtree; ordinary
mutations run tests in the affected package. Integration mode expands tests to
the whole module. `--coverpkg` controls coverage instrumentation, not mutation
selection. [CLI source](https://github.com/go-gremlins/gremlins/blob/v0.6.0/cmd/unleash.go),
[coverage implementation](https://github.com/go-gremlins/gremlins/blob/v0.6.0/internal/coverage/coverage.go)

**Audit kills before trusting a score.** In v0.6.0, exit status 1 is classified
as `KILLED`, and 2 as `NOT VIABLE`. A Go compilation failure can consequently
appear as a killed mutant. The local
[Gremlins wrapper](/Users/ramon/src/butter/scripts/mutate-gremlins.py) and
[Go invocation audit](/Users/ramon/src/butter/scripts/mutation/gremlins-go-audit.py)
separate compilation errors from test failures. Preserve that distinction,
plus timeout, infrastructure error, no tests, and genuine assertion failure.
[Tagged executor](https://github.com/go-gremlins/gremlins/blob/v0.6.0/internal/engine/executor.go)

### Bounded first campaign

1. Snapshot the current source, including uncommitted tests, native inputs,
   fixtures, and **`third_party/compress`**: the local `replace` in
   [go.mod](go.mod) requires it. Butter's snapshot helper excludes `third_party`
   and must not be copied unchanged. Run the baseline in the snapshot and hash
   its inputs.
2. Start with `internal/gitdelta`, then `internal/pathspec` once it has direct
   package tests. Use two workers, a 15-minute campaign deadline, and the
   following dry run from the snapshot root:

   ```sh
   gremlins unleash internal/gitdelta --dry-run --workers=2 \
     --timeout-coefficient=3 --output=mutation-dry.json
   ```

3. Require nonempty runnable mutations. Repeat without `--dry-run`, retaining
   JSON results and audited Go test logs. The executed first trial is recorded
   below; broader campaigns remain proposals.
4. Review survivors individually. Add an assertion or boundary case only when
   it expresses intended behavior, then replay the exact mutant to prove the
   improved test kills it. Document equivalent mutants; do not write assertions
   that merely duplicate the implementation.
5. Expand to publication/store logic after the first audited run. Use
   `--integration --coverpkg=./internal/pathspec` when callers provide the
   relevant assertions. Exclude `internal/gen/`, `third_party/`, and generated
   sources from any broader mutation scope. Add `--diff` only after the baseline
   works; a tests-only diff will not select unchanged production mutants.

Gremlins supplies file exclusions, diff selection, JSON output, worker limits,
timeout multipliers, and optional efficacy/coverage thresholds. Thresholds
default to zero, so a successful exit does not mean all mutants died. A dry run
still gathers coverage. Begin with reporting; introduce gates only after
classification and runtime are understood.
[Command reference](https://gremlins.dev/latest/usage/commands/unleash/)

Complement automatic operator mutations with a tiny reviewed semantic catalogue:
publish HEAD early, omit authentication, ignore CAS conflict, reuse a mutable
cache buffer, or return before canceled workers finish. Apply one fault in an
isolated source snapshot, require a clean baseline, and count only intended
test failures as kills. Butter's
[semantic mutation runner](/Users/ramon/src/butter/scripts/mutate-simulation.py)
provides a model for separating baseline, build, timeout, and assertion outcomes.
These catalogue results measure the stated fault hypotheses; they are not a
general mutation-coverage percentage.

## Initial local trial

Gremlins v0.6.0 was built with Go 1.26.6 on Darwin arm64 and run against an
isolated source snapshot, including the local compression replacement. The
binary reports `dev` when installed from source; `go version -m` confirms the
v0.6.0 module version. The bounded `internal/gitdelta` trial used two workers,
`--timeout-coefficient=5`, and `GOFLAGS='-count=1 -timeout=30s -json'`.
Butter's Go-invocation audit was copied into the trial's temporary tooling and
classified each actual test run independently of Gremlins' reported outcome.

| Trial | Assertion/panic failures | Survived | Uncovered | Build failures / timeouts |
| --- | ---: | ---: | ---: | ---: |
| Initial tests plus byte-interpreter fuzz seeds | 82 | 19 | 18 | 0 / 0 |
| Added exact-size, buffer and empty-header boundaries, plus edit-chain seeds | 88 | 13 | 18 | 0 / 0 |
| Added nonadjacent-copy and span-boundary seeds | 93 | 8 | 18 | 0 / 0 |

The final audit reconciles all 101 executed mutations: 93 actual test failures
and 8 runs with passing tests. Gremlins' efficacy is 92.08% **of executed
mutations**, with 18 additional uncovered candidates. The final run took about
19 seconds locally. This qualifies this small package on this toolchain; it is
not a project-wide mutation score or a CI performance budget. Mutations changed
only isolated copies; production code was not changed.

The initial survivors were at `program.go` lines 41, 52, 80 (two operators), 90,
143 (two operators), and 239. The initial uncovered candidates included diagnostic
native encoding, memory accounting, and the operation-budget constant. These
were investigated in the follow-up below. No project-wide mutation threshold is
enabled: the evidence covers this package and the default Gremlins operators.

Local evidence is retained under `.build/testing-mutation/`,
`.build/testing-mutation-boundary/`, and `.build/testing-mutation-final/`:
source snapshots, target-source hashes, native JSON results, and per-invocation
Go JSON logs. The final directory also has `audited-summary.json`. These are
ignored local artifacts, not committed fixtures.

Validation included the ordinary whole Go suite, focused race checks over the
new campaigns and delta tests, and vet on both affected packages. Bounded
20-second fuzz smoke runs found no failures; the valid-edit campaign executed
about 1.57 million inputs. These short runs establish that the harnesses work,
not the absence of bugs or coverage of all schedules. No cloud resources were
created.

## Mutation closure

All **26 initial gaps** are resolved: 25 are caught by meaningful assertions,
and one is equivalent under the current representation invariants. No production
code was changed to improve the score, and no generated files were edited.

| Initial gap | Resolution |
| --- | --- |
| Empty-base span (`41:10`) | Empty recipes retain less storage than a full-base reference. Runtime-layout checks independently bound retained allocations. |
| Instruction-buffer boundary (`52:15`) | A legal exactly-2-MiB native instruction stream must compose and materialize; one byte beyond must request fallback. |
| Output accounting (`80:28`, two mutations) | A roughly 16-KiB malicious instruction stream describes over 4 GiB of output and wraps an unchecked counter back to the declared size. It must be rejected as malformed. |
| Copy coalescing (`90:15`) | 262144 adjacent one-byte copies must retain one small range instead of exceeding the span budget. |
| Copy bounds (`143:18`, two mutations) | An oversized copy over a maximally fragmented source must be rejected as malformed before composition encounters the operation limit. Returning `ErrLimit` would incorrectly request fallback. |
| Wire boundary (`239:20`) | Proven equivalent below: a valid recipe cannot reach 2 MiB of encoded wire data. |
| Native encoder (14 candidates) | Independent byte interpretation covers empty output, multi-byte offsets/lengths, composed spans, split literals, prefixes, and the implicit 65536-byte copy length. A supervised child makes a non-terminating literal loop an explicit test failure and joins/kills the child. |
| Memory accounting (2 candidates) | Tests distinguish caller-owned base storage from owned literals, and compare accounting to actual Go struct sizes and backing capacities. |
| Budget constants (`18:30`, `18:33`) | Independent tests accept 131073 uncoalescible spans and reject 131074. Both constant mutations compile and fail these assertions when replayed through Go overlays. |

The final audited Gremlins run reports **116 killed, 1 lived, 2 not covered**, with
no timeouts, build errors, or incomplete audits. Every kill corresponds to a
real failing test. Its raw efficacy is 99.15%, not 100%. The two constant
candidates have no executable Go coverage counters, so Gremlins leaves them
uncovered even though their behavior is tested. A separate build-and-test replay
kills both. Thus all **119 discovered candidates** have dispositions: **118
test failures and 1 proven equivalent**. This is not an assertion that all
possible mutations or all program behaviors have been explored.

The follow-up tests are in
[program_limits_test.go](internal/gitdelta/program_limits_test.go),
[program_native_test.go](internal/gitdelta/program_native_test.go),
[program_memory_test.go](internal/gitdelta/program_memory_test.go), and the
expanded [fuzz corpus](internal/gitdelta/program_fuzz_test.go).

Repeat the constant mutations independently of Gremlins:

```sh
python3 scripts/check_gitdelta_budget_mutations.py
```

The [runner](scripts/check_gitdelta_budget_mutations.py) first requires a clean
baseline, compiles each exact mutation independently using Go overlays, and
requires the named budget test to fail. Build failures, missing tests, process
errors, and timeouts are never credited as kills. It preserves logs, overlays,
binaries, and JSON evidence in a new `.build/gitdelta-budget-*` directory.

### Proof for the one equivalent wire-boundary mutation

Mutation: `len(dst)-start > 2*MaxSize` becomes `>=`. This changes behavior only
when the newly appended recipe is exactly 2097152 bytes. Caller prefixes do not
contribute because `start` is subtracted.

For a valid `Program`, let `S` be its span count, `L` its literal-span count,
and `N` its output size:

1. `N <= 1048576` and `S <= 131073`. Only `From` and `Compose` construct the
   private representation. Nonempty spans cover positive output lengths.
2. Adjacent literal spans coalesce, so `L <= ceil(S/2)`.
3. The size header costs at most four bytes. A literal span costs at most its
   payload length plus eight bytes (inner and outer tags/lengths).
4. A copy costs at most its output length plus seven bytes. For lengths 1–127,
   its wire record costs at most eight bytes; for lengths 128–16383, at most
   nine; for longer lengths, at most ten. Root offsets fit in three varint bytes.
5. Total wire bytes are therefore at most
   `4 + N + 8*L + 7*(S-L)`, bounded above by
   `4 + 1048576 + 7*131073 + 65537 = 2031628`.

That bound is strictly below 2097152, so equality cannot occur. The original
guard remains as defensive code. No fabricated invalid internal state or
assertion about an error string is used to force this mutant to die.

This proof is tied to `internal/gitdelta/program.go` SHA-256
`7bd5b841554841c0bf299da046df29d93a4d8b3840fc194308b406d7f9513ead`.
Changes to size/span limits, literal coalescing, or wire layout require reviewing
the proof again; it is not a blanket exemption for that line number.

### Closure evidence

The final automatic campaign is in `.build/close-mutations/final/`, including
`closure.json`, `audited-summary.json`, the source snapshot and hashes, raw Gremlins results,
and each actual Go invocation's log. It took about 27 seconds locally. The
larger timeout multiplier lets the native encoder's five-second child deadline
produce a test assertion before Gremlins' external watchdog.

The constant replay is in `.build/gitdelta-budget-lhi_pc6x/report.json`: baseline
passed; division-to-multiplication and addition-to-subtraction both compiled and
failed `TestFragmentedRecipeOperationBudget`. Earlier follow-up attempts remain
in `.build/close-mutations/round1/` and `round2/`; their intermediate survivors
and timeout are not final results.

Final verification: the complete Go suite passed, as did the delta package's
race tests and vet. Both fuzz targets ran for another 20 seconds without a
failure (about 637 thousand raw-input executions and 1.23 million valid-edit
executions). No mutation or test child processes remained after verification.
