# AQ-03 review remediation

Date: 2026-09-10.

## Finding dispositions

These are implementation claims for the next reviewer to verify. No finding was
withdrawn or deferred.

| Finding | Disposition | Change and regression evidence |
|---|---|---|
| F-001 | Fixed | Run deserialization accepts supported early promotion. Snapshot encoding passes its payload through the loader's decoder before publication. Tests cover JSON/postcard round trips, an early-Ready snapshot with a WAL tail, storage restart, and backup/restore. A runtime subscription test exhausts the task's budget to hold its future run Ready through automatic snapshot publication and both original/restored runtime restarts. |
| F-002 | Fixed | The shared reducer rejects cancellation before task creation before changing state or sequence. Target append preparation and recovery also validate the projection image before writing or repairing. Authority and direct-writer tests verify unchanged source bytes and sequence on rejection. Corruption tests inject validly framed invalid cancellation records, with and without snapshots and incomplete tails, and verify that recovery, repair, inspection and backup refuse without changing the source. Cancellation at exactly creation time remains valid. |
| F-003 | Fixed | Attempt-history validation now lives in the common snapshot validator. A canceled run may retain one unfinished historical attempt with no current attempt; other active/history mismatches remain invalid. Tests cover both `RunCanceled` and `RunStateChanged` cancellation during an attempt, snapshot publication, tail replay, and backup/restore, asserting the exact unfinished history, cleared active ID and absent lease. |
| F-004 | Fixed | Descriptor reading, inventory hashing and copying share a regular-file opener that checks file type before open and checks the opened descriptor again. A bounded CLI subprocess test replaces the descriptor and every inventory file with a FIFO, verifies prompt rejection and source preservation, and confirms that no restore destination is created. |

## Related inconsistencies found by verification

The shared projection validation exposed two existing paths that also needed fixes
to preserve the retained persistence behavior:

- Lease release/expiry in `Leased` changed the run to `Ready` without updating its
  state history. Both now use the ordinary state-transition reducer, preserving
  the durable timestamp and effective priority. Tests compare snapshot-plus-tail
  and snapshot hydration for both lease-close record types and assert the history.
- The task-cancel HTTP endpoint used WAL sequence numbers as timestamps. It now
  uses one reading of the daemon clock for the task and its canceled runs. The HTTP
  regression checks the recorded timestamp and repeated-request behavior.

Two older tests assumed every Ready run scheduled after creation was invalid.
The core test now round-trips supported promotion before, at, and after the due
time. The snapshot loader negative test now injects an active attempt outside
Running into a target-format envelope and asserts the specific semantic error.
Direct Ready construction remains guarded by its existing constructor test.

The runtime regression uses the existing custom-event test file so the legacy
subscription API's file footprint remains within the boundary policy. No policy
limits, frozen contracts, planning-package inputs or archived evidence changed.

## Verification

The exit gate passes: target replay, reject-old, backup/restore and projection
equivalence, including the new review regressions.

| Command | Result |
|---|---|
| `cargo test --workspace` | Pass |
| `cargo test --workspace --features workflow` | Pass |
| `cargo test --workspace --features workflow,budget,actor,platform` | Pass |
| `cargo aq-conformance` | Pass |
| `cargo fmt --all -- --check` | Pass |
| `cargo build --workspace` | Pass |
| `conformance/aq-cont-1/cross-feature-persistence.sh` | Pass |
| `git diff --check` | Pass |

Target persistence has 27 passing tests in the base conformance invocation and
29 with all production features. CLI smoke coverage has seven passing tests,
including five FIFO substitutions in the bounded restore test. Stable rustfmt emits the existing nightly-option configuration warnings.

## Operator attention

Snapshot encoding decodes and validates the encoded payload before publication.
The per-append projection-image check introduced here was later consolidated
(finalization, F-028): the mutation authority runs the shared target-event
validation once on its prepared copy, and recovery validates the projection image
once per open. No throughput benchmark is claimed. The existing full-WAL
validation and immutable feature-profile policy remain in place.

This remediation adds no format migration, feature-profile upgrade, or new durable
record family.

## F-005 follow-up: isolate process spawning

The follow-up review of 2026-09-10 verified F-001 through F-004 as resolved and
retained only F-005.

**F-005 disposition: Fixed.** The process-kill lock test and its child helper now
live in the separate `conformance_store_process_lock` test executable. Spawning a
child can briefly inherit descriptors belonging to concurrent tests before exec,
extending lock lifetime past their final session drop. Separating the executables
removes that interaction with the rapid close/reopen loops in
`conformance_target_persistence`.

The moved test preserves its readiness handshake, live-child `StoreInUse`
assertion, kill/wait sequence, and successful recovery at WAL sequence 2. Both
executables are registered in Cargo and the conformance alias and inventory.
Comments and the living acceptance classification document the isolation boundary.
Production locking and the configured four test threads are unchanged.

Verification after the split:

- 50 quiet four-thread repetitions of both executables passed in each of default,
  workflow, and workflow/budget/actor/platform configurations: 150/150 total.
  Each repetition includes all persistence tests and the subprocess lock test.
- All three quiet workspace test configurations passed with the configured four
  threads, as did `cargo aq-conformance`, `cargo build --workspace`,
  `conformance/aq-cont-1/cross-feature-persistence.sh`, formatting and whitespace
  checks. The exit gate remains satisfied.
- Base conformance now runs 25 persistence tests and 2 process-lock tests; the
  full-feature configuration runs 27 persistence tests and 2 process-lock tests.
  The original test cases and assertions remain covered.

The intermittent failure did not reproduce in the 20 pre-change workflow
repetitions run here; the reviewer recorded the earlier failures. The fix follows
the recommended process isolation, with the stress results as supporting evidence.
No findings are deferred.
