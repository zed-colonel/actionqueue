# AQ-PERF-1 Implementation Plan

*Mutation and recovery cost bounded by the change, not by retained history*

**Repository:** `zed-colonel/actionqueue` · **Baseline:** `29e096c` (0.2.0) · **Target:** 0.3.0 ·
**Integration branch:** `aq-perf-1` · **Normative contract change:** none (ADRs only) ·
**Date:** October 17, 2026

## 0. Purpose

WorldInterface's WIP-07 measured ActionQueue 0.2.0 at its supported scale, with one tenant and
one boundary node per run:
- submitting each batch of 1,000 admissions slowed from 10.9 s to about 44 s as retained runs
  grew to 10,000, so filling a tenant is quadratic;
- cancellation behaved the same way;
- bootstrap at 10,000 retained runs took 405 s, on every process restart.

The 0.2.0 release notes already say so: "Mutation preparation clones the full projection;
recovery verifies the complete retained WAL. Compaction is absent" (`docs/releases/0.2.0.md:22`).

This plan makes per-mutation cost follow the change and bootstrap follow the WAL tail, while
keeping ActionQueue's durable-execution invariants. It designs, but does not ship, retention of
terminal history. All of it ships as one 0.3.0 release, once every item has landed. The operator
runs `release.py verify` and the crates.io publish outside CraftingTable.

## 1. Baseline facts (0.2.0)

Paths are relative to `crates/actionqueue-storage/src` unless named.

**Per-mutation copies:**
- *The authority* copies the whole projection before every event (`mutation/authority.rs:1893`),
  applies the event to the copy, appends it to the WAL, then publishes the copy.
- *The reducer* (`ReplayReducer`, `recovery/reducer.rs:291`) copies itself again inside compound
  events, which also makes replay quadratic: admission (`reducer.rs:753`), disposition
  (`recovery/dispositions.rs:62`), and wait established, resolved and cancelled (`recovery/waits.rs:649`,
  `678`, `692`), plus a scratch copy in `mutation/disposition.rs:210`.
- *The runtime* rebuilds its dependency gate and hierarchy tracker over all tasks after each created
  admission (`actionqueue-runtime/src/dispatch.rs:1773-1782`).
- *The daemon* publishes a fresh full copy after each mutation (`actionqueue-daemon/src/http/mod.rs:302-318`).
  For an unchanged sequence it computes two full digests instead.
- *Per-event scans:* `subscription_observations()` runs before and after every event; there are
  linear `key_reservations` and `ledger_entries` checks.

**Checks are interleaved with writes.** `validate_target_event` (`recovery/validation.rs:5`) pre-checks
only some event types. Elsewhere checks and writes interleave (`apply_run_state_changed`,
`apply_run_canceled`, `apply_attempt_finished`), and the copies are what make compound events atomic.
The crash-atomicity suites (`tests/acceptance/compound_admission_crash.rs:65-67` and others) assert that
a fenced authority's image equals the pre-mutation image at each fault point.

**Recovery replays everything.** `recover()` (`recovery/bootstrap.rs:77-163`) replays the complete WAL
even when a snapshot exists, applies the tail to the hydrated snapshot too, and compares digests.
Publishing a snapshot replays the WAL prefix from sequence 1 (`validate_snapshot_for_session`,
`bootstrap.rs:227`). There are three more points:
- A snapshot carries a CRC and a self-digest, but nothing binds it to the WAL's contents.
- The operation-history index is not in the snapshot; hydration marks it unavailable.
- Only the runtime publishes snapshots, counting only events inside a tick (`dispatch.rs:1511-1543`).

**Retention.** Terminal runs, tasks and admissions stay in the projection forever.
`trim_terminal_history` (`reducer.rs:1687`) exists but only tests call it. Signals have logical
retirement (`SignalsRetired`).

## 2. Constraints

- **The frozen contracts stay frozen.** The texts under `docs/contracts/` are hash-pinned
  (`SHA256SUMS`, checked by `tests/conformance/frozen_evidence.rs`). Recovery and retention changes
  are recorded as ADRs (next: ADR-019), and the non-frozen docs that state today's behaviour are
  amended. The invariant "recovery MUST be reconstructible from snapshot plus WAL tail, or WAL
  alone" (`docs/invariant-boundaries-v1.0.md:37`) already allows AQ-P3.
- **Atomicity and the crash suites are unchanged.** Every crash-atomicity acceptance suite passes
  without edits to its assertions. A validation failure leaves the projection untouched without
  fencing, as today.
- **Digests and formats.** Projection digests stay canonical and deterministic. AQ-P2 changes no WAL,
  snapshot or digest format; AQ-P3 may change the snapshot schema, with an upgrade path.
- **No product fault hooks** beyond the existing `store::fault` checkpoints.
- **Checks.** `.craftingtable/checks.json` is committed with this plan, and the operator adopts it
  before AQ-P1:
  - `fmt`: `cargo fmt --all -- --check`;
  - `clippy`: workspace, all targets, features `workflow,budget,actor,platform`, `-D warnings`;
  - `test-default`: the workspace's default features;
  - `test-all-features`: the same four features.

  Every check is `--locked`, and the tests run with `--test-threads=1`, as in CI. Measured on 0.2.0
  in a clean clone on a busy workstation (2026-10-17), all within CraftingTable's 30-minute check
  limit, so none is split:
  - fmt 2 s and clippy 23 s;
  - a cold all-features test build 637 s, then the all-features suite 169 s (1,164 tests);
  - the default suite 154 s, including its build.

  The full seven-profile matrix stays a release gate in `release.py`, run by the operator.

## 3. Items

### AQ-P1: the scale benchmark
- **The benchmark.** Add `benches/retained_history.rs`, custom with `harness = false` like
  `benches/continuation.rs`. It builds N retained terminal runs, then measures:
  - the time for the next 100 admissions and the next 100 cancellations;
  - full-projection copies and reducer visits, through new counters beside `recovery/work.rs`'s;
  - bootstrap time, WAL-only and from snapshot plus tail.
- **Policy.** Timing is informational; structural counts are asserted. Record the 0.2.0 baseline at
  1,000 and 10,000. Do not attempt 100,000 at 0.2.0 (quadratic), but the benchmark accepts it for
  later items.

### AQ-P2: mutation cost independent of retained history
- **Approach, decided in the design step, with a spike measured on the benchmark:**
  - (a) persistent maps with structural sharing (e.g. `imbl`) for the projection's growing
    collections, so a copy costs O(log n) and the prepare-then-publish shape, the internal atomicity
    copies and the crash suites stay as they are;
  - (b) a complete read-only check for every event type, then an infallible apply in place (a deep
    rewrite of the interleaved reducers);
  - (c) an undo journal.

  (a) is the recommended starting point, because it changes the least. The ADR records the choice,
  the measured constant factors and the determinism of iteration and digests.
- **Also in scope:**
  - make the runtime's per-admission rebuilds incremental;
  - make the daemon publish cheaply (structural sharing makes its copy cheap too);
  - index the per-event linear scans.
- **Acceptance:** the exit gate's 2× bound at 100,000 runs, no full copy per mutation, and every crash
  suite unchanged.

### AQ-P3: bounded bootstrap
- **First, measure again.** The reducer's internal copies are what make replay quadratic, so AQ-P2
  alone may bring bootstrap at 10,000 runs from minutes to seconds. The design scopes what remains
  from that measurement.
- **Then, as needed:**
  - Bind each snapshot to its WAL prefix, by a prefix digest or a frame hash chain recorded in the
    envelope, and publish snapshots from the live projection without replaying the prefix.
  - Bootstrap from the verified snapshot plus the tail.
  - Persist the operation-history index, or report it unavailable as `docs/aq-12-apis.md:70` allows.
  - Count admissions made between ticks toward the snapshot threshold, and decide whether the daemon
    publishes snapshots.
  - Move full verification (WAL-only replay compared with the snapshot) into an explicit
    `store verify` command and into backup verification.
- **Docs to amend:**
  - `docs/wal-recovery-guide.md:20` and `:32`;
  - `docs/data-dir-format-v1.0.md:29-30`;
  - `crates/actionqueue-storage/README.md:21-27`;
  - `docs/aq-06-continuations.md:76-77`;
  - `docs/aq-12-apis.md:70`;
  - ADR-001 and ADR-006, by a superseding ADR.

  The WAL itself stays complete; nothing here truncates it.

### AQ-P4: retained terminal history, design only
An ADR for keeping terminal runs out of the live projection, as an archive or by compaction.
- **Bounds:** the preservation rules of the hardening contract §21.5 (active work, admission keys for
  the retry horizon, causal records, audit attribution).
- **It must settle:** how `trim_terminal_history`'s digest effect and the WAL-retention rules apply,
  and what scoped follow-up plan carries it. Nothing in this plan ships it.

### AQ-P5: the 0.3.0 release candidate
- **Release notes.** Bump the workspace to 0.3.0 and write `docs/releases/0.3.0.md`: the measured
  budgets before and after, recovery behaviour, any snapshot schema change and its upgrade, and the
  verify command.
- **Prepare.** `release.py prepare` passes on the candidate.
- **Release.** After merge, the operator runs `release.py verify` (about 73 minutes) and
  `crate-release publish --verified`. WorldInterface bumps its pin in its own item.

## 4. Order and review

The items are sequential. AQ-P4 may run beside AQ-P3. Each item starts with a design step that brings
its decisions to the operator before any build. Each build gets an independent review and a mutation
round over the changed reducer and recovery code, with the WIP-07 measurements as the motivating
evidence.
