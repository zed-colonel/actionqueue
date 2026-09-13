# Finalization remediation: F-020 and F-022

Plan version: `3d8a857d-25fe-4202-accf-bb133b5453cb`.
Reviewed candidate: `3417e630855a18793dfe27c981389e04d176bf27`.
Integration snapshot: `48b84834a690d57dee30b564f2e745d8c856b422`.
Destination: `main` at `97c9dc26c19c697dbfb204ed503e82c5f053394f`.

The complete supplied plan bundle, fourteen-item inventory and handoff were
reconciled. The formatting retry was already completed. The latest independent
review supersedes the earlier implementation claims: F-017, F-018, F-019 and
F-021 are verified resolved; F-020 remains partially open and F-022 identifies a
new runtime interaction. All 118 finding IDs remain intact; the other 115 resolved
and one withdrawn dispositions are unchanged, including the operator corrections
and withdrawn optional refactor. This record makes implementation claims for the
next reviewer; it does not independently close findings.

Initial Git status, including all untracked files, was clean. The source-run
journal records verification output only in controller scratch, so there were no
worktree artifacts to remove or unrelated files to stage. The complete candidate
was compared with `main`, including the previously integrated implementation.

## Finding dispositions

- **F-020: fixed.** `EnsureTaskRequest::digest` now documents canonical v2, matching
  its implementation and the previously corrected living ADRs. Historical v1
  encoding documentation and all hash-pinned documents remain intact.
- **F-022: fixed.** Local `WaitCapacity` rejection commits the existing minimal
  terminal-failure disposition with bounded code/message `wait_capacity`. The
  minimum framed-size calculation includes that rejection. The normal durable
  completion path removes worker tracking and releases the lease and concurrency
  key. The rejected checkpoint, wait, children, signals and consumption never
  commit. Remote rejection still preserves the active attempt for retry under its
  accepted fence. Stale ownership and uncertain writes retain their existing
  failure handling.

## Focused proof

`acceptance_attempt_disposition` adds real-handler cases for both zero and
occupied positive quotas, independently limiting the store and tenant namespace.
The same cases run in explicitly provisioned platform tenants. One worker slot
and a shared concurrency key force queued work to depend on closure of the
rejected worker; the rejected task uses `HoldWhileAwaiting`. Tests assert one
failed physical attempt, no retry, a finished durable failure with the specific
code, no subordinate effects, queued completion and no key/lease reservation.
They run at `RuntimeConfig::minimum_disposition_bytes()` and advance ten more
ticks, asserting an unchanged projection rather than renewed abandoned leases.
Independent WAL replay, snapshot hydration and runtime restart preserve history
and do not redispatch completed work.

`acceptance_remote_protocol::remote_wait_capacity_rejection_preserves_attempt_for_retry`
checks append-free remote rejection, active ownership, WAL/snapshot recovery,
retry after capacity increases and append-free duplicate acknowledgement.

The local regression was also executed with only the new fallback arm removed:
it failed with `Dispatch(Authority(Disposition(WaitCapacity)))`. Restoring the
fix made the focused cases pass. An initial test setup used the shared helper's
repeat policy; the fixture now explicitly uses `Once`, matching the intended
single-occurrence liveness assertion.

## Plan-wide conformance assessment

The assessment includes the combined candidate, not just this remediation.
Required command evidence is recorded below; previous green reports alone do not
establish this candidate's conformance.

| Item | In-scope obligation and evidence |
|---|---|
| AQ-01 | Frozen hashes, archive completeness, tracked-file scans, ratcheted boundaries and neutral vocabulary: frozen-evidence and boundary conformance suites. |
| AQ-02 | Bounded values, validating serde, exhaustive transitions, opaque causal refs and routing/authority separation: core default/serde/no-default tests and boundary checks. |
| AQ-03 | Manifest-before-decode, fresh lineage, ownership, replay/snapshot equality, corruption, backup/restore and feature compatibility: persistence, process-lock, recovery and cross-feature tests. |
| AQ-04 | Compound-only admission, canonical conflicts, complete initial runs/edges/attribution, bounds and crash/race recovery: admission proof binaries, including F-018 regressions and constrained cron replenishment. |
| AQ-05 | Durable signal identity, ordering, deduplication, tenant matching and retention: canonical, admission, retention and signal crash suites. |
| AQ-06 | Durable waits, race-free matching, deadlines/cancellation, lease/key policy and aggregate quotas: wait and checkpoint suites plus F-022 local closure under capacity rejection. |
| AQ-07 | Immutable checkpoint/hash and assigned resume lineage through recovery: checkpoint/resume tests; F-022 proves a rejected checkpoint cannot leak into history. |
| AQ-08 | Atomic disposition effects/accounting, stale fences, interrupted workers and bounded rejection closure: disposition/crash suites and the new real-handler quota regression. |
| AQ-09 | Transactional child causality, DAG/hierarchy gates, parent waits and cascades: child/parent/coordinator suites; F-022 also rejects proposed children atomically. |
| AQ-10 | Budget suspension versus awaiting, durable accounting and internal-reactivity separation: budget/subscription suites and feature matrix; rejected quota proposals charge no consumption. |
| AQ-11 | Authenticated tenant controls, attribution, actor liveness, remote fences and scheduling parity: actor/control/remote/cron suites, priority HTTP regression, and preserved remote quota retry semantics. |
| AQ-12 | Target operational schemas, pagination, auth, redaction, telemetry, CLI/daemon lifecycle and accurate API guidance: surface tests/builds, docs/rustdoc checks and F-020. |
| AQ-13 | All twenty invariants, eighteen developmental cases, model/chaos cuts, four public drivers and indexed-work bounds: standalone/full conformance, developmental, persistence and benchmark gates. Revision 15 adds the quota-liveness coverage to existing proof binaries. |
| AQ-14 | Consistent eleven-crate versions, real packages/consumer, source metadata, API docs and release-report validation: release gates. Publication, tagging, integration and external WorldInterface/Exoskeleton certification remain operator/downstream actions. |

The new cross-item proof connects AQ-06 capacity enforcement to AQ-08 local
worker completion, AQ-07/AQ-09 subordinate-effect atomicity, AQ-10 accounting,
and AQ-11 remote retry behavior. Existing priority/admission fixes and neutrality
remain within the required matrix. There is no change to frozen contract,
persistence formats, scheduling policy or downstream authority ownership.

## Simplification and scope

The fix reuses the established bounded terminal-failure path rather than adding a
new retry state or separate cleanup path. Tenant and store cases share one focused
fixture. No additional optional refactor is needed for these findings. Package
revision 15 distinguishes the added executable coverage; no frozen fixture bytes
or normative acceptance criteria changed.

## Verification

Verification uses Rust 1.89.0 and a reused copy of the existing Cargo cache in
controller scratch. Test stores, logs and generated package artifacts also stay
in scratch. Loopback tests require sandbox escalation: a socket capability probe
returned `Operation not permitted` under the default sandbox.

Final command results will be recorded here after the full matrix completes.
