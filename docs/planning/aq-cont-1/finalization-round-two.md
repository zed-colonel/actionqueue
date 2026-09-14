# AQ-CONT-1 plan-wide finalization, polish round 2

Plan version: `3d8a857d-25fe-4202-accf-bb133b5453cb`.
Candidate baseline: `48b84834a690d57dee30b564f2e745d8c856b422`; destination `main`.
Round one and the wait-capacity follow-up are recorded in
[finalization-polish.md](finalization-polish.md) and
[finalization-wait-capacity.md](finalization-wait-capacity.md). This round
implements the ten findings the independent review left open (F-027 through
F-036). Promotion remains operator-owned; these are implementation claims for
independent review.

## Semantic decision recorded this round

Completed work is immutable history (operator decision behind F-036). A cancel
whose target task already succeeded or failed, or whose target run is already
terminal, is rejected with `WaitRejection::AlreadyTerminal` in the shared
validation that live preparation and replay both run; nothing is appended and
`task_terminal_status` is unchanged. Dependency and descendant cascades reach
only unfinished tasks, so a satisfied prerequisite never cascades cancellation.
Unfinished dependents remain cancelable directly, and repeating a cancel of an
already-canceled target is an append-free acknowledgement. The daemon returns
409 `already_terminal`; the CLI passes the code through. See
[aq-06-continuations.md](../../aq-06-continuations.md) and
[aq-12-apis.md](../../aq-12-apis.md).

## Finding dispositions

| Finding | Disposition and implementation | Focused evidence |
|---|---|---|
| F-027 | Fixed. Maintenance publishes only when the durable revision moved; an idle pass copies nothing and computes no digest. Mutation attempts keep the equal-revision digest tripwire and `projection_mismatch` telemetry. Inspection reads the authoritative projection in place under the guard instead of cloning it. | `acceptance_structural_inspection::idle_daemon_maintenance_performs_no_projection_image_or_digest_work`; daemon `same_revision_projection_divergence_fails_closed_and_counts_once`. |
| F-028 | Fixed. `WalFsWriter` owns framing and durability only; session and daemon bootstrap build it from their completed recovery, so a writable open replays the WAL once. The authority's prepared copy runs `validate_target_event` once before append, the same check replay performs. The benchmark counts one indexed candidate removal per match. | `conformance_target_persistence::authority_preparation_validates_target_events_like_replay`; storage, conformance and benchmark gates. |
| F-029 | Fixed. One dependency-failure cascade: the task-level `waits::recover_cancellations` that bootstrap and every tick already ran; the run-level copies, `RetryDecision`, `DispatchError::{RetryDecision,StateInconsistency}`, the redundant budget re-check, and the uncalled `claim_remote` wrapper are deleted. Dispatch consults the engine's ADR-009 evaluator. `in_flight` ownership is released immediately after a successful commit. | `dispatch::dependency_failure_cascade_releases_suspended_key`; executor-local outcome tests assert `DispositionOutcome::accounting`; full feature matrix. |
| F-030 | Fixed. One `pub(crate)` canonical byte encoder serves admission, signal and disposition bytes; `AdmissionDigest::new` produces the current version; canonical digests are infallible; each admission is bounds-checked once at request construction and digested once. Byte output and stored digests are unchanged. | Independent v1/v2 admission, signal and disposition vector tests; frozen fixture hashes. |
| F-031 | Fixed. Bootstrap always constructs router state with the control authority, expressed through the plain initializer; the dead `control_enabled` re-checks are gone because mutating routes and the auth layer exist only when control is enabled. Engine pause/resume are `ControlOperation` variants executed through the shared service, so they are authorized once and published through the tripwire path. Actor routes share one helper. | Daemon control, actor and tenant tests; full-driver conformance. |
| F-032 | Fixed. Package revision 16 lists only executed evidence: superseded projection vectors v1–v7, `coverage.json`, `coverage-v2.json` and the unread workload files are removed; proof entries name only `projection-v9-vector.json`; the drivers README defers to the package README. Retained fixture and normative hashes are unchanged. | `release-tests`, `cargo aq-conformance`, full `aq_conformance` report at revision 16. |
| F-033 | Fixed. Living documentation describes the released target: stale validation narrative, dangling `v0.1` references, "capability routing", the charter version, the README snapshot sentence and controller run journaling are corrected or removed. Hash-pinned documents are untouched. | `check-docs.py`, rustdoc with warnings denied, frozen-document checks. |
| F-034 | Fixed. Snapshots emit runs in per-task index order so hydration rebuilds `runs_by_task` exactly as WAL replay did; cascades emit records in the same order on every replica. | `conformance_target_persistence::snapshot_hydration_preserves_per_task_run_order` (eight runs whose creation order disagrees with identifier order). |
| F-035 | Fixed. Resume wakes use a `(run, sequence)` resolved-wait index; accepted starts and stale-attempt checks use the attempt-owner index; the broad-wait gauge is maintained incrementally. Remote claims are asserted to carry the admission's tenant and causal context, including a platform tenant. | `acceptance_remote_protocol`, daemon tenant tests, storage suites, benchmark bounds. |
| F-036 | Fixed as described above. | `acceptance_parent_wait_child_atomicity::completed_work_is_immutable_history_and_never_cascades`; `exhausted_zero_run_cron_task_is_terminal_and_cannot_be_canceled`; replayed fixtures unchanged. |

## Simplification after the findings

Committed cancellations are indexed by target, so cancel preparation, the
terminal guard, historical task status and control-sequence inspection look a
target up instead of scanning cancellation history. Engine pause and resume share
one control path parameterised by direction. Actor-scoped remote routes settle
the remote scheduler before and after every operation rather than deciding from
the response shape. No wire, fixture or snapshot byte changed.

## Verification

The release gate set (`scripts/release.py::gates()`: eight-profile test, clippy
and production builds, stable and nightly formatting, serde and
no-default-feature suites, conformance and developmental runners, the full
public-driver report, cross-feature persistence, benchmarks, release,
documentation, consumer and package checks) passed on the round-two
implementation commit. Evidence is retained in the controller run bundle, not
in source.
