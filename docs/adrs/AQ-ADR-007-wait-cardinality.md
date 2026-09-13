# AQ-ADR-007 — Wait cardinality

- **Status:** Accepted.
- **Decide before:** `AQ-06`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H4, AQ-H5
- **Architecture references:** §8.2, §10.3, §10.10, §29.2, §29.14 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

Multiple concurrent waits per run multiply state-machine, snapshot, and inspection
complexity and make wake-up ordering ambiguous.

## Recommended decision

Exactly one active wait per run. A `WaitSpec` may contain more than one condition only
where the combined semantics remain deterministic (first-match among a bounded set). A run in
`Awaiting` has exactly one `WaitId`.

## Alternatives considered

N active waits with join semantics (deferred until workloads demonstrate need;
applications compose through child tasks and DAG gates instead).

## Consequences

Simpler recovery algorithm (§10.11). Fan-in is expressed through child completion
filters and DAG dependencies, not through multiple waits.

## Verification required

Rejection of a second wait registration for a run; property test that an `Awaiting`
run always has exactly one active wait after replay.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | `AQ-02` |
| Accepted on | 2026-09-09 |
| Superseded by | — |
| Deferred verification | Verified in `AQ-02`: `WaitSpec` and `ResumeContext` carry exactly one `WaitId`, and resume identifies its wait for every wake kind (`continuation_vocabulary.rs`). Deferred to `AQ-06`: rejection of a second wait registration for a run, and the replay property that an `Awaiting` run has exactly one active wait. |

## Aggregate creation quotas (F-019)

Plan §11.4 and architecture §25.8 also require bounded aggregate active waits.
`ContinuationLimits` defaults to 100,000 active waits per store and 10,000 per
tenant namespace. Hosts configure `active_waits` and `active_waits_per_tenant`
through `RuntimeConfig::continuation_limits` or the storage authority setter.
Zero stops new waits. Each count is an indexed projection of unresolved waits;
child and signal waits consume the same capacity. Single-tenant stores use the
`None` namespace.

Both standalone establishment and compound dispositions check capacity before
append. Rejection commits no checkpoint, children, signals, or consumption.
Resolution (including cancellation and deadlines) releases capacity. Pending
resume context and retained history do not count as active waits. WAL replay and
snapshot hydration reconstruct counts without applying creation quotas: lowering
limits never discards accepted waits or prevents their resolution, and exact
establishment retries remain append-free.

`acceptance_checkpoint_resume` and `acceptance_attempt_disposition` cover store
and tenant rejection, atomicity, release, duplicate retries, and WAL/snapshot
recovery. This completes the deferred AQ-06 verification alongside
`acceptance_waits` and `acceptance_wait_crash`.
