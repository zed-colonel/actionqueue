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
