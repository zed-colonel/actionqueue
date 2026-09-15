# AQ-ADR-012 — Attempt accounting

- **Status:** Accepted; implemented and covered by the AQ-08 acceptance suites.
- **Decide before:** `AQ-08`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H13
- **Architecture references:** §8.4, §12.6, §29.9 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

`Awaiting` and `Suspended` cause additional physical attempts that are not failures.
Counting them against the retry cap would punish long-running work; not counting them at all
would hide lineage.

## Recommended decision

Track the physical attempt ordinal and the failure-attempt count separately.
`Awaiting` and `Suspended` transitions consume no retry allowance. Retry caps apply to the
failure count only. Both values are durable and inspectable.

## Alternatives considered

Single attempt counter (rejected: conflates recovery with failure); resetting the
counter on resume (rejected: loses lineage).

## Consequences

Developmental profile rule that a retry is not a sample (`AQ-DD-011`) is
representable without any campaign type: the higher layer reads physical and failure counts.

## Verification required

Accounting tests across await/resume/suspend/crash sequences; `AQ-DD-011`.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | AQ-08 work item |
| Accepted on | 2026-09-12 |
| Superseded by | — |

Accepted starts increment `attempt_count`. Committed failure, timeout, terminal failure, and interrupted accepted execution increment `failure_attempt_count` exactly once. Completion, awaiting, suspension, cancellation, and recovery before acceptance do not. Retry caps and backoff use failures. The AQ-08 schema-6 milestone introduced both counters; current schema-9 snapshots retain them. WAL replay uses the same accounting rules.
