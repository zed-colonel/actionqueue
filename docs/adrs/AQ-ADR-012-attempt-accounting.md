# AQ-ADR-012 — Attempt accounting

- **Status:** Proposed. The recommended default below is the working implementation choice
  until code review produces a concrete counterexample (implementation plan, Section 3).
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
| Accepted in PR | _pending_ |
| Accepted on | _pending_ |
| Superseded by | — |
