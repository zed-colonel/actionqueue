# AQ-ADR-008 — Wait timeout race

- **Status:** Proposed. The recommended default below is the working implementation choice
  until code review produces a concrete counterexample (implementation plan, Section 3).
- **Decide before:** `AQ-06`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H5
- **Architecture references:** §10.9, §18.5, §28.4, §32.7 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

A signal and a deadline can resolve the same wait concurrently. Two resolutions would
resume a run twice or leave it in an inconsistent state.

## Recommended decision

WAL sequence order determines the winner. The first committed resolution
(`WaitSatisfied`, `WaitTimedOut`, or `WaitCanceled`) is final; later attempts to resolve the same
wait are rejected before append and are not replayed.

## Alternatives considered

Prefer signal over timeout regardless of order (rejected: non-deterministic
under replay); prefer timeout (rejected: loses a durable wake).

## Consequences

Timeout handling is an ordinary validated mutation. Recovery re-evaluates only
unresolved waits.

## Verification required

Race matrix with crash points between evaluation and commit; replay reproduces the
committed winner exactly.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | _pending_ |
| Accepted on | _pending_ |
| Superseded by | — |
