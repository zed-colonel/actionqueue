# AQ-ADR-008 — Wait timeout race

- **Status:** Accepted.
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
| Accepted in PR | `AQ-06` |
| Accepted on | 2026-09-11 |
| Superseded by | — |

## AQ-06 algorithm and evidence

Live mutations serialize through the storage authority. Identical retries return the
original sequence; conflicting resolutions fail before append. An elapsed deadline
does not invalidate a signal candidate. Recovery reconciles retained matches in
`(signal_sequence, wait_id)` order, then due deadlines in `(deadline_at, wait_id)`
order. Exact and broad waiters may both observe one signal; neither consumes it.

`tests/acceptance/waits.rs` tests every pairing of signal, timeout, explicit wake and
wait cancellation in both commit orders, plus restart prefixes and storage failure
points. `tests/acceptance/wait_crash.rs` supplies process-kill evidence. See
[the continuation handoff](../aq-06-continuations.md) for the AQ-07 delivery boundary.
