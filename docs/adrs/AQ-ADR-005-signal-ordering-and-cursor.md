# AQ-ADR-005 — Signal ordering and cursor

- **Status:** Proposed. The recommended default below is the working implementation choice
  until code review produces a concrete counterexample (implementation plan, Section 3).
- **Decide before:** `AQ-05`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H5
- **Architecture references:** §9.6, §9.8, §10.3, §10.6 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

A wait may be established before or after a matching signal arrives. Without a
durable ordering, early signals are lost or duplicated across restart.

## Recommended decision

Assign a monotonic per-store `SignalSequence` at admission. Every `WaitSpec` carries a
lower-bound cursor; under `FirstMatch` the lowest eligible sequence at or above the cursor wins.
The WAL commit order of the wait and the signal is the sole tie-breaker.

## Alternatives considered

Wall-clock timestamps (rejected: not monotonic across restart); per-namespace
sequences (deferred: adds index complexity without a first-release need).

## Consequences

Signals are fan-out observations, not consumed queue items (§9.7). Cursor
semantics are part of the durable record model.

## Verification required

No-lost-wakeup matrix (§28.4): signal-before-wait, wait-before-signal, duplicate
signal, restart between each step.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | _pending_ |
| Accepted on | _pending_ |
| Superseded by | — |
