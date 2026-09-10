# AQ-ADR-009 — Concurrency key while awaiting

- **Status:** Accepted.
- **Decide before:** `AQ-06`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H13
- **Architecture references:** §8.6, §18.7, §29.10 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

A run that waits while holding its concurrency key can block a key indefinitely,
especially for long external callbacks.

## Recommended decision

Default `ReleaseWhileAwaiting`. `HoldWhileAwaiting` is an explicit per-task
constraint (`ConcurrencyKeyWaitPolicy`). On wake, a released key is re-acquired through ordinary
eligibility before the run becomes `Running`.

## Alternatives considered

Always hold (rejected: §29.10); always release (rejected: some workloads need
strict exclusivity across the wait).

## Consequences

Wake promotion may be delayed by key contention; inspection highlights long-held
keys.

## Verification required

Key released on `Awaiting` by default; held under explicit policy; resumed run
re-acquires in order; suspended and awaiting runs do not deadlock the key.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | `AQ-02` |
| Accepted on | 2026-09-09 |
| Superseded by | — |
| Deferred verification | Verified in `AQ-02`: the default is `ReleaseWhileAwaiting`; engine evaluator tests cover release on `Awaiting` by default, retention under `HoldWhileAwaiting`, and release of a held key when the awaiting run terminates (`crates/actionqueue-engine/src/concurrency/lifecycle.rs`). Dispatch currently uses an accessor that always returns `ReleaseWhileAwaiting`. Deferred to `AQ-03`: persisted per-task policy selection. Deferred to `AQ-06`: dispatch verification of both wait policies under live continuation, resumed key reacquisition in order, and freedom from key deadlocks for suspended and awaiting runs. |
