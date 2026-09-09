# Crash scenarios frozen at the pre-AQ-CONT-1 baseline

Sources live verbatim at tag `actionqueue/pre-aq-cont-1` (commit `97c9dc26`). `catalogue.json` pins their SHA-256 so that later pull requests can show which crash behaviours were retained and re-proved, replaced, or rejected. Nothing here is loaded by target crates.

| Source at baseline | Feature | Tests | Scenario |
|---|---|---:|---|
| `tests/acceptance/crash_recovery.rs` | `core` | 5 | Deterministic crash points around attempt start/finish with HTTP/metrics parity after restart |
| `tests/acceptance/crash_during_promotion.rs` | `core` | 1 | Promoted Ready runs survive a crash and are not re-promoted |
| `tests/acceptance/wal_corruption_recovery.rs` | `core` | 7 | Trailing WAL corruption variants (partial header, partial payload, garbage, bit flip) under truncate-partial and strict repair policies |
| `tests/acceptance/snapshot_corruption_recovery.rs` | `core` | 3 | Corrupt, truncated, or missing snapshot falls back to WAL-only replay |
| `tests/acceptance/lease_expiry.rs` | `core` | 5 | Lease expiry releases the run and re-eligibility survives restart |
| `tests/chaos/kill_recovery.rs` | `core` | 7 | Simulated kill -9 (std::mem::forget) at task creation, state transitions, mixed terminal/active runs, sequential crashes, sequence monotonicity, retry wait, high volume |
| `tests/acceptance/workflow_crash_recovery.rs` | `workflow` | 2 | DAG, hierarchy, and cron state survive WAL recovery |
| `tests/acceptance/dag_snapshot_recovery.rs` | `workflow` | 1 | Dependency declarations survive snapshot-plus-tail recovery |
| `tests/acceptance/budget_recovery.rs` | `budget` | 2 | Budget allocations and consumption survive recovery |
| `tests/acceptance/ledger_recovery.rs` | `platform` | 2 | Ledger entries survive recovery |
| `tests/acceptance/remote_actor_crash.rs` | `actor` | 2 | Remote actor heartbeat loss is detected and work is recovered |

## Disposition under AQ-CONT-1

- **Retain and re-prove:** WAL-first authority, deterministic replay, snapshot-as-acceleration, trailing-corruption repair, lease expiry, promotion durability, sequence monotonicity.
- **Replace:** crash points that assume `HandlerOutput`, `Suspended`-as-waiting, or fire-and-forget child submission are re-expressed against `AttemptDisposition`, `Awaiting`, and compound child admission (`AQ-06`–`AQ-09`).
- **Reject:** any scenario whose expected outcome depends on automatic opening of a pre-contract store or on transient custom events surviving restart.
