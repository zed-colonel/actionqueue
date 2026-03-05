# Policy Defaults Reference

## Purpose

This document captures shipped default policy behavior for ActionQueue so operators and integrators can reason from implementation truth.

## 1) Run policy defaults

### Supported policy families

- `Once`
- `Repeat { count, interval_secs }`
- `Cron { expression }` (requires `workflow` feature)

Validation rules:

- `Repeat.count >= 1`
- `Repeat.interval_secs >= 1`
- `Cron.max_occurrences >= 1` (rejects 0, matching RepeatPolicy)

Reference surface:

- `crates/actionqueue-core/src/task/run_policy.rs`

## 2) Task constraint defaults

Default `TaskConstraints` values:

- `max_attempts = 1`
- `timeout_secs = None`
- `concurrency_key = None`
- `concurrency_key_hold_policy = HoldDuringRetry` (default)
- `safety_level = Pure` (default)
- `required_capabilities = None`

Reference surface:

- `crates/actionqueue-core/src/task/constraints.rs`

## 3) Selection default

Default ready-run selection order is **Priority then FIFO then RunId tie-break**:

1. higher `priority_snapshot` first,
2. earlier `created_at` first,
3. deterministic `RunId` ordering for full ties.

Reference surface:

- `crates/actionqueue-engine/src/selection/default_selector.rs`

## 4) Scheduled promotion default

Scheduled runs are eligible for promotion when:

- `scheduled_at <= current_time`.

Runs with `scheduled_at > current_time` remain scheduled.

Reference surface:

- `crates/actionqueue-engine/src/scheduler/promotion.rs`

## 5) Retry decision default

Retries are attempt-scoped and bounded by `max_attempts`.

Decision mapping:

- `Success` -> `Completed`
- `TerminalFailure` -> `Failed`
- `Suspended` -> `Suspended` (does not count toward max_attempts)
- `RetryableFailure` or `Timeout`:
  - if `attempt_number < max_attempts` -> `RetryWait`
  - else -> `Failed`

Invariant checks reject:

- `max_attempts == 0`
- `attempt_number == 0`
- `attempt_number > max_attempts` (explicit no `N+1` path)

Reference surface:

- `crates/actionqueue-executor-local/src/retry.rs`

## 6) Budget defaults (requires `budget` feature)

- Budget dimensions: `Token`, `CostCents`, `TimeSecs`
- No budget allocated by default (dispatch proceeds without budget gate)
- Budget exhaustion blocks dispatch until replenishment
- Suspended attempts do not count toward `max_attempts`

Reference surface:

- `crates/actionqueue-budget/src/tracker.rs`
- `crates/actionqueue-budget/src/gate.rs`

## 7) Actor defaults (requires `actor` feature)

- Heartbeat timeout multiplier: 3x interval (default)
- No actors registered by default
- Capability matching: actor must have ALL required capabilities (strict intersection)

Reference surface:

- `crates/actionqueue-actor/src/heartbeat.rs`
- `crates/actionqueue-actor/src/routing.rs`

## 8) CLI surface defaults

### Data directory default

CLI commands resolve default data root to literal path:

- `~/.actionqueue/data`

Reference surface:

- `crates/actionqueue-cli/src/cmd/mod.rs`

### Daemon command defaults

Default daemon config values:

- `bind_address = 127.0.0.1:8787`
- `metrics_bind = Some(127.0.0.1:9090)`
- `enable_control = false`
- `data_dir = ~/.actionqueue/data`

Reference surface:

- `crates/actionqueue-daemon/src/config.rs`

## 9) Downtime / misfire behavior

ActionQueue ships **eager catch-up** as its misfire policy. When a scheduler tick occurs after one or more runs are past their `scheduled_at` time, all overdue runs are promoted to Ready in a single tick. No runs are skipped and no coalescing occurs.

Current shipped behavior:

- Deterministic scheduled-to-ready promotion based on `scheduled_at <= now`.
- All overdue runs are promoted eagerly; there is no configurable skip or coalesce policy.
- Deterministic run derivation for Once/Repeat/Cron.

This behavior is tested by the `misfire` acceptance test (`tests/acceptance/misfire.rs`).

Reference surface:

- `crates/actionqueue-engine/src/scheduler/promotion.rs`
