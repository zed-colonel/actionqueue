# ActionQueue Agent Notes

## Purpose

ActionQueue is the durable execution substrate in this stack. It schedules and
executes opaque tasks with WAL-backed recovery, daemon surfaces, and optional
workflow, budget, actor, and platform layers.

## Repository Structure

- `crates/actionqueue-core`: domain types and state machine
- `crates/actionqueue-storage`: WAL, snapshots, recovery, mutation authority
- `crates/actionqueue-engine`: scheduling, leases, dispatch semantics
- `crates/actionqueue-executor-local`: local handler execution
- `crates/actionqueue-runtime`: embedded runtime and async dispatch loop
- `crates/actionqueue-daemon`: HTTP daemon and metrics
- `crates/actionqueue-cli`: CLI entry point
- `crates/actionqueue-workflow`: DAG, hierarchy, cron
- `crates/actionqueue-budget`: budget enforcement and subscriptions
- `crates/actionqueue-actor`: actor registration and routing
- `crates/actionqueue-platform`: tenancy, RBAC, ledgers, approvals
- `tests/`: acceptance and chaos coverage
- `docs/`, `plans/`, `scripts/`: documentation, design notes, repo helpers

## Working Conventions

- Treat feature flags as first-class architecture boundaries.
- Run Rust commands from the repo root so workspace features resolve correctly.
- Prefer acceptance tests when validating engine semantics.
- Avoid editing `target/` and `dev-output/`.

## Verification

- Default validation from the repo root:
  - `cargo test --workspace`
  - `cargo fmt --all -- --check`
- When changing feature-gated code, run the relevant expanded matrix:
  - `cargo test --workspace --features workflow`
  - `cargo test --workspace --features workflow,budget,actor,platform`
- For daemon or CLI surface changes, build the workspace:
  - `cargo build --workspace`

## Generated And Runtime State

- Do not edit `target/` or `dev-output/` during normal code changes.
- Treat WAL, snapshot, and temporary execution data as runtime artifacts unless the task explicitly targets recovery fixtures or corruption tests.
