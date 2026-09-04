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
- `tests/`: acceptance, chaos, and `AQ-CONT-1` conformance coverage
- `docs/contracts/`, `docs/adrs/`, `docs/planning/`: frozen `AQ-CONT-1` contract, ADR queue, and planning package
- `conformance/aq-cont-1/`: conformance manifest, boundary policy, developmental acceptance matrix
- `archive/pre-aq-cont-1/`: frozen pre-contract evidence; never referenced from `crates/`
- `docs/`, `plans/`, `scripts/`: documentation, design notes, repo helpers

## Working Conventions

- The repository implements contract `AQ-CONT-1` (see `docs/contracts/AQ-CONT-1.md`). Do not
  add campaign, arm, benchmark, evaluation, or free-form metadata concepts to `crates/`; the
  boundary policy in `conformance/aq-cont-1/contract-boundaries.json` fails the build.
- Frozen documents under `docs/contracts/`, `docs/planning/`, and `archive/` are hash-pinned;
  changing them requires updating the matching `SHA256SUMS` and a contract amendment.
- When a removal PR deletes a legacy symbol, flip its stage to `forbid` in the boundary policy.
- Treat feature flags as first-class architecture boundaries.
- Run Rust commands from the repo root so workspace features resolve correctly.
- Prefer acceptance tests when validating engine semantics.
- Avoid editing `target/` and `dev-output/`.

## Verification

- Default validation from the repo root:
  - `cargo test --workspace`
  - `cargo fmt --all -- --check`
  - `scripts/check_contract_boundaries.sh` (also part of `cargo test --workspace`)
- When changing feature-gated code, run the relevant expanded matrix:
  - `cargo test --workspace --features workflow`
  - `cargo test --workspace --features workflow,budget,actor,platform`
- For daemon or CLI surface changes, build the workspace:
  - `cargo build --workspace`

## Generated And Runtime State

- Do not edit `target/` or `dev-output/` during normal code changes.
- Treat WAL, snapshot, and temporary execution data as runtime artifacts unless the task explicitly targets recovery fixtures or corruption tests.
