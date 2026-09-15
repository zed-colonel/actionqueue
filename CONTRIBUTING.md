# Contributing to ActionQueue

Thank you for your interest in contributing to ActionQueue! This document covers the
development workflow, coding standards, and submission process.

## Prerequisites

- **Rust toolchain:** 1.89.0 (pinned in `rust-toolchain.toml`)
- **cargo** with fmt, clippy components

## Building

```bash
cargo build --workspace
```

## Running Tests

```bash
# Core tests (no feature flags)
cargo test --workspace

# With specific feature flags
cargo test --workspace --features workflow
cargo test --workspace --features budget
cargo test --workspace --features actor
cargo test --workspace --features platform

# Full test suite (all features)
cargo test --workspace --features workflow,budget,actor,platform
```

## Linting & Formatting

All code must pass these checks before merge:

```bash
cargo fmt --all -- --check
cargo clippy --all --all-targets --features workflow,budget,actor,platform -- -D warnings
```

Key lint thresholds (see `clippy.toml`):
- `too-many-arguments-threshold = 5`
- `too-many-lines-threshold = 80`
- `cognitive-complexity-threshold = 15`

Formatting rules (see `rustfmt.toml`):
- 4-space indent, 100-character max line width
- `group_imports = "StdExternalCrate"`

## Crate Architecture

ActionQueue is organized as 11 workspace crates with a strict dependency DAG. Key principles:

- **`actionqueue-core`** has no internal dependencies and no I/O
- Extension features live in separate crates (`workflow`, `budget`, `actor`, `platform`)
- Feature flags gate optional crate integration at the runtime/daemon level
- The root `Cargo.toml` is the acceptance test harness, not a library

## Core Invariants

These are non-negotiable — never weaken them in a contribution:

1. All runs originate from durable intent (TaskSpec or explicit append)
2. State transitions are validated and monotonic
3. Completion is durable (WAL-backed, survives restart)
4. Lease and concurrency constraints are core-enforced
5. External extensions cannot mutate persisted state directly

See `docs/invariant-boundaries-v1.0.md` for the full invariant specification.

## AQ-CONT-1 boundary checks

`cargo test --workspace` includes the `AQ-CONT-1` conformance binaries under
`tests/conformance/`. They fail if target crates gain downstream domain types, developmental
(campaign/arm/benchmark) concepts, free-form metadata maps, attribution reads in scheduling or
authority code, developmental metric labels, archive references, or new uses of legacy symbols
slated for removal. Run them alone with `cargo aq-conformance`. The checks read only
git-tracked files, so untracked notes cannot affect them. The policy lives in
`conformance/aq-cont-1/contract-boundaries.json`; see `docs/contracts/AQ-CONT-1.md`.

## Submitting Changes

1. Fork the repository and create a feature branch
2. Make your changes, ensuring all tests pass
3. Run `cargo fmt --all` and `cargo clippy` before committing
4. Open a pull request with a clear description of the change. `AQ-CONT-1` implementation
   PRs target the `aq-cont-1` integration branch and include the review template from
   Section 2.3 of `docs/planning/aq-cont-1/aq-cont-1-implementation-plan.md`

## License

By contributing, you agree that your contributions will be licensed under the
Apache License, Version 2.0 that covers this project.
