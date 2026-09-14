# ActionQueue 0.2.0 — AQ-CONT-1

ActionQueue is a durable execution substrate for opaque tasks. It provides
idempotent admission, scheduling, leases, retry, durable waits and signals,
checkpoint delivery, transactional child admission, and structural inspection.
Queue completion describes execution. Application verification and acceptance
remain the caller's responsibility; opaque references grant no authority.

The breaking release targets contract `AQ-CONT-1`, revision
`STACK-2026-07-20-CLEAN-1`, and conformance package revision **16**. The additive
developmental profile changes only bounded attribution; it introduces no protocol
primitive, persistence authority, scheduler semantics, or authorization path.
See [release notes and cutover](docs/releases/0.2.0.md).

## Start here

- [Getting started](docs/getting-started.md)
- [Independent durable outbox consumer](docs/examples/downstream-handoff.md)
- [Embedded, HTTP v2 and CLI operations](docs/aq-12-apis.md)
- [Store format](docs/data-dir-format-v1.0.md) and [recovery](docs/wal-recovery-guide.md)
- [Conformance package](conformance/aq-cont-1/README.md)
- [Frozen contract](docs/contracts/AQ-CONT-1.md)

## Architecture and feature boundaries

| Crate | Responsibility |
|---|---|
| [core](crates/actionqueue-core/README.md) | Domain types, validated requests, state machine |
| [storage](crates/actionqueue-storage/README.md) | WAL, snapshots, recovery, mutation authority |
| [engine](crates/actionqueue-engine/README.md) | Scheduling, derivation, concurrency, leases |
| [executor-local](crates/actionqueue-executor-local/README.md) | Physical handler attempts, timeout and cancellation |
| [runtime](crates/actionqueue-runtime/README.md) | Embedded control, dispatch and inspection |
| [daemon](crates/actionqueue-daemon/README.md) | Authenticated HTTP v2, readiness and metrics |
| [cli](crates/actionqueue-cli/README.md) | `actionqueue` operational binary |
| [workflow](crates/actionqueue-workflow/README.md) | DAG and hierarchy support; optional cron |
| [budget](crates/actionqueue-budget/README.md) | Budget enforcement and subscriptions |
| [actor](crates/actionqueue-actor/README.md) | Remote executor registration and routing |
| [platform](crates/actionqueue-platform/README.md) | Tenancy, RBAC and ledgers |

Base operation includes continuation, DAG, hierarchy and compound child admission.
`workflow` enables cron; `budget`, `actor`, and `platform` add their respective
integrations. `platform` implies actor support. Store feature profiles are fixed
at initialization; a broader binary does not upgrade an existing store.
The `testing` feature is for proof drivers and fault injection only.

## Durability and operations

Target stores use manifest schema 1, WAL framing 1, and snapshot/projection
version 9. They reject pre-contract and earlier development stores without
migration. The complete WAL is retained and replayed during recovery; a snapshot is
cross-checked against its WAL prefix and never replaces history. Mutation preparation currently
clones the full projection; no general throughput claim is made.

Embedded callers configure `HostControlContext` explicitly. HTTP object inspection
requires host authentication; mutations also require control enablement. CLI
controls use either `--offline --data-dir PATH` or `--daemon URL` with a token.
Offline backup and restore require exclusive ownership of the source/destination.

## Verification

Run from the repository root with Rust 1.89.0:

```sh
cargo test --workspace
cargo fmt --all -- --check
cargo clippy --all --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace --features workflow
cargo test --workspace --features workflow,budget,actor,platform
cargo aq-conformance
cargo aq-developmental
python3 -B -m unittest discover -s tests/release
python3 -B scripts/check-consumer.py
```

Release preparation executes the complete feature and evidence matrix on one clean
commit. See [release procedure](docs/releases/0.2.0.md); preparation does not publish.

Licensed under [Apache-2.0](LICENSE).
