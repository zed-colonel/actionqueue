# ActionQueue

## Overview

**ActionQueue** is a queue *and* an action engine.

It turns **intent** into **durable, inspectable, scheduled action**. It does not care what an action "means." It cares that an action is **accounted for**, **executed according to the schedule you asked for**, and **never dispatched beyond that schedule**.

ActionQueue's value is leverage:

- Small, composable units (tasks)
- Durable scheduling (WAL-backed state)
- Predictable lifecycle semantics
- Extensibility at stable boundaries

**Implemented in Rust.** Designed as an embeddable library with optional HTTP daemon and CLI surfaces.

> ActionQueue is meant to be a point of leverage through which intention becomes action.

---

## Features

### Core Engine

- **TaskSpec** -- durable intent: payload (opaque bytes) + run policy + constraints + metadata
- **Run policies** -- Once, Repeat(N, interval), Cron (7-field UTC expressions)
- **Canonical run states** -- Scheduled, Ready, Leased, Running, RetryWait, Suspended, Completed, Failed, Canceled
- **Validated monotonic transitions** -- terminal states are final; no backward movement
- **Retry model** -- configurable max_attempts with fixed or exponential backoff
- **Concurrency keys** -- single-flight execution per key with configurable hold policy
- **Lease-based execution** -- at most one active lease per run; expiry returns to eligibility
- **Misfire handling** -- eager catch-up for overdue scheduled runs

### Persistence

- **WAL (v5)** -- append-only postcard binary + CRC-32; mandatory for correctness
- **Snapshots (schema v8)** -- derived acceleration artifacts; atomic writes via temp+rename
- **Deterministic recovery** -- reconstructible from snapshot + WAL tail, or WAL alone
- **Crash safety** -- trailing corruption truncated; in-flight runs re-eligible per uncertainty clause

### Workflow (feature: `workflow`)

- **DAG dependencies** -- task execution ordering with cycle detection
- **Task hierarchy** -- parent-child cascade cancellation and completion gating
- **Dynamic submission** -- coordinator handlers submit child tasks at runtime
- **Cron scheduling** -- rolling window derivation from 7-field UTC cron expressions

### Budget Enforcement (feature: `budget`)

- **Per-task budgets** -- Token, CostCents, TimeSecs dimensions with configurable limits
- **Suspend/resume** -- handler-initiated suspension; suspended attempts don't count toward max_attempts
- **Event subscriptions** -- reactive task promotion on completion, state change, budget threshold, or custom events
- **In-flight cancellation** -- budget exhaustion signals running handlers via CancellationToken

### Remote Actors (feature: `actor`)

- **Actor registration** -- WAL-backed registration with capability declaration
- **Heartbeat monitoring** -- configurable interval with timeout-based auto-deregistration
- **Capability routing** -- filter actors by required task capabilities
- **Department grouping** -- actors organized into named departments

### Platform (feature: `platform`)

- **Multi-tenant isolation** -- tasks and actors scoped to organizational tenants
- **RBAC** -- role assignment (Operator, Auditor, Gatekeeper, Custom) with typed capability grants
- **Append-only ledgers** -- audit, decision, relationship, and incident ledgers with tenant/key indexing
- **Approval workflows** -- DAG-based Operator-Auditor-Gatekeeper approval chains

### Operational Surfaces

- **HTTP daemon** -- Axum-based REST API (`/api/v1/` introspection, `/api/v2/` actor/platform), Prometheus metrics
- **CLI** -- `daemon`, `submit`, `stats` commands with JSON output

---

## Crate Architecture

Eleven workspace crates forming a strict dependency DAG:

```
  actionqueue-core           Pure domain types, state machine, no I/O
  actionqueue-storage        WAL v5, snapshots v8, recovery, mutation authority
  actionqueue-executor-local Dispatch queue, retry, backoff, timeout
  actionqueue-engine         Scheduling, derivation, concurrency, leases
  actionqueue-workflow       DAG, hierarchy, cron, dynamic submission
  actionqueue-budget         Budget tracking, enforcement, subscriptions
  actionqueue-actor          Actor registry, heartbeat, capability routing
  actionqueue-platform       Tenant registry, RBAC, append-only ledgers
  actionqueue-runtime        Async dispatch loop, embedded API
  actionqueue-daemon         HTTP server, REST API, Prometheus metrics
  actionqueue-cli            Binary entry point
```

Feature flags control optional integration:

```bash
cargo test --workspace                                              # Core tests
cargo test --workspace --features workflow                          # + workflow
cargo test --workspace --features budget                            # + budget
cargo test --workspace --features actor                             # + actor
cargo test --workspace --features platform                          # + platform
cargo test --workspace --features workflow,budget,actor,platform    # All (939 tests)
```

---

## Quick Start

### As an embedded library

```rust
use actionqueue_executor_local::{ExecutorHandler, HandlerInput, HandlerOutput};

struct MyHandler;

impl ExecutorHandler for MyHandler {
    fn execute(&self, input: HandlerInput) -> HandlerOutput {
        let payload = String::from_utf8_lossy(&input.payload);
        println!("[run={}] executing: {payload}", input.run_id);
        HandlerOutput::Success { output: None }
    }
}

// Wire into engine at construction time:
// let engine = ActionQueueEngine::new(config, MyHandler);
// let boot = engine.bootstrap()?;
// boot.submit_task(task_spec)?;
// boot.run_until_idle()?;
```

### CLI

```bash
# Start daemon
cargo run -p actionqueue-cli -- daemon --data-dir ./data --bind 127.0.0.1:8787

# Submit a task
cargo run -p actionqueue-cli -- submit --data-dir ./data --run-policy once --json

# View stats
cargo run -p actionqueue-cli -- stats --data-dir ./data --format json
```

---

## Contract Guarantees

1. **Run policy guarantee** -- ActionQueue creates exactly the runs implied by the policy and never dispatches more
2. **Completion durability** -- once durably marked Completed, a run is never dispatched again
3. **Retry cap** -- retries never create extra runs and never exceed max_attempts
4. **Concurrency enforcement** -- lease and concurrency key constraints are core-enforced, not policy-overridable
5. **Mutation authority** -- all durable state changes route through validated core interfaces

See [`actionqueue-charter.md`](actionqueue-charter.md) for the full contract and [`invariant-boundaries-v0.1.md`](invariant-boundaries-v0.1.md) for enforcement policy.

---

## Testing

48 acceptance tests + 1 chaos test organized by feature:

- **Core contract** (18 tests) -- once/repeat accounting, retry cap, crash recovery, concurrency, observability, cancellation, lease expiry, WAL corruption, misfire, dispatch invariants, stress tests
- **Workflow** (11 tests, `--features workflow`) -- DAG ordering, failure propagation, cycle rejection, hierarchy, dynamic submission, cron scheduling, crash recovery
- **Budget** (10 tests, `--features budget`) -- enforcement, replenishment, suspend/resume, recovery, threshold suspension, event subscriptions
- **Actor** (4 tests, `--features actor`) -- registration, capability matching, crash detection, department routing
- **Platform** (5 tests, `--features platform`) -- tenant isolation, RBAC, approval workflow, ledger recovery, triad MVP

See [`docs/acceptance-test-taxonomy.md`](docs/acceptance-test-taxonomy.md) for detailed invariant mappings.

---

## Documentation

- [Getting started guide](docs/getting-started.md)
- [Acceptance test taxonomy](docs/acceptance-test-taxonomy.md)
- [Idempotency and RunId](docs/examples/idempotency-runid.md)
- [Policy defaults reference](docs/policy-defaults-v0.1.md)
- [Data directory format](docs/data-dir-format-v0.md)
- [WAL recovery guide](docs/wal-recovery-guide.md)
- [Project charter](actionqueue-charter.md)
- [Invariant boundaries](invariant-boundaries-v0.1.md)

---

## License

This project is licensed under the MIT License -- see the [LICENSE](LICENSE) file for details.
