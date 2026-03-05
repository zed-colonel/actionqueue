# ActionQueue Getting Started

## Purpose

This guide is the fastest contract-accurate path to:

1. understand how ActionQueue executes work,
2. bootstrap daemon runtime wiring,
3. submit Once and Repeat tasks, and
4. inspect durable state through the shipped CLI surface.

This walkthrough uses the current workspace behavior.

## Prerequisites

- Rust toolchain installed for this workspace.
- Run from repository root.

Optional preflight:

```bash
cargo build --workspace
```

## 1) Understand the execution model

ActionQueue is an **embedded task engine**, not a standalone job runner. It schedules,
dispatches, retries, and accounts for work,  but it does not decide *what* work to do.

The architecture separates three concerns:

| Concern | Responsible | Example |
|---------|------------|---------|
| **What** to execute | The `payload` — opaque bytes attached to each task | JSON work order, serialized protobuf, file path |
| **When and how** to execute | ActionQueue — scheduling, leasing, retry, concurrency | Once, Repeat(3, 60s), max_attempts=5, concurrency_key |
| **Actually executing** | Your `ExecutorHandler` implementation | HTTP call, shell command, LLM invocation, database write |

When you submit a task, ActionQueue stores the payload as opaque bytes and schedules
runs according to the run policy. When a run is dispatched, ActionQueue passes the
payload to your handler along with the `RunId`, `AttemptId`, and execution
metadata. Your handler interprets the payload and does the actual work.

This means ActionQueue is typically **embedded as a library** inside a larger
application. The application provides the handler; ActionQueue provides durable
scheduling and lifecycle management.

### Minimal handler example

Here is a complete `ExecutorHandler` implementation:

```rust
use actionqueue_executor_local::{ExecutorHandler, HandlerInput, HandlerOutput};

struct MyHandler;

impl ExecutorHandler for MyHandler {
    fn execute(&self, input: HandlerInput) -> HandlerOutput {
        // input.run_id    — stable identity for idempotent side effects
        // input.attempt_id — unique per retry (never use for idempotency)
        // input.payload   — the opaque bytes you submitted

        let payload = String::from_utf8_lossy(&input.payload);
        println!("[run={}] executing: {payload}", input.run_id);

        // Do your actual work here — call an API, write to a database,
        // invoke an LLM, start a process, etc.

        HandlerOutput::Success { output: None }
    }
}
```

The handler is wired into the engine at construction time:

```rust
let engine = ActionQueueEngine::new(config, MyHandler);
let bootstrapped = engine.bootstrap()?;
```

From that point, ActionQueue handles scheduling, leasing, timeout enforcement,
retry with backoff, concurrency key gating, and WAL-backed durability.
Your handler just does the work.

### What about the CLI and daemon?

The CLI (`actionqueue-cli`) and daemon (`actionqueue-daemon`) are operational surfaces —
they let you submit tasks, inspect state, and expose HTTP/metrics endpoints.
The daemon currently validates configuration and exposes read-only HTTP APIs
but does not run a dispatch loop with a user-provided handler (that requires
embedding ActionQueue as a library in your application).

The CLI examples below demonstrate the **scheduling surface**: submitting
tasks, inspecting state, and verifying contract behavior through the WAL.

## 2) Choose a deterministic data directory

Use a local, explicit data directory so all examples are reproducible:

```bash
mkdir -p target/tmp/getting-started
```

In this guide, `target/tmp/getting-started` is used for every command.

## 3) Bootstrap daemon runtime config

Run the daemon command in JSON mode:

```bash
cargo run -p actionqueue-cli -- daemon \
  --data-dir target/tmp/getting-started \
  --bind 127.0.0.1:8787 \
  --json
```

Expected shape (fields may vary by configured values):

```json
{
  "command": "daemon",
  "data_dir": "target/tmp/getting-started",
  "bind_address": "127.0.0.1:8787",
  "metrics_bind": "127.0.0.1:9090",
  "control_enabled": false,
  "ready": true,
  "ready_reason": "ready"
}
```

Notes:

- This confirms deterministic daemon bootstrap/config validation.
- It does **not** claim a standalone HTTP server loop is running in this command path.

## 4) Submit a Once task

```bash
cargo run -p actionqueue-cli -- submit \
  --data-dir target/tmp/getting-started \
  --task-id 11111111-1111-1111-1111-111111111111 \
  --run-policy once \
  --json
```

Expected key fields:

- `"command": "submit"`
- `"run_policy": "once"`
- `"runs_created": 1`

### Submitting with a payload

The `--payload` flag attaches a file's contents as the task's opaque payload
bytes. The `--content-type` flag sets the MIME type (informational only —
ActionQueue does not inspect the payload).

```bash
echo '{"action": "send-email", "to": "user@example.com"}' > /tmp/work.json

cargo run -p actionqueue-cli -- submit \
  --data-dir target/tmp/getting-started \
  --task-id 33333333-3333-3333-3333-333333333333 \
  --payload /tmp/work.json \
  --content-type application/json \
  --run-policy once \
  --json
```

The payload bytes are stored in the WAL and delivered to your handler's
`input.payload` field when the run is dispatched.

### Submitting with constraints

The `--constraints` flag accepts inline JSON or `@path` to a JSON file:

```bash
cargo run -p actionqueue-cli -- submit \
  --data-dir target/tmp/getting-started \
  --task-id 44444444-4444-4444-4444-444444444444 \
  --run-policy once \
  --constraints '{"max_attempts": 5, "timeout_secs": 30, "concurrency_key": "customer:123"}' \
  --json
```

Constraint fields:

- `max_attempts` — maximum retry attempts before terminal failure (default: 1)
- `timeout_secs` — per-attempt execution timeout in seconds (optional)
- `concurrency_key` — single-flight key; only one run with this key executes at a time (optional)

### Submitting with metadata

The `--metadata` flag attaches task metadata (inline JSON or `@path`):

```bash
cargo run -p actionqueue-cli -- submit \
  --data-dir target/tmp/getting-started \
  --task-id 55555555-5555-5555-5555-555555555555 \
  --run-policy once \
  --metadata '{"tags": ["demo", "getting-started"], "priority": 10}' \
  --json
```

## 5) Submit a Repeat task

```bash
cargo run -p actionqueue-cli -- submit \
  --data-dir target/tmp/getting-started \
  --task-id 22222222-2222-2222-2222-222222222222 \
  --run-policy repeat:3:60 \
  --json
```

Expected key fields:

- `"run_policy": "repeat:3:60"`
- `"runs_created": 3`

This proves run derivation for Repeat(N, interval) through the operator-visible submit surface.

## 6) Inspect durable stats

```bash
cargo run -p actionqueue-cli -- stats \
  --data-dir target/tmp/getting-started \
  --format json
```

Expected shape includes:

- `summary.total_tasks`
- `summary.total_runs`
- `summary.latest_sequence`
- `summary.runs_by_state`

After the submissions above, a deterministic example is:

- `total_tasks = 5`
- `total_runs = 7`

## 7) Verify contract behavior from automated evidence

For black-box contract proofs, use the full acceptance suite (48 tests + 1 chaos test):

**Core contract tests:**

- `tests/acceptance/once_accounting.rs`
- `tests/acceptance/repeat_accounting.rs`
- `tests/acceptance/retry_cap.rs`
- `tests/acceptance/crash_recovery.rs`
- `tests/acceptance/concurrency_key.rs`
- `tests/acceptance/observability.rs`

**Extended contract tests:**

- `tests/acceptance/cancellation.rs`
- `tests/acceptance/negative_transitions.rs`
- `tests/acceptance/lease_expiry.rs`
- `tests/acceptance/wal_corruption_recovery.rs`
- `tests/acceptance/misfire.rs`
- `tests/acceptance/dispatch_invariants.rs`
- `tests/acceptance/concurrent_dispatch_stress.rs`
- `tests/acceptance/snapshot_corruption_recovery.rs`
- `tests/acceptance/concurrent_mutation_boundary.rs`
- `tests/acceptance/mixed_attempt_outcomes.rs`
- `tests/acceptance/crash_during_promotion.rs`
- `tests/acceptance/multi_mutation_session.rs`

**Workflow feature tests** (requires `--features workflow`):

- `tests/acceptance/handler_output_roundtrip.rs`
- `tests/acceptance/dag_ordering.rs`
- `tests/acceptance/dag_failure_propagation.rs`
- `tests/acceptance/dag_cycle_rejection.rs`
- `tests/acceptance/hierarchy_lifecycle.rs`
- `tests/acceptance/dynamic_submission.rs`
- `tests/acceptance/coordinator_multi_attempt.rs`
- `tests/acceptance/cron_scheduling.rs`
- `tests/acceptance/workflow_crash_recovery.rs`
- `tests/acceptance/dag_snapshot_recovery.rs`
- `tests/acceptance/attempt_lineage.rs`

**Budget feature tests** (requires `--features budget`):

- `tests/acceptance/budget_enforcement.rs`
- `tests/acceptance/budget_replenishment.rs`
- `tests/acceptance/suspend_resume.rs`
- `tests/acceptance/budget_recovery.rs`
- `tests/acceptance/suspended_concurrency_key.rs`
- `tests/acceptance/budget_threshold_suspension.rs`
- `tests/acceptance/subscription_triggered_promotion.rs`
- `tests/acceptance/budget_threshold_subscription.rs`
- `tests/acceptance/cascading_budget.rs` (also requires `workflow`)
- `tests/acceptance/custom_event_subscription.rs`

**Actor feature tests** (requires `--features actor`):

- `tests/acceptance/actor_registration.rs`
- `tests/acceptance/capability_matching.rs`
- `tests/acceptance/remote_actor_crash.rs`
- `tests/acceptance/department_routing.rs`

**Platform feature tests** (requires `--features platform`):

- `tests/acceptance/multi_tenant_isolation.rs`
- `tests/acceptance/rbac_enforcement.rs`
- `tests/acceptance/approval_workflow.rs` (also requires `workflow`)
- `tests/acceptance/ledger_recovery.rs`
- `tests/acceptance/triad_mvp.rs` (also requires `workflow`)

**Chaos test:**

- `tests/chaos/kill_recovery.rs`

And for CLI command-surface checks:

- `crates/actionqueue-cli/tests/smoke.rs`

## 8) Read this next

- `docs/examples/idempotency-runid.md`
- `docs/policy-defaults-v0.1.md`
- `docs/acceptance-test-taxonomy.md`
