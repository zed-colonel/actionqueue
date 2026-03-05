# ACTIONQUEUE  
*A simple device that acts as a force-multiplier.*

---

**NOTE** This charter was the orginal design artifact and, as such, should be treated as accurate only in spirit.

## I. OVERVIEW

**ActionQueue** is a queue *and* an action engine.

It exists to turn **intent** into **durable, inspectable, scheduled action**. It does not care what an action “means.” It cares that an action is **accounted for**, **executed according to the schedule you asked for**, and **never dispatched beyond that schedule**.

ActionQueue’s value is leverage:

- small, composable units (tasks)
- durable scheduling (WAL-backed state)
- predictable lifecycle semantics
- extensibility at stable boundaries

**ActionQueue is implemented in Rust** and designed was designed in layers with a minimal core while preserving clean interfaces for future growth.

> ActionQueue is the point of leverage through which intention becomes action.

---

## II. GOALS AND NON-GOALS

### Goals

1. **Durable scheduling & execution accounting**  
   If you ask for “once,” ActionQueue will not dispatch that run more than once.  
   If you ask for “6 times, every 30 minutes,” ActionQueue will produce **exactly 6 scheduled runs** and will not dispatch more than those runs.

2. **Clear, explicit execution semantics**  
   Ordering, retries, deadlines, and failure handling are all defined as part of the contract (not implication).

3. **Extensibility without chaos**  
   Policies can be swapped. Invariants cannot.

4. **Introspection through transparency**  
   It must be easy to observe “what will happen next,” “why,” and “what happened before.”

### Non-Goals (for v1.0)

- **No clustering / consensus / raft.**  
  ActionQueue is single-node by design. If you want consensus later, layer it over daemon mode.

- **No magical exactly-once external side effects.**  
  ActionQueue can control dispatch count and internal state transitions. It cannot control the external world unless effects are brought under transactional boundaries or made idempotent (see Contract).

---

## III. MODES OF OPERATION

> These are usage modes, not restrictive design cages.

### 1) Embedded Mode
- Library/component use inside another application.
- Same core engine and semantics as daemon mode.
- Caller supplies the “action execution host” (local executor).

### 2) Daemon Mode
- Long-running service/container.
- HTTP API (admin/metrics/control) and optional worker gateway.
- Provides an operational envelope: persistence, scheduling, observability, and a stable control plane.

---

## IV. THE CONTRACT (WHAT ACTIONQUEUE GUARANTEES)

This section is the spine. Everything else exists to make these statements true.

### A. Core Terms

- **Task (TaskSpec):** the user’s durable intent (payload + run policy + constraints).
- **Run (RunInstance):** a specific scheduled occurrence of a task.
- **Attempt:** a single try of a run (retries are attempts of the same run).

### B. Run Policy (How Many Times?)

A task declares a run policy. Examples:

- **Once:** exactly one run
- **Repeat:** N runs at a fixed interval (e.g., 6 runs, every 30 minutes)
- **Cron/Calendar:** runs derived from a schedule expression (optional)
- **Ad-hoc:** runs appended directly (no derived schedule)

**Guarantee (Run Accounting):**
- ActionQueue will create **exactly the runs implied by the run policy**.
- ActionQueue will **never dispatch more runs** than that policy produces.

### C. Dispatch Semantics (How Many Times Will It Be Sent to an Executor?)

**Guarantee (Dispatch Cap):**
- Each run has a durable lifecycle.
- Once a run is durably marked **Completed**, it will never be dispatched again.
- ActionQueue will not intentionally dispatch a run more than once *unless the system has uncertainty about completion*.

That last clause matters, so here’s the honest truth:

### D. External Side Effects (Reality Clause)

ActionQueue can guarantee **internal exactly-once completion records**. It cannot, by itself, guarantee external side effects happened exactly once if a crash occurs mid-flight.

Therefore ActionQueue supports **three execution safety levels**:

1) **Pure / deterministic actions**  
   Safe. Re-execution is harmless.

2) **Idempotent actions** (recommended default)  
   The action must accept a **RunId / IdempotencyKey** and behave idempotently.  
   ActionQueue guarantees it will *not exceed the dispatch cap*, and your handler guarantees duplicates don’t cause duplicate effects.

3) **Transactional actions** (advanced)  
   Effects occur within a transactional boundary ActionQueue can fence (e.g., DB transaction with a unique constraint on RunId, or an outbox pattern).

**ActionQueue will expose RunId and AttemptId to all executors.** If you ignore them, you’re choosing chaos.

### E. Ordering Semantics (What “In Order” Means Now)

ActionQueue does **not** promise a universal global ordering across all tasks.

Instead:

- ActionQueue guarantees **dependency order** (if task B depends on A, B will not be eligible until A is Completed).
- ActionQueue guarantees **per-key sequencing** if the task declares a **concurrency key** (e.g., `customer:123`), meaning only one run for that key may be Running at a time.
- ActionQueue provides a default selection policy (FIFO + priority), but **ordering is a policy** that can be replaced.

### F. Retry Semantics (Replay “Within Limits”)

Retries apply to **attempts**, not **runs**.

- A run may have up to `max_attempts`.
- Attempts follow a backoff policy.
- When attempts are exhausted, the run becomes **Failed** (and may trigger compensating behavior / dead-letter policy).

**Guarantee:**
- Retries will never create extra runs.
- Retries will never exceed `max_attempts`.
- Retry policy is explicit and inspectable.

### G. Misfires / Downtime (When ActionQueue Wasn’t Running)

If ActionQueue is down and a schedule window passes, a task’s policy defines what to do:

- **Catch up** (default, with a cap)
- **Skip** missed runs
- **Coalesce** (emit one run representing the missed window)

This is policy-driven and visible; it must never be “surprising.”

---

## V. INVARIANTS

> **The queue is sacred.** It may be shaped, observed, and reflected upon — but never bypassed.

To keep that sentence from becoming poetry, ActionQueue has non-overridable invariants:

1. **All runs originate from durable intent**  
   Runs are derived from TaskSpecs or explicitly appended, and are recorded durably.

2. **State transitions are validated and monotonic**  
   A run cannot “go backward” (Completed → Ready is forbidden).

3. **Completion is durable**  
   If a run is Completed, it stays Completed across restarts.

4. **Leases and concurrency constraints are enforced by core**  
   Policies may influence *which* eligible run is selected, but cannot violate constraints.

5. **Plugins cannot directly mutate persisted state**  
   Plugins may *propose* changes through validated interfaces. Core enforces invariants.

---

## VI. ARCHITECTURE (CRATE-BASED, SINGLE NODE)

Everything below is implementation detail in service of the contract.

### A. Core Concepts

- **TaskSpec**
  - Payload (opaque bytes/JSON)
  - RunPolicy (once/repeat/etc.)
  - Constraints (deadlines, max_attempts, concurrency key)
  - Metadata (origin, tags, priority hints)

- **RunInstance**
  - RunId (stable, unique)
  - ScheduledAt
  - State (see below)
  - Attempt counters / timestamps

- **State Machine**
  - Durable event log (WAL)
  - Materialized indices for:
    - Ready runs (eligible now)
    - Scheduled runs (future)
    - Running (leased / in-flight)
    - Completed / Failed / Canceled
  - Snapshotting to accelerate recovery

### B. Run Lifecycle (Canonical States)

A minimal, explicit model:

- **Scheduled** → **Ready** → **Leased** → **Running** → **Completed**
- Failure path:
  - **Running** → **RetryWait** → **Ready**
  - or **Running** → **Failed**
- Control path:
  - **Ready/RetryWait/Scheduled** → **Canceled**
  - (optional) Running cancellation is best-effort depending on executor support

### C. Engine

The ActionQueue Engine:
- derives runs from schedule policies
- advances time (promotes Scheduled → Ready)
- selects eligible runs (policy)
- enforces constraints (dependencies, concurrency keys, deadlines)
- coordinates execution (local executor and/or gateway)
- persists transitions to WAL

### D. Persistence

- **WAL (append-only) is mandatory for correctness.**
- Snapshots are allowed for startup performance, never as a source of truth.
- Storage backend can be file-based for embedded; daemon may add optional DB projection.

---

## VII. EXTENSIBILITY (TWO TIERS, NOT A LIE)

We want both performance and dynamism. So ActionQueue has two distinct extension mechanisms:

### Tier 1: Native Extensions (Rust Crates, Compiled-In)
- Feature-gated crates implementing extension traits.
- Used for:
  - selection policy (ordering semantics)
  - schedule derivation policies
  - retry policies
  - projections (metrics, DB write models)
  - enrichers (context attachment)
- **Not hot-swappable.** Stable, fast, debuggable.

### Tier 2: Sandboxed Plugins (WASM Components) — *Planned for future versions*
- Optional add-on host (Wasmtime Component Model + WASI Preview 2).
- Used for dynamic mutation of behavior.
- Must operate through **capability-limited interfaces**:
  - observe events
  - propose queue mutations (append runs / adjust priorities within rules)
  - request stats
- Hard limits:
  - fuel metering
  - memory caps
  - deny-by-default host calls

**Important:** WASM plugins are *not* required for v1.0 correctness. They are a power tool for later.

---

## VIII. EXECUTION SURFACES

### A. Local Executor (Embedded + Daemon)
Default implementation: in-process worker pool.
- simplest correctness story
- best place to nail semantics first

### B. Worker Gateway (Optional / Daemon)
A stateless bridge for out-of-process workers (gRPC or similar):
- workers pull eligible runs
- ActionQueue grants leases
- workers heartbeat lease and report completion/failure

**Lease Semantics:**
- At most one active lease per run at a time.
- Expired leases return the run to eligibility subject to policy.

**Reality clause still applies:** if a worker crashes after effects but before reporting completion, ActionQueue may re-dispatch due to uncertainty. That is why RunId exists.

---

## IX. API & ADMIN UI

### A. HTTP API (Axum)
Default posture: read-only.

- `/healthz`, `/ready`
- `/api/v1/stats` (engine stats)
- `/api/v1/tasks`, `/api/v1/runs/:id` (introspection)
- control endpoints behind explicit feature flag:
  - cancel run/task
  - pause/resume queues
  - inject a run (append)

### B. Admin UI (Static SPA) *Planned for future version*
- served as static assets by daemon
- speaks only to `/api/*`
- focuses on:
  - what’s ready / running / scheduled
  - why something is blocked
  - run timelines
  - policy visibility (what policy selected this run?)

---

## X. OBSERVABILITY

ActionQueue must be legible.

- Prometheus metrics:
  - runs by state
  - scheduling lag
  - attempt counts / failures
  - lease expirations
  - executor durations
- Tracing spans:
  - RunId and AttemptId always attached
- Event stream (internal):
  - every state transition is an event
  - projections subscribe (DB, analytics, etc.)

---

## XI. SECURITY (MINIMUM VIABLE POSTURE)

- Default bind: localhost
- Optional:
  - mTLS or static admin token for control endpoints
- Audit log:
  - run/task mutations
  - cancellations
  - policy changes (if dynamic)

Plugin security (future):
- allowlist plugin directory
- optional signature verification

---

## XII. DEVELOPMENT PHILOSOPHY (REALISTIC VERSION)

- **Debt is explicit.** Any shortcut must have an owner and an exit criteria.
- **Invariants are hard-coded.** Policies are pluggable.
- **Correctness before cleverness.** If it can’t be explained, it doesn’t ship.
- **Composability is earned at stable boundaries.** Not by making everything override everything.

---

## XIII. MYTHOS

> _“Give me a place to stand, and I shall move the world.”_  
> — Archimedes

ActionQueue is the hinge between stillness and motion: the place where a plan becomes scheduled reality. It is not the action itself. It is the leverage that makes action inevitable, while remaining accountable.

---

## XIV. FINAL NOTES

This is a living document. Additions must preserve the foundational property:

> **Intention becomes motion through durable, inspectable leverage.**

---

**Document Last Updated:** `2026-02-11`  
**Version:** `0.1.0-alpha`
