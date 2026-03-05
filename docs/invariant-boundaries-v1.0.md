# ActionQueue Invariant Boundaries Policy v1.0

**Status:** normative policy for pre-control-plane hardening
**Applies to:** ActionQueue core runtime and persistence surfaces in v0.1 hardening tranche
**Last Updated:** 2026-03-02

---

## 1. Purpose

This document defines the non-negotiable mutation boundaries that protect ActionQueue correctness.

ActionQueue may extend policies and execution surfaces, but it may not weaken invariant enforcement. Any implementation change must preserve this policy.

Primary contract references:

- [`actionqueue-charter.md`](actionqueue-charter.md)

---

## 2. Normative language

- **MUST**: mandatory for correctness.
- **MUST NOT**: forbidden for correctness.
- **SHOULD**: strong requirement unless there is an explicit, reviewed exception.

---

## 3. Source-of-truth hierarchy

1. **WAL is authoritative.**
2. **Snapshots are derived acceleration artifacts.**
3. **In-memory indices are ephemeral projections.**

Therefore:

- State changes that matter **MUST** be durably representable in WAL event history.
- Recovery **MUST** be reconstructible from snapshot plus WAL tail, or WAL alone.
- No in-memory-only state may decide post-restart truth for run lifecycle outcomes.

---

## 4. Mutation authority model

### 4.1 Allowed mutation lanes

State mutation is legal only in these lanes:

1. **Validated construction lane** for new task or run entities.
2. **Validated transition lane** for run lifecycle changes.
3. **WAL append lane** for durable event admission.
4. **Replay application lane** for deterministic reconstruction.

Any other mutation path is forbidden.

### 4.2 Forbidden mutation patterns

- Direct public-field mutation of state-bearing domain objects.
- State transition writes that skip transition validation.
- WAL append acceptance of non-monotonic sequence values.
- Recovery logic that invents lifecycle history absent durable evidence.

---

## 5. Sacred invariants and enforcement map

| Invariant | Mandatory enforcement | Primary implementation surfaces |
| --- | --- | --- |
| All runs originate from durable intent | Run creation must trace to task policy or explicit append; creation must be durably representable | [`crates/actionqueue-engine/src/derive/repeat.rs`](crates/actionqueue-engine/src/derive/repeat.rs), [`crates/actionqueue-storage/src/wal/event.rs`](crates/actionqueue-storage/src/wal/event.rs) |
| State transitions are monotonic and validated | Transition checks must gate state evolution; backward transitions forbidden | [`crates/actionqueue-core/src/run/transitions.rs`](crates/actionqueue-core/src/run/transitions.rs), [`crates/actionqueue-storage/src/recovery/reducer.rs`](crates/actionqueue-storage/src/recovery/reducer.rs) |
| Completion is durable | Completed truth must depend on durable record, not volatile memory | [`crates/actionqueue-storage/src/wal/event.rs`](crates/actionqueue-storage/src/wal/event.rs), [`crates/actionqueue-storage/src/recovery/reducer.rs`](crates/actionqueue-storage/src/recovery/reducer.rs) |
| Lease and concurrency constraints are core-enforced | Eligibility and execution ownership must be constrained by lease and key gates | [`crates/actionqueue-engine/src/lease/model.rs`](crates/actionqueue-engine/src/lease/model.rs), [`crates/actionqueue-engine/src/concurrency/key_gate.rs`](crates/actionqueue-engine/src/concurrency/key_gate.rs) |
| External extensions cannot mutate persistence directly | Mutations must route through validated core interfaces | [`actionqueue-charter.md`](actionqueue-charter.md), [`crates/actionqueue-storage/src/wal/event.rs`](crates/actionqueue-storage/src/wal/event.rs) |

---

## 6. Hard policy rules by subsystem

### 6.1 Core model rules

- State-bearing fields in domain structs **MUST NOT** be broadly mutable by downstream call sites.
- Domain constructors **MUST** validate constraints that can break lifecycle safety.
- Transition APIs **MUST** return typed errors for invalid movement.

Current hardening targets:

- [`crates/actionqueue-core/src/run/run_instance.rs`](crates/actionqueue-core/src/run/run_instance.rs)
- [`crates/actionqueue-core/src/task/task_spec.rs`](crates/actionqueue-core/src/task/task_spec.rs)
- [`crates/actionqueue-core/src/task/constraints.rs`](crates/actionqueue-core/src/task/constraints.rs)

### 6.2 Derivation and accounting rules

- Repeat derivation **MUST** satisfy exact accounting or fail explicitly.
- Partial derivation under arithmetic overflow **MUST NOT** be treated as success.
- Run policy arithmetic representability **SHOULD** be validated before derivation loop entry.

Current hardening targets:

- [`crates/actionqueue-engine/src/derive/repeat.rs`](crates/actionqueue-engine/src/derive/repeat.rs)
- [`crates/actionqueue-engine/src/derive/mod.rs`](crates/actionqueue-engine/src/derive/mod.rs)

### 6.3 WAL admission rules

- WAL sequence values **MUST** be strictly increasing per writer session and resume state.
- Non-increasing sequence append attempts **MUST** fail fast with typed errors.
- Critical transitions **SHOULD** be followed by durability sync according to durability policy.

Current hardening targets:

- [`crates/actionqueue-storage/src/wal/fs_writer.rs`](crates/actionqueue-storage/src/wal/fs_writer.rs)

### 6.4 Recovery and uncertainty rules

- Replay reducer **MUST** derive post-crash eligibility from durable lease and run evidence.
- Uncertainty clause behavior **MUST** be deterministic and explainable from WAL history.
- Lease lifecycle facts needed for restart decisions **MUST** be durably representable.

Current hardening targets:

- [`crates/actionqueue-storage/src/wal/event.rs`](crates/actionqueue-storage/src/wal/event.rs)
- [`crates/actionqueue-storage/src/recovery/reducer.rs`](crates/actionqueue-storage/src/recovery/reducer.rs)

### 6.5 Executor timeout rules

- Timeout classification and timeout enforcement **MUST** be distinct concerns.
- If timeout is documented as cancellation, execution boundary **MUST** support cancellation semantics.
- Classification-only timeout behavior **MUST NOT** be misrepresented as cancellation.

Current hardening targets:

- [`crates/actionqueue-executor-local/src/attempt_runner.rs`](crates/actionqueue-executor-local/src/attempt_runner.rs)
- [`crates/actionqueue-executor-local/src/timeout.rs`](crates/actionqueue-executor-local/src/timeout.rs)

---

## 7. Integration boundary rules for downstream systems

ActionQueue guarantees internal accounting and durable lifecycle semantics. Downstream systems are responsible for external side-effect discipline.

Therefore downstream adapters and executors:

- **MUST** accept and propagate `RunId` for idempotency.
- **MUST** preserve attempt lineage with `AttemptId`.
- **SHOULD** align concurrency keys with external collision domains.

---

## 8. Verification requirements

Any hardening change is incomplete until tests prove boundary behavior.

Minimum proof categories:

1. Exact run accounting for Once and Repeat.
2. Sequence monotonic WAL append rejection.
3. Crash recovery with uncertainty-clause re-eligibility.
4. Attempt cap invariance.
5. Concurrency key non-overlap for same key and overlap for different keys.
6. Timeout behavior proving enforcement semantics, not only elapsed classification.

Primary test surfaces:

- [`crates/actionqueue-engine/tests/`](crates/actionqueue-engine/tests/)
- [`crates/actionqueue-storage/tests/`](crates/actionqueue-storage/tests/)
- [`crates/actionqueue-executor-local/tests/`](crates/actionqueue-executor-local/tests/)

---

## 9. Policy change control

This policy may evolve only if changes:

1. Preserve or strengthen invariant enforcement.
2. Include explicit migration notes for affected layers.
3. Add or update tests that prove preserved guarantees.

No policy change may authorize direct persisted-state mutation outside validated core interfaces.
