# AQ-CONT-1 Implementation Plan

*Clean-break delivery of durable continuations, idempotent admission, and causal execution*

**Status:** Repository-level development planning baseline with neutral developmental-campaign execution clarification  
**Target repository:** [`zed-colonel/actionqueue`](https://github.com/zed-colonel/actionqueue)  
**Pinned source baseline:** `97c9dc26c19c697dbfb204ed503e82c5f053394f` (`main`, June 9, 2026)  
**Normative architecture:** `ActionQueue Hardening: Durable Continuations, Idempotent Admission, and Causal Execution`  
**Contract:** `AQ-CONT-1`  
**Planning profile:** `AQ-PLAN-2`  
**Coordinated stack revision:** `STACK-2026-07-20-CLEAN-1`  
**Developmental diagnostics profile:** `STACK-DEVELOPMENTAL-DIAGNOSTICS-1`  
**Downstream contracts:** `WI-FABRIC-2`, then `EXO-V3`  
**Planning posture:** Pre-production clean break; no runtime compatibility obligation to pre-`AQ-CONT-1` code or persisted state  
**Date:** August 10, 2026

> **Implementation thesis:** Preserve ActionQueue's durable-execution invariants. Replace the lifecycle, admission, continuation, and attribution seams that cannot safely carry long-horizon asynchronous work. Carry developmental campaign attribution through those same bounded seams without creating queue-owned research semantics, scheduler backchannels, or performance judgments.

---

## 0. Purpose and decision summary

This document turns the `AQ-CONT-1` architecture baseline into an executable repository plan. It is intentionally narrower than the architecture document. It answers:

- which source areas change;
- which decisions must be locked before code depends on them;
- how to stage the breaking work while keeping one implementation path;
- which pull requests form the critical path;
- what each pull request must prove before the next may depend on it;
- how the target persistence lineage, recovery model, and conformance package are established;
- when WorldInterface may safely begin depending on ActionQueue;
- how Exoskeleton or a specialized application may durably execute developmental campaigns through existing causal and continuation seams;
- how conformance proves that campaign attribution changes neither scheduling nor authority and that queue outcomes are not evaluation.

The implementation is organized as **fourteen ordered pull requests** against one integration branch. The sequence preserves green builds and reviewable correctness boundaries without retaining a selectable legacy runtime.

### 0.1 Decisions fixed by this plan

1. **Use a clean integration branch.** Create `aq-cont-1` from the pinned baseline. Pull requests target that branch until the complete target contract passes conformance. There is one runtime path in the branch at every commit; no feature flag switches between old and new semantics.
2. **Freeze the baseline as evidence.** Tag the current commit, archive selected fixtures and performance results, and stop treating old stores or APIs as compatibility targets.
3. **Keep the existing eleven-crate dependency DAG for the first release.** Add modules, not a new continuation crate, until the interfaces and performance profile are proven.
4. **Start a fresh persistence lineage.** `AQ-CONT-1` stores carry an explicit manifest and target format version. Pre-contract stores are rejected without modification.
5. **Use purpose-built atomic records.** Implement compound task admission and compound attempt disposition. Do not introduce a general transaction language.
6. **Make `Awaiting` a distinct run state.** `Suspended` remains preemption; `Awaiting` means a durable continuation condition exists.
7. **Make external wake-up durable.** Durable `SignalEnvelope` records and durable `WaitSpec` records replace transient custom events for external coordination.
8. **Make admission knowable and idempotent.** `ensure_task` returns `Created`, `AlreadyExists`, or conflict; it never means merely “queued in memory.”
9. **Replace `HandlerOutput` outright.** `AttemptDisposition` becomes the only target handler result contract.
10. **Delete fire-and-forget child submission.** Mandatory child admissions are committed with the parent attempt disposition.
11. **Carry causal references without interpreting them.** Attribution is preserved; authorization remains outside ActionQueue.
12. **Rename actor capabilities to executor traits.** Routing ability is not resource authority.
13. **Prove every new guarantee at domain, replay, and crash-recovery levels.** A happy-path integration test is not a release gate.
14. **Publish ActionQueue before downstream redesign releases.** WorldInterface may claim `WI-FABRIC-2` continuation semantics only after pinning a released `AQ-CONT-1` implementation and conformance revision.
15. **Use existing causal seams for developmental attribution.** `trace_id`, `correlation_id`, `CausationLink`, and one bounded `origin_ref` may preserve campaign and intervention-arm lineage; no new campaign primitive or metadata map is added.
16. **Keep execution distinct from evaluation.** Queue terminal states, timings, retries, and recovery facts are evidence for a higher layer, never task correctness, arm preference, binding-constraint diagnosis, saturation, or candidate acceptance.

### 0.2 What this plan deliberately does not do

This plan does not:

- migrate WAL v5 or snapshot schema v8 into the target store;
- maintain an old handler adapter;
- expose both `HandlerOutput` and `AttemptDisposition` in the released runtime;
- retain transient custom external events as a fallback;
- add an ActionQueue-level `Uncertain` effect state;
- make ActionQueue understand Vessels, commitments, communication, effects, receipts, identities, signatures, or authorization;
- build a distributed queue or consensus system;
- introduce hierarchical budgets or multi-tenant fair scheduling in the first target release;
- change the Rust toolchain merely because a breaking architecture release is occurring;
- implement campaign, intervention-arm, benchmark, task-distribution, capability-profile, representation-profile, binding-constraint, saturation, or candidate-acceptance types;
- add a free-form experiment metadata channel, campaign-specific scheduler branch, or statistical evaluator;
- infer a new experimental sample from an ActionQueue retry or physical attempt.

---

## 1. Baseline source map

The current repository already has the correct broad decomposition: eleven crates in a strict dependency DAG, a WAL-backed mutation authority, deterministic recovery, a pure run state machine, and feature-gated workflow, budget, actor, and platform layers. The target should preserve that shape rather than collapse it into a rewrite monolith.

### 1.1 Current crates and target impact

| Crate | Existing responsibility | `AQ-CONT-1` disposition | Change intensity |
|---|---|---|---:|
| `actionqueue-core` | Pure IDs, tasks, runs, transitions, constraints, actor/platform domain types | Add target vocabulary and state; rename routing terminology; remain I/O-free | High |
| `actionqueue-storage` | WAL, snapshots, mutation authority, replay/recovery | Establish fresh format, compound records, target projections, manifest, backup/restore | Very high |
| `actionqueue-engine` | Scheduling, derivation, leases, concurrency, eligibility | Add admission planning, wait/deadline eligibility, continuation indexes and transition helpers | High |
| `actionqueue-executor-local` | Handler contract, worker execution, cancellation, attempt result | Replace `HandlerOutput`; add resume context, checkpoint references, lease-fenced result envelope | Very high |
| `actionqueue-runtime` | Dispatch loop, embedded API, composition root | Integrate admission, signals, waits, compound disposition, recovery reconciliation, inspection | Very high |
| `actionqueue-workflow` | DAGs, hierarchy, cron, dynamic submission | Delete best-effort channel; use compound child admissions and wait helpers | High |
| `actionqueue-budget` | Budget tracking, cancellation, suspension, subscription registry | Keep budget semantics; stop owning general external continuation; bridge structural events | Medium |
| `actionqueue-actor` | Remote actor registration, heartbeat, routing, result reporting | Rename traits; extend result/resume protocol; enforce fences and tenant context | Medium-high |
| `actionqueue-platform` | Tenancy, RBAC, ledgers, approval workflows | Add control-operation permissions and attribution; preserve isolation | Medium |
| `actionqueue-daemon` | HTTP API and metrics | Replace target endpoints and inspection surfaces; reject old store formats | Medium-high |
| `actionqueue-cli` | Daemon, submit, stats | Add target commands; remove obsolete terminology | Medium |

### 1.2 Current surgical points

The following files are not the only files that will change, but they are the clearest entry points into the present design:

| Current path | Current role | Target change |
|---|---|---|
| `crates/actionqueue-core/src/run/state.rs` | Defines nine current states, including `Suspended` but not `Awaiting` | Add `Awaiting`; update display, terminal logic, and documentation |
| `crates/actionqueue-core/src/run/transitions.rs` | Valid transition table | Add exact `Running → Awaiting`, `Awaiting → Ready/Failed/Canceled` rules and exhaustive tests |
| `crates/actionqueue-executor-local/src/handler.rs` | `HandlerInput`, `HandlerOutput`, `TaskSubmissionPort`, `ExecutorContext` | Replace with target handler input, `AttemptDisposition`, `ResumeContext`, and no fire-and-forget submission port |
| `crates/actionqueue-workflow/src/submission.rs` | Unbounded, fire-and-forget dynamic submission channel | Delete after compound child admission lands |
| `crates/actionqueue-core/src/mutation/mod.rs` | Typed single-mutation command surface | Replace/extend with target commands and purpose-built compound admission/disposition commands |
| `crates/actionqueue-core/src/subscription.rs` | Tick-evaluated filters including `Custom` | Separate internal reactive subscriptions from external durable signals; remove `Custom` external semantics |
| `crates/actionqueue-core/src/event.rs` | In-memory tick events used for subscription matching | Retain internal queue events where useful; remove their use as durable external wake-up |
| `crates/actionqueue-core/src/actor.rs` | `ActorCapabilities` free-form routing strings | Rename to `ExecutorTraits` and make routing-only semantics explicit |
| `crates/actionqueue-core/src/task/constraints.rs` | `required_capabilities` and concurrency/retry constraints | Rename routing fields; add wait-time concurrency policy |
| `crates/actionqueue-storage/src/mutation/authority.rs` | Sole validated persistence mutation authority | Add scratch validation and atomic commit for target record families |
| `crates/actionqueue-storage/src/recovery/*` | Replay and projection reconstruction | Reconstruct admissions, waits, signals, checkpoints, causal indexes, and pending reconciliation |
| `crates/actionqueue-storage/src/snapshot/*` | Snapshot model and loading | Replace with target schema and projection digest |
| `crates/actionqueue-runtime/src/dispatch.rs` | Central orchestration and mutation integration | Split by responsibility as target behavior lands; remain one authority lane |

### 1.3 Existing behavior classification

Before writing target code, classify existing behavior into three buckets.

**Retain and re-prove**

- run-policy accounting;
- terminal-state finality;
- retry caps;
- lease fencing and expiry;
- concurrency-key enforcement;
- DAG cycle rejection and dependency gating;
- parent-child cancellation and completion rules that remain intentional;
- WAL-first mutation authority;
- deterministic replay;
- snapshot-as-acceleration rather than source of truth;
- budget consumption and preemptive suspension;
- actor heartbeat and tenant isolation where semantics remain valid.

**Replace**

- generic waiting through `Suspended`;
- `HandlerOutput`;
- fire-and-forget child submission;
- transient custom external events;
- actor “capabilities” terminology;
- current public task-admission semantics where durability is not returned as a result;
- checkpoint bytes hidden inside suspended output;
- control mutations without complete causal attribution.

**Reject as target behavior**

- silent child-submission loss;
- external signal loss across restart;
- waiting while holding an execution lease;
- caller metadata being treated as authorization;
- blind retry as a response to application-level uncertain external effects;
- old-store automatic opening or mutation;
- live dual authority or fallback to a pre-contract path.

---

## 2. Delivery model and repository workflow

### 2.1 Baseline freeze

Create an immutable source anchor before destructive changes:

```text
tag: actionqueue/pre-aq-cont-1
commit: 97c9dc26c19c697dbfb204ed503e82c5f053394f
```

Archive under a clearly non-runtime location:

```text
archive/pre-aq-cont-1/
├── README.md
├── selected-wal-fixtures/
├── selected-snapshot-fixtures/
├── characterization-results/
├── crash-scenarios/
├── performance-baseline/
└── known-failure-cases/
```

The archive is evidence. Target crates must not depend on it. A test may invoke a frozen binary or offline parser for diagnostic comparison, but target runtime code must not read or upgrade those files.

### 2.2 Branch model

Use one long-lived integration branch:

```text
main @ frozen experimental baseline
    └── aq-cont-1
          ├── PR AQ-01
          ├── PR AQ-02
          └── ...
```

Rules:

- all implementation PRs target `aq-cont-1`;
- every merged PR compiles and passes the gates assigned to its stage;
- no runtime feature flag selects old versus new semantics;
- temporary compile scaffolding may exist only when it cannot become runtime authority and is deleted by its named removal PR;
- downstream repositories use exact pinned Git revisions only after the relevant ActionQueue interface gate is stable;
- `aq-cont-1` merges to `main` only after the complete conformance and release gate passes.

This isolates incomplete breaking work without creating two live products.

### 2.3 Review policy

Every PR description should include:

```text
Contract clauses implemented:
Invariant IDs covered:
Persistence records added or changed:
Crash points tested:
Public APIs added or removed:
Forbidden domain concepts checked:
Downstream contract unlocked:
```

A PR that changes a durable record, transition, matching rule, or digest algorithm requires an ADR reference and replay tests in the same PR.

### 2.4 Toolchain and dependency posture

Keep the current pinned Rust toolchain for the first implementation sequence unless a target requirement cannot be implemented on it. A toolchain bump should be an isolated decision, not hidden inside lifecycle work.

Dependency rules:

- prefer existing dependencies where they satisfy the target contract;
- add a cryptographic hash dependency only after the admission-digest ADR fixes the algorithm and canonical representation;
- do not add a database to avoid designing the WAL projections;
- do not add a message broker to implement durable signals;
- do not extract `actionqueue-continuation` into a new crate until profiling and dependency pressure justify it.

---

## 3. Architectural decisions to lock before dependent code

The architecture document lists several ADRs. This implementation plan turns them into a decision queue and recommends defaults. A recommended default is treated as the working implementation choice unless code review produces a concrete counterexample.

| ADR | Must be fixed before | Recommended implementation decision |
|---|---|---|
| `AQ-ADR-001 Store identity and format lineage` | AQ-03 | Human-readable `manifest.json` with `contract = "AQ-CONT-1"`, `wal_format = 1`, `snapshot_schema = 1`; refuse nonempty directories without a recognized target manifest |
| `AQ-ADR-002 Admission canonicalization` | AQ-04 | Build a dedicated normalized `CanonicalAdmission` value; hash versioned canonical bytes, not arbitrary `serde` output or in-memory object layout |
| `AQ-ADR-003 Admission hash algorithm` | AQ-04 | SHA-256 or another broadly implemented cryptographic digest; store algorithm ID with digest to preserve future agility |
| `AQ-ADR-004 Signal matching grammar` | AQ-05 | Exact tenant plus namespace/kind, optional exact correlation/source filters; no arbitrary predicates or regex in `AQ-CONT-1` |
| `AQ-ADR-005 Signal ordering and cursor` | AQ-05 | Monotonic per-store `SignalSequence`; `WaitSpec` includes a lower bound; lowest eligible sequence wins under `FirstMatch` |
| `AQ-ADR-006 Signal retention` | AQ-05 | Conservative retention with explicit pins and configurable horizon; no automatic deletion of signals referenced by active or historical waits |
| `AQ-ADR-007 Wait cardinality` | AQ-06 | Exactly one active wait per run; multiple conditions are represented inside one bounded `WaitSpec` only if their semantics remain deterministic |
| `AQ-ADR-008 Wait timeout race` | AQ-06 | WAL sequence order determines signal-versus-deadline winner; first committed resolution is final |
| `AQ-ADR-009 Concurrency key while awaiting` | AQ-06 | Default `ReleaseWhileAwaiting`; opt-in `HoldWhileAwaiting` is explicit in task constraints |
| `AQ-ADR-010 Checkpoint representation` | AQ-07 | `DataRef::{Inline, External}`; immutable hash; strict inline and disposition-size limits; ActionQueue does not fetch external data |
| `AQ-ADR-011 Disposition invalidity` | AQ-08 | Handler semantic invalidity terminally fails the run with a bounded engine error; impossible projection/storage mismatch halts mutation processing |
| `AQ-ADR-012 Attempt accounting` | AQ-08 | Physical attempt ordinal and failure-attempt count are separate; `Awaiting` and `Suspended` do not consume retry allowance |
| `AQ-ADR-013 Parent completion with children` | AQ-09 | Preserve explicit completion gating; parent cannot complete while required children are nonterminal unless policy explicitly declares detached children |
| `AQ-ADR-014 Causal inheritance` | AQ-09 | Children inherit trace and purpose by default; new causation points to parent attempt; explicit bounded override only for requester/origin refs |
| `AQ-ADR-015 Reactive subscription ownership` | AQ-10 | Internal queue-event subscriptions remain a separate mechanism; external `Custom` events are deleted in favor of durable signals |
| `AQ-ADR-016 Control authentication hook` | AQ-11 | ActionQueue records host-attested `ControlMutationContext`; authentication implementation remains daemon/platform responsibility |
| `AQ-ADR-017 Remote actor result envelope` | AQ-11 | Every result carries run, attempt, lease fence, disposition digest, and target contract revision; stale results cannot mutate state |
| `AQ-ADR-018 Crate extraction` | post-release | Keep continuation modules in current crates for `AQ-CONT-1`; reconsider extraction after dependency and performance evidence |

### 3.1 Decisions that are not open during implementation

The following are contract commitments, not design options:

- `Awaiting` is distinct from `Suspended`.
- A run owns no execution lease while `Awaiting`.
- A signal can arrive before or after a wait and still satisfy it exactly once.
- Signals are immutable fan-out facts, not globally consumed queue messages.
- A wait is resolved at most once.
- A task admission key cannot silently refer to two semantic tasks.
- A parent cannot wait for a child that was never durably admitted.
- ActionQueue does not interpret an external effect as uncertain.
- Causal attribution does not grant execution authority.
- executor traits only influence routing.
- old store formats are not opened by the target runtime.

---

## 4. Target module map

The following paths are proposed implementation boundaries. Names may change during review, but the dependency direction and ownership must not.

### 4.1 `actionqueue-core`

```text
crates/actionqueue-core/src/
├── admission.rs
├── causal.rs
├── data_ref.rs
├── continuation/
│   ├── mod.rs
│   ├── checkpoint.rs
│   ├── resume.rs
│   ├── signal.rs
│   └── wait.rs
├── ids/
│   ├── admission.rs
│   ├── checkpoint.rs
│   ├── correlation.rs
│   ├── signal.rs
│   ├── trace.rs
│   └── wait.rs
├── mutation/
│   ├── mod.rs
│   ├── admission.rs
│   ├── attempt.rs
│   ├── control.rs
│   └── signal.rs
├── run/
│   ├── state.rs
│   └── transitions.rs
└── executor.rs
```

Responsibilities:

- validated pure value objects;
- bounded strings and opaque references;
- target lifecycle and transition rules;
- command and outcome shapes;
- no file I/O, indexes, network, clocks, or signature verification.

### 4.2 `actionqueue-storage`

```text
crates/actionqueue-storage/src/
├── format/
│   ├── manifest.rs
│   ├── version.rs
│   └── limits.rs
├── wal/
│   ├── frame.rs
│   ├── record.rs
│   ├── reader.rs
│   └── writer.rs
├── projection/
│   ├── mod.rs
│   ├── admission.rs
│   ├── continuation.rs
│   ├── execution.rs
│   └── indexes.rs
├── mutation/
│   ├── authority.rs
│   ├── validate_admission.rs
│   ├── validate_disposition.rs
│   └── validate_control.rs
├── recovery/
│   ├── replay.rs
│   ├── reconcile.rs
│   └── projection_digest.rs
└── snapshot/
    ├── model.rs
    ├── build.rs
    ├── loader.rs
    └── writer.rs
```

Responsibilities:

- target store recognition;
- append and sync authority;
- deterministic reduction;
- scratch validation for compound operations;
- no semantic interpretation of opaque references.

### 4.3 `actionqueue-engine`

```text
crates/actionqueue-engine/src/
├── admission/
│   ├── planner.rs
│   └── digest.rs
├── continuation/
│   ├── deadline_index.rs
│   ├── match_index.rs
│   ├── matcher.rs
│   └── reconcile.rs
├── scheduler/
├── lease/
├── concurrency/
└── derive/
```

Responsibilities:

- pure or in-memory planning/index algorithms;
- matching candidates and deterministic ordering;
- deadline eligibility;
- no WAL writes except through storage authority contracts.

### 4.4 `actionqueue-executor-local`

```text
crates/actionqueue-executor-local/src/
├── handler.rs
├── disposition.rs
├── input.rs
├── worker.rs
├── lease_fence.rs
└── cancellation.rs
```

Responsibilities:

- one target handler trait;
- `AttemptDisposition` construction and local validation;
- `ResumeContext` delivery;
- worker result fencing;
- no direct child-task mutation port.

### 4.5 `actionqueue-runtime`

The current dispatch module should be decomposed as behavior lands rather than rewritten all at once:

```text
crates/actionqueue-runtime/src/
├── dispatch/
│   ├── mod.rs
│   ├── admission.rs
│   ├── attempts.rs
│   ├── continuations.rs
│   ├── controls.rs
│   └── recovery.rs
├── api/
│   ├── admission.rs
│   ├── signals.rs
│   ├── waits.rs
│   └── inspection.rs
├── engine.rs
├── config.rs
└── metrics.rs
```

The dispatch authority remains serialized. Module splitting is for reviewability, not concurrent mutation ownership.

### 4.6 Feature crates

- `actionqueue-workflow`: replace `submission.rs` with `child_admission.rs` and wait-target helpers.
- `actionqueue-budget`: retain budget tracker/gates; remove general external custom-event ownership.
- `actionqueue-actor`: rename routing types and implement the target result envelope.
- `actionqueue-platform`: add typed permissions for task admission, signal admission, wait inspection/resolution, cancellation, and reprioritization.
- `actionqueue-daemon` and `actionqueue-cli`: expose only target APIs and target store diagnostics.

---

## 5. Critical path and parallel lanes

### 5.1 Critical path

```text
AQ-01 Contract and evidence freeze
    ↓
AQ-02 Core vocabulary and lifecycle
    ↓
AQ-03 Fresh persistence lineage
    ↓
AQ-04 Compound idempotent admission
    ↓
AQ-05 Durable signal admission
    ↓
AQ-06 Awaiting, waits, deadlines, and matching
    ↓
AQ-07 Checkpoints and resume context
    ↓
AQ-08 AttemptDisposition and executor cutover
    ↓
AQ-09 Workflow and transactional children
    ↓
AQ-10 Budget and reactive-event separation
    ↓
AQ-11 Remote actor, platform, and control attribution
    ↓
AQ-12 Runtime API, daemon, CLI, and observability
    ↓
AQ-13 Conformance, crash, model, and performance suite
    ↓
AQ-14 Release cutover and downstream handoff
```

### 5.2 Work that may proceed in parallel

After AQ-02:

- documentation and ADR refinement;
- construction of pure state-machine/property tests;
- design of conformance fixture schemas;
- offline characterization of current acceptance scenarios.

After AQ-03:

- CLI/inspection mockups against target query types;
- test-only crash-point infrastructure;
- projection-digest tooling;
- backup/restore harness.

After AQ-06:

- WorldInterface may prototype an unpublished adapter against pinned ActionQueue Git, but cannot claim target conformance;
- ActionQueue actor protocol design can proceed using the fixed continuation types.

The critical path must not be shortened by implementing duplicate persistence or matching logic in downstream crates.

---

## 6. Pull-request implementation sequence

### AQ-01 — Freeze evidence and establish the development contract

**Objective:** Create the clean-break development environment and make the target contract executable as checks before behavior changes.

**Primary changes**

- tag `actionqueue/pre-aq-cont-1`;
- create `aq-cont-1` integration branch;
- add `docs/contracts/AQ-CONT-1.md` or link the normative architecture;
- add `conformance/aq-cont-1/manifest.yaml` with contract revision and empty fixture inventory;
- add `docs/adrs/` with the ADR queue from Section 3;
- archive selected old fixtures outside runtime crates;
- add a forbidden-domain vocabulary check for target code and conformance packages;
- commit `aq-cont-1-developmental-campaign-execution-profile.md` and `aq-cont-1-developmental-campaign-acceptance-matrix.yaml`;
- add a check forbidding developmental domain types, free-form experiment metadata, and campaign-specific scheduler interpretation in queue core;
- add a forbidden-legacy-symbol check that will become stricter as removal PRs land;
- record baseline test and benchmark results without making them target thresholds.

**Current tests to freeze as evidence**

- once/repeat accounting;
- retry cap;
- crash recovery;
- lease expiry;
- WAL and snapshot corruption;
- DAG ordering/cycle rejection;
- dynamic submission failure cases;
- suspend/resume and subscription behavior;
- actor routing and tenant isolation.

**Required outputs**

```text
conformance/aq-cont-1/manifest.yaml
docs/contracts/aq-cont-1-developmental-campaign-execution-profile.md
conformance/aq-cont-1/aq-cont-1-developmental-campaign-acceptance-matrix.yaml
docs/adrs/AQ-ADR-001-*.md ...
archive/pre-aq-cont-1/README.md
scripts/check_contract_boundaries.*
```

**Tests/gates**

- current baseline suite remains green before structural edits;
- archive is not referenced from any target crate dependency;
- contract check rejects domain words such as `Vessel`, `Commitment`, `EffectIntent`, `Receipt`, and `Narrative` from target public types, allowing them only in explanatory tests/examples where clearly opaque;
- contract check also rejects queue-owned `Campaign`, `InterventionArm`, `BindingConstraint`, `Saturation`, benchmark-score, and free-form metadata types;
- CI reports the pinned contract revision and developmental profile revision.

**Exit criterion:** The repository can state exactly what is frozen, what will be removed, and which conformance revision future PRs implement.

---

### AQ-02 — Introduce target core vocabulary, state, and routing terminology

**Objective:** Establish all pure domain types and lifecycle rules that later persistence and runtime work depend upon.

**Primary crates:** `actionqueue-core`; compile updates across all dependents.

**Changes**

- add IDs: `AdmissionKey`/`AdmissionId` as selected, `WaitId`, `SignalId`, `SignalSequence`, `CheckpointId`, `TraceId`, `CorrelationId`;
- add bounded `OpaqueRef`, signal namespace/kind, bounded errors, content hashes, and data-reference shells;
- add `CausalContext` and `ControlMutationContext` pure types;
- document that `correlation_id` and `origin_ref` are neutral opaque attribution and cannot affect scheduling or authority;
- add `SignalEnvelope`, `SignalFilter`, `WaitSpec`, `CheckpointRef`, `ResumeContext` domain shapes;
- add `Awaiting` to `RunState`;
- add exact transitions and transition-rejection reasons;
- add `ConcurrencyKeyWaitPolicy` with `ReleaseWhileAwaiting` default;
- rename `ActorCapabilities` to `ExecutorTraits`;
- rename `required_capabilities` to `required_executor_traits` throughout public APIs, tests, docs, and platform routing;
- introduce target `AdmissionPlan`, `AdmissionDigest`, `AttemptDisposition`, and command shapes as pure types, even where runtime support lands later;
- prohibit invalid combinations through constructors wherever practical.

**Implementation guidance**

- use private fields and validating constructors for invariant-sensitive values;
- distinguish absence of a filter from wildcard syntax; do not accept arbitrary empty strings;
- keep timestamps and signal sequence semantics explicit rather than overloading one integer;
- do not add matching indexes or I/O to core;
- do not add a campaign/arm/evaluation type or free-form metadata map; rich developmental state remains in an application-owned immutable record behind one bounded `origin_ref`.

**Tests/gates**

- exhaustive valid-transition table;
- property test: terminal states never leave terminality;
- property test: `Awaiting` can originate only from `Running`;
- constructor rejection tests for empty/broad/unbounded identifiers;
- serialization round trips for all durable target types;
- compile-fail or API tests showing a routing trait cannot be passed as an authorization object;
- no `ActorCapabilities` or `required_capabilities` symbol remains.

**Exit criterion:** All downstream crates compile against the target vocabulary, even though behavior is not yet fully wired.

---

### AQ-03 — Establish the fresh `AQ-CONT-1` persistence lineage

**Objective:** Replace the target store identity and durable record model before adding new behavior.

**Primary crates:** `actionqueue-storage`, `actionqueue-core`, `actionqueue-runtime` bootstrap.

**Changes**

- add a target `StoreManifest` containing at minimum contract ID, WAL format, snapshot schema, creation metadata, and feature compatibility data;
- define `UnsupportedStoreFormat` and `MissingTargetManifest` errors;
- refuse nonempty pre-contract data directories without modifying them;
- define target WAL framing and durable record enum;
- port retained baseline semantics into the target record/reducer model;
- define record variants or reserved families for:
  - compound admission;
  - attempt start and compound disposition;
  - signal admission;
  - wait establishment/satisfaction/timeout/cancellation;
  - control operations;
  - actor/platform records where enabled;
- create target snapshot schema and projection version;
- add deterministic projection hashing;
- add backup and restore commands for target stores;
- preserve trailing-corruption handling only where it remains correct under the new framing.

**Recommended durable-store behavior**

```text
empty directory
    → initialize target manifest and store

target manifest with supported versions
    → open and recover

nonempty directory without target manifest
    → fail closed, no writes

future unsupported target version
    → fail closed with exact version error
```

**Tests/gates**

- empty-store initialization;
- target reopen and replay;
- old fixture rejection without byte changes;
- WAL-only and snapshot-plus-tail projection hash equality;
- truncated/corrupt tail handling;
- backup/restore projection equality;
- unsupported future version rejection;
- feature-set compatibility validation.

**Exit criterion:** Every subsequent semantic record can land in a versioned target store with deterministic replay. No old reader is linked into the runtime.

---

### AQ-04 — Implement compound idempotent task admission

**Objective:** Make task existence a durable, knowable, replay-stable result.

**Primary crates:** `actionqueue-core`, `actionqueue-engine`, `actionqueue-storage`, `actionqueue-runtime`.

**Public contract**

```rust
pub struct EnsureTaskRequest {
    pub admission_key: AdmissionKey,
    pub task_spec: TaskSpec,
    pub dependencies: Vec<TaskId>,
    pub causal_context: CausalContext,
    pub control_context: Option<ControlMutationContext>,
}

pub enum EnsureTaskOutcome {
    Created { task_id: TaskId, digest: AdmissionDigest },
    AlreadyExists { task_id: TaskId, digest: AdmissionDigest },
}
```

A mismatched digest returns a typed conflict and does not mutate state.

**Changes**

- implement normalized canonical admission representation;
- implement versioned digest calculation;
- build `AdmissionPlan` including task, derived runs, parentage, dependencies, tenant context, and causal context;
- validate the whole plan against a scratch projection;
- append one `AdmissionCommitted` record;
- atomically project task, derived runs, dependency edges, hierarchy edges, admission-key index, and causal context;
- expose embedded `ensure_task`;
- make ordinary submit APIs thin target-oriented conveniences that still require or derive an explicit admission key according to documented policy;
- add bounds for dependencies, child count, payload size, and compound record size.

**Pseudocode**

```text
ensure_task(request):
    canonical = normalize(request)
    digest = hash(contract_revision || canonical)

    if admission_key exists:
        if stored_digest == digest:
            return AlreadyExists(stored_task_id, digest)
        else:
            return AdmissionConflict

    plan = derive_task_runs_edges(request)
    validate(plan, scratch_projection)
    append_and_sync(AdmissionCommitted(plan, digest))
    apply(plan)
    return Created(task_id, digest)
```

**Tests/gates**

- first admission creates exactly one task and implied runs;
- exact repeat returns `AlreadyExists` after restart;
- same key with changed payload, constraints, dependencies, parentage, tenant, or causal context conflicts according to canonicalization policy;
- crash after append before response returns `AlreadyExists` on retry;
- no partial task/run/dependency visibility at any crash point;
- concurrent identical admissions converge to one task;
- concurrent conflicting admissions produce one winner and one conflict;
- replay reconstructs the admission index;
- admission key is tenant-scoped as specified.

**Exit criterion:** A cross-engine outbox can safely retry task creation without duplicates or ambiguity.

---

### AQ-05 — Add durable signal admission and retained signal indexing

**Objective:** Make externally originated wake-up facts durable, idempotent, bounded, and replayable before introducing waits.

**Primary crates:** `actionqueue-core`, `actionqueue-engine`, `actionqueue-storage`, `actionqueue-runtime`.

**Changes**

- implement `admit_signal` with host-attested control context;
- assign monotonic `SignalSequence` at durable admission;
- deduplicate by tenant and `SignalId` or declared deduplication key;
- detect conflict when the same identity carries different canonical content;
- persist immutable payload reference/hash rather than unbounded inline data;
- build indexes by tenant, namespace, kind, correlation, source ref, sequence, and retention status;
- implement inspection queries and retention pins;
- add backpressure and serialized-size limits;
- explicitly keep internal `ActionQueueEvent` separate from durable signals.

**Public contract sketch**

```rust
pub enum AdmitSignalOutcome {
    Admitted { signal_id: SignalId, sequence: SignalSequence },
    AlreadyExists { signal_id: SignalId, sequence: SignalSequence },
}
```

**Tests/gates**

- new signal admitted and replayed;
- exact duplicate idempotent;
- changed content conflicts;
- sequence is monotonic and replay-stable;
- cross-tenant collision cannot wake another tenant;
- oversized field/payload rejected before WAL append;
- crash after append before response is idempotently recoverable;
- retention cannot remove a pinned signal;
- no durable-signal path depends on the budget subscription registry.

**Exit criterion:** ActionQueue can durably accept wake-up facts, even though no run consumes them yet.

---

### AQ-06 — Implement `Awaiting`, durable waits, deadlines, and race-free matching

**Objective:** Deliver the central continuation guarantee: no lost wakeups whether the signal or wait arrives first.

**Primary crates:** `actionqueue-core`, `actionqueue-engine`, `actionqueue-storage`, `actionqueue-runtime`.

**Changes**

- project one active wait per awaiting run;
- implement `WaitSpec` validation and matching indexes;
- implement `Running → Awaiting` and wait establishment as one semantic mutation;
- release lease and, by default, concurrency key in the same committed transition;
- scan already retained eligible signals after wait establishment;
- match newly admitted signals against active waits;
- commit `WaitSatisfied` exactly once and promote run to `Ready` with pending resume metadata;
- add deadline index and deterministic signal-versus-timeout resolution;
- implement wait cancellation as part of run cancellation;
- reconcile waits, signals, and due deadlines after recovery before normal dispatch;
- reject administrative “resume” of unresolved waits; expose explicit resolution/cancellation controls instead.

**No-lost-wakeup algorithm**

```text
Signal first:
    SignalAdmitted S
    ... crash/restart allowed ...
    WaitEstablished W with lower-bound cursor
    matcher finds S
    WaitSatisfied(W,S)
    run → Ready

Wait first:
    WaitEstablished W
    ... crash/restart allowed ...
    SignalAdmitted S
    matcher finds W
    WaitSatisfied(W,S)
    run → Ready
```

A crash between any two durable records is repaired by deterministic reconciliation. Recovery never invents a match without a durable signal and wait.

**Tests/gates**

- complete no-lost-wakeup ordering matrix;
- duplicate signal causes one wake;
- one signal fans out to multiple matching waits;
- one wait resolves once;
- stale signal below cursor does not match;
- exact correlation wins over broad candidates according to policy;
- canceled wait never wakes;
- signal/deadline same-tick ordering is deterministic by WAL sequence;
- awaiting run owns no lease;
- concurrency key is released or retained according to explicit task policy;
- recovery reconciliation is idempotent;
- no wait is represented only in memory.

**Exit criterion:** The continuation state machine is durable and independently proven before handler cutover.

---

### AQ-07 — Add immutable checkpoints and resume context

**Objective:** Ensure a resumed attempt receives exactly the state and wake reason produced by the previous attempt/wait lifecycle.

**Primary crates:** `actionqueue-core`, `actionqueue-storage`, `actionqueue-executor-local`, `actionqueue-runtime`, `actionqueue-actor` type preparation.

**Changes**

- finalize `DataRef::{Inline, External}` and content-hash semantics;
- define hard inline limits for output, checkpoint, signal payload, and total compound record;
- persist immutable `CheckpointRef` records and link them to producing attempts;
- build `ResumeContext` from satisfied wait plus checkpoint;
- ensure pending resume context survives `Ready` state, budget gating, process restart, and lease acquisition;
- deliver resume context exactly once to the next accepted attempt start;
- retain consumed resume context in attempt lineage;
- redact external locators from default logs and metrics;
- define behavior when executor cannot resolve an external checkpoint as ordinary handler failure, not queue reinterpretation.

**Tests/gates**

- inline and external reference round trip;
- content hash preserved;
- resume context survives crash before new attempt starts;
- budget-blocked ready run retains pending context;
- stale or repeated executor start does not consume context twice;
- previous checkpoints remain immutable and queryable;
- logs never print inline checkpoint bytes or sensitive locators by default.

**Exit criterion:** The queue can carry durable continuation state across attempts without interpreting its contents.

---

### AQ-08 — Replace `HandlerOutput` with `AttemptDisposition`

**Objective:** Cut the local executor and runtime over to one complete, lease-fenced end-of-attempt contract.

**Primary crates:** `actionqueue-executor-local`, `actionqueue-core`, `actionqueue-storage`, `actionqueue-runtime`; broad test updates.

**Changes**

- replace the `ExecutorHandler` return type with `AttemptDisposition`;
- remove `HandlerOutput` and all helper constructors;
- remove `TaskSubmissionPort` from `ExecutorContext`;
- include causal context and optional resume context in handler input;
- define outcome-combination validation;
- include lease fence, expected state, attempt identity, and expected mutation sequence in disposition commit;
- atomically persist outcome, output/checkpoint, wait, consumption, child-admission proposals, emitted signals, run transition, lease release, and attempt lineage;
- separate physical attempt count from failure-attempt count;
- reject stale executor results before any subordinate mutation is applied;
- fill emitted signal source/causation from runtime context rather than trusting arbitrary handler-supplied attribution.

**Combination matrix**

| Outcome | Output | Checkpoint | Wait | Children | Signals |
|---|---:|---:|---:|---:|---:|
| `Complete` | optional | no | no | no initially | optional if explicitly allowed |
| `RetryableFailure` | no | no | no | no | no |
| `TerminalFailure` | no | no | no | no | no |
| `Timeout` | no | no | no | no | no |
| `Suspended` | no | optional | no | no | optional structural event only |
| `Awaiting` | no | optional | exactly one | optional | optional |

The exact Rust encoding may make invalid cells unrepresentable rather than validating a loose struct.

**Tests/gates**

- all old handler tests rewritten against `AttemptDisposition`;
- invalid combinations reject before durable mutation;
- stale lease fence commits nothing;
- yielded attempt appears in lineage but does not consume retry cap;
- timeout remains authoritative when cancellation was requested;
- crash after disposition append reconstructs every included child/signal/wait atomically;
- no `HandlerOutput` or `TaskSubmissionPort` symbol remains in target crates or public docs.

**Exit criterion:** The target runtime has one handler result model and can establish continuations through normal execution.

---

### AQ-09 — Rebuild workflow coordination around transactional child admission

**Objective:** Make dynamic fan-out and coordinator waiting durable without synchronous dispatch-loop reentrancy.

**Primary crates:** `actionqueue-workflow`, `actionqueue-runtime`, `actionqueue-storage`, `actionqueue-executor-local`.

**Changes**

- introduce `ChildAdmission` as a bounded component of `AttemptDisposition`;
- canonicalize child admission keys relative to parent/workflow context;
- validate child tasks, runs, dependencies, hierarchy, tenant, depth, and cycle constraints against a scratch projection;
- commit children and parent wait in the same `AttemptDispositionCommitted` record;
- provide helpers for waiting on child terminal success/failure sets without translating all DAG edges into signals;
- preserve DAG dependencies as first-class eligibility gates;
- preserve explicit parent-completion gating and cascade cancellation;
- delete `crates/actionqueue-workflow/src/submission.rs` and all mpsc submission wiring;
- defer synchronous mid-attempt admission until a separate proof shows it cannot deadlock or violate authority serialization.

**Tests/gates**

- parent cannot enter `Awaiting` if any required child admission is invalid;
- crash before compound frame creates no child and no wait;
- crash after frame creates all children, edges, and wait;
- cycle rejection remains atomic;
- repeated parent attempt cannot duplicate children when admission keys repeat;
- child terminal state wakes parent once;
- cascade cancellation cancels active child waits and tasks deterministically;
- workflow recovery produces the same projection from WAL-only and snapshot-plus-tail;
- no fire-and-forget submission code remains.

**Exit criterion:** Coordinator workflows can safely decompose work and suspend without hidden in-memory delivery assumptions.

---

### AQ-10 — Separate budgets and internal reactivity from durable external continuation

**Objective:** Preserve useful budget and queue-event behavior while deleting the conceptual overlap that made external custom events ephemeral.

**Primary crates:** `actionqueue-budget`, `actionqueue-core`, `actionqueue-runtime`, `actionqueue-engine`.

**Changes**

- retain budget tracking, consumption, cancellation token behavior, replenishment, and `Suspended` semantics;
- move or rename the general subscription registry so it no longer appears to own continuation;
- retain typed internal filters for queue facts where useful: task completion, run state changes, budget thresholds;
- delete `EventFilter::Custom` and `ActionQueueEvent::CustomEvent` as external coordination surfaces;
- bridge any application-relevant durable wake-up through `SignalEnvelope` admission;
- define interaction between satisfied waits and exhausted budgets: run becomes `Ready` with pending resume context, but budget gate may prevent leasing;
- define budget behavior while `Awaiting` without inventing domain semantics;
- add retention/inspection for threshold events separately from signal history where appropriate.

**Tests/gates**

- existing budget enforcement and replenishment retained;
- `Suspended` remains distinct from `Awaiting` in state and metrics;
- wait satisfied under exhausted budget retains resume context until lease is allowed;
- custom external event tests are deleted/replaced with durable signal tests;
- budget subsystem restart cannot lose a continuation wake;
- no general continuation registry remains owned by the budget crate.

**Exit criterion:** Budget preemption and durable continuation are separate mechanisms with explicit integration.

---

### AQ-11 — Update remote actors, platform controls, and mutation attribution

**Objective:** Make remote execution and multi-principal control safe under the target lifecycle without turning ActionQueue into an identity or authorization system.

**Primary crates:** `actionqueue-actor`, `actionqueue-platform`, `actionqueue-runtime`, `actionqueue-core`, `actionqueue-daemon` protocol types.

**Changes**

- replace actor capability declarations with `ExecutorTraits`;
- update routing indexes and task constraints;
- extend remote claim/result protocol with contract revision, run ID, attempt ID, lease fence, disposition digest, and optional resume context;
- reject stale actor dispositions before any output, child, wait, checkpoint, signal, or consumption mutation;
- propagate tenant and causal context to remote attempts;
- define typed queue-control actions for:
  - task admission;
  - signal admission;
  - wait inspection and explicit resolution/cancellation;
  - task/run cancellation;
  - suspension/resumption;
  - reprioritization where supported;
- attach host-attested `ControlMutationContext` to every mutating control record;
- extend platform RBAC to govern those queue-control operations without interpreting external resource authority;
- preserve actor heartbeat semantics and deterministic deregistration.

**Tests/gates**

- trait matching works and old terminology is absent;
- actor cannot self-declare authorization by adding a trait;
- stale result cannot commit subordinate state;
- cross-tenant signal/wait/admission attempts fail;
- control mutation record identifies caller/context supplied by authenticated host;
- forged payload metadata cannot replace host-attested control context;
- actor crash/lease expiry remains recoverable;
- remote resume context delivered exactly once to accepted attempt.

**Exit criterion:** Embedded and remote execution share the same target result, fence, continuation, and attribution contract.

---

### AQ-12 — Complete embedded API, daemon, CLI, inspection, and observability

**Objective:** Expose the target contract operationally and make every continuation/admission state inspectable.

**Primary crates:** `actionqueue-runtime`, `actionqueue-daemon`, `actionqueue-cli`; query support across storage/engine.

**Embedded API surface**

```text
ensure_task
admit_signal
get_admission
get_task / get_run / get_attempt
get_wait / list_waits
get_signal / list_signals
cancel_task / cancel_run
resolve_or_cancel_wait (explicit control path)
backup / restore / inspect_store
```

**Daemon guidance**

- mutating endpoints are disabled unless control mode and authentication hook are configured;
- `Created`, `AlreadyExists`, and digest conflict map to distinct HTTP outcomes;
- signal duplicate and conflict are distinct;
- inspection endpoints redact inline data and external locators by default;
- all mutating requests produce `ControlMutationContext` before entering runtime;
- no anonymous compatibility principal exists.

**CLI commands**

```text
actionqueue ensure-task
actionqueue signal admit
actionqueue signal inspect
actionqueue wait inspect
actionqueue wait cancel
actionqueue run inspect
actionqueue trace
actionqueue trace --correlation <id>
actionqueue inspect --origin-ref <opaque-ref>
actionqueue store inspect
actionqueue backup
actionqueue restore
```

**Metrics**

At minimum:

```text
actionqueue_admission_total{outcome}
actionqueue_admission_conflict_total
actionqueue_signals_admitted_total{namespace,kind}
actionqueue_signal_duplicate_total
actionqueue_waits_active
actionqueue_waits_satisfied_total{reason}
actionqueue_wait_latency_seconds
actionqueue_runs_awaiting
actionqueue_resume_context_pending
actionqueue_disposition_rejected_total{reason}
actionqueue_recovery_reconciliations_total{kind}
actionqueue_compound_record_bytes
actionqueue_projection_digest_mismatch_total
```

Avoid unbounded labels such as raw correlation IDs, principal refs, signal IDs, campaign IDs, intervention-arm IDs, task-distribution IDs, or component fingerprints.

**Tests/gates**

- embedded and daemon results are semantically identical;
- all mutating endpoints require configured host authorization;
- CLI JSON output has stable target schemas;
- trace output links task, run, attempt, wait, signal, checkpoint, and opaque causal refs;
- authorized filtering by exact correlation or origin reference remains structural and redacted;
- inspection exposes ordinary priority, budget, deadline, constraint, and executor-trait differences without interpreting them;
- no output declares a campaign winner, benchmark score, binding constraint, saturation state, or candidate acceptance;
- readiness fails during recovery reconciliation or projection mismatch;
- metrics do not expose sensitive payloads or high-cardinality IDs;
- no old endpoint, old store reader, or compatibility alias remains.

**Exit criterion:** Operators and downstream systems can use and diagnose every target primitive without reaching into internal projections, while developmental references remain opaque and queue execution remains visibly distinct from evaluation.

---

### AQ-13 — Build the target conformance, model, chaos, and performance suite

**Objective:** Prove the contract under crash, replay, race, feature-combination, and adversarial inputs.

**Primary location:** `conformance/aq-cont-1`, root acceptance/chaos tests, crate property tests.

**Conformance package contents**

```text
conformance/aq-cont-1/
├── manifest.yaml
├── fixtures/
│   ├── admissions/
│   ├── signals/
│   ├── waits/
│   ├── checkpoints/
│   └── compound-dispositions/
├── drivers/
├── crash-matrix/
├── expected-projections/
├── aq-cont-1-developmental-campaign-acceptance-matrix.yaml
└── README.md
```

**Required proof layers**

1. **Pure domain/property tests** — transitions, validation, canonicalization, matching.
2. **Storage/replay tests** — every record family, snapshots, corruption, projection digests.
3. **Runtime acceptance tests** — embedded behavior and feature combinations.
4. **Kill/restart tests** — deterministic crash points around every semantic boundary.
5. **Black-box conformance drivers** — usable by WorldInterface without linking private modules.
6. **Developmental neutrality tests** — prove that bounded campaign attribution changes no scheduling or authority behavior and that execution outcomes remain non-evaluative.

**Deterministic crash points**

Add test-only hooks around:

```text
after compound record serialization
before WAL append
after append before sync
after sync before projection apply
after admission commit before response
after signal commit before matcher
after wait commit before scan
after WaitSatisfied commit before Ready dispatch
after checkpoint/disposition commit before lease release response
after snapshot write before rename
after snapshot rename before old WAL cleanup
```

The hook mechanism must not ship as an uncontrolled production mutation path.

**Model testing**

Build a small pure reference model for generated sequences involving:

- one to three tasks;
- one run each;
- admission retries/conflicts;
- signal-before-wait and wait-before-signal;
- timeout and cancellation races;
- lease expiry and stale results;
- child admission and parent waiting.

Compare projected state and terminal history with the implementation after every generated prefix.

**Performance work**

Measure and record:

- ordinary task admission without continuation features;
- signal admission with no matching waits;
- wait establishment with no retained match;
- signal matching fan-out;
- recovery with increasing signal/wait histories;
- snapshot size and build time;
- compound disposition serialization size;
- completion throughput when continuation is unused.

Do not set arbitrary public throughput claims. Establish reproducible benchmark fixtures and reject algorithmic regressions or unbounded scans.

**Tests/gates**

- complete invariant traceability matrix;
- complete no-lost-wakeup matrix;
- exact duplicate/conflict matrices for admissions and signals;
- WAL-only versus snapshot-plus-tail equality for every fixture;
- all feature combinations compile and pass;
- old store fixture rejection;
- forbidden legacy symbols absent;
- WorldInterface reference adapter passes the published black-box suite;
- all eighteen `AQ-DD-*` developmental-campaign cases pass with required replay and crash variants;
- no campaign-specific public type, persisted field, scheduler path, metric label, score, winner, binding-constraint, saturation, or candidate-acceptance output exists;
- all conformance fixture hashes recorded in manifest.

**Exit criterion:** `AQ-CONT-1` behavior and the additive developmental execution profile are independently executable and pin-able by downstream repositories.

---

### AQ-14 — Release cutover and downstream handoff

**Objective:** Merge the single target implementation to `main`, publish the breaking release, and provide WorldInterface a stable contract.

**Recommended release identity**

- contract label: `AQ-CONT-1`;
- recommended crate semantic version: `0.2.0` for the currently published `0.1.2` crates;
- the contract label, store manifest, and conformance revision remain more precise than the crate version alone.

**Changes/checklist**

- remove temporary integration-only diagnostics and offline comparison harnesses that no longer add value;
- confirm target crates contain no pre-contract reader or adapter;
- confirm current architecture docs and README describe only target APIs;
- set crate versions consistently for the breaking release;
- publish `conformance/aq-cont-1` revision and immutable fixture hashes;
- publish the additive developmental campaign execution profile and its eighteen-case matrix, explicitly declaring no protocol or authority-path change;
- generate API documentation and downstream integration examples for both ordinary continuation and developmental campaign attribution;
- merge `aq-cont-1` to `main`;
- tag the target release;
- publish a release manifest containing:
  - exact commit;
  - crate versions;
  - contract revision;
  - conformance revision;
  - Rust toolchain;
  - store format versions;
  - fixture hashes;
  - developmental profile revision and matrix hash;
  - known limitations;
- give WorldInterface a minimal outbox/`ensure_task`/signal/wait example.

**Release blockers**

- any old store opens successfully;
- any `HandlerOutput`, `TaskSubmissionPort`, `ActorCapabilities`, or external `CustomEvent` path remains;
- any wait can exist only in memory;
- any admission response can be returned before durable commit;
- any stale executor can partially commit;
- any conformance or chaos fixture is nondeterministic without an explicit reason;
- any downstream-required API is private or undocumented;
- any developmental reference changes scheduler or authority behavior;
- any queue surface reports task quality, a winning arm, binding constraint, saturation, or candidate acceptance;
- backup/restore or projection-digest validation is incomplete.

**Exit criterion:** WorldInterface can pin one released ActionQueue implementation and one conformance revision and begin `WI-FABRIC-2` implementation without depending on experimental internals.

---

## 7. Persistence and recovery implementation detail

### 7.1 Target store manifest

A target manifest should be inspectable without opening the WAL:

```json
{
  "contract": "AQ-CONT-1",
  "wal_format": 1,
  "snapshot_schema": 1,
  "created_at": 0,
  "created_by": "actionqueue/<version>",
  "enabled_features": ["workflow", "budget"],
  "hash_algorithms": {
    "admission": "sha-256",
    "content": "sha-256"
  }
}
```

The exact fields require ADR approval. The invariant is that a target process identifies the store before mutation and never guesses based on file shape.

### 7.2 Target durable records

The target likely needs ordinary records plus compound semantic records. The precise Rust enum should be finalized in AQ-03, but the semantic families are:

```text
StoreInitialized
AdmissionCommitted
AttemptStarted
AttemptDispositionCommitted
SignalAdmitted
WaitSatisfied
WaitTimedOut
WaitCanceled
Task/RunControlApplied
LeaseGranted / LeaseExpired where retained
Actor/Platform records under features
SnapshotCommitted metadata where needed
```

`AttemptDispositionCommitted` may include an embedded wait, checkpoint, children, emitted signals, consumption, attempt outcome, and run transition. `AdmissionCommitted` includes every task/run/edge/index fact required for atomic existence.

### 7.3 Projection indexes

The target projection should maintain direct indexes for common correctness paths:

```text
admission_key → digest, task_id
signal_id/dedup_key → canonical digest, sequence
signal sequence → signal record
(tenant, namespace, kind, correlation) → ordered signal sequences
run_id → active wait_id
wait_id → wait state, matcher, checkpoint, resolution
matcher keys → active wait_ids
wait deadline → wait_ids
run_id → pending resume context
task/run/attempt → causal context
concurrency key → current holder
lease fence → active attempt
```

A correct implementation must not linearly scan all signals or waits for the common exact-correlation path.

### 7.4 Scratch validation

Compound operations are validated by cloning or overlaying only the affected projection regions, not by appending and rolling back.

Validation order for a disposition:

```text
identity and lease fence
    → current run/attempt state
    → outcome combination
    → checkpoint/data limits
    → child admission plans and DAG/hierarchy constraints
    → wait validity and uniqueness
    → emitted signal validity
    → tenant and causal rules
    → total serialized size
    → target transitions
```

No partial durable record is emitted if any stage fails.

### 7.5 Recovery phases

```text
1. Read and validate target manifest.
2. Load latest valid target snapshot, if present.
3. Replay WAL tail deterministically.
4. Validate projection invariants and digest.
5. Repair allowed trailing corruption according to policy.
6. Reconcile expired leases.
7. Reconcile active waits against retained signals and due deadlines.
8. Rebuild pending resume indexes.
9. Open dispatch only after reconciliation succeeds.
```

Readiness must remain false through phase 8.

### 7.6 Future evolution policy

Although pre-contract compatibility is rejected, `AQ-CONT-1` must establish future discipline:

- source store is never modified until destination validation succeeds;
- supported upgrades have committed fixtures and projection equivalence tests;
- backups are mandatory before upgrade;
- store manifests identify exact supported versions;
- downgrade is not assumed;
- old target-version readers are removed only through explicit support policy, not accidental refactoring.

---

## 8. Runtime algorithms and invariants

### 8.1 Compound attempt commit

```text
receive executor result
    ↓
verify contract revision and lease fence
    ↓
validate AttemptDisposition against current projection
    ↓
derive one target record
    ↓
append + durability barrier
    ↓
apply projection atomically
    ↓
release lease / concurrency according to outcome
    ↓
run matcher for newly created waits/signals
    ↓
return durable result to executor host
```

The handler may propose. The mutation authority decides whether the proposal is structurally valid.

### 8.2 Signal matching

For exact-correlation waits:

```text
candidate key = (tenant, namespace, kind, correlation_id)
ordered candidates = signals[key] at sequence >= wait.lower_bound
winner = lowest sequence satisfying optional source constraints
```

For signals arriving after waits:

```text
candidate waits = waits indexed by signal's exact keys
sort by stable wait identity or stored registration sequence
for each active matching wait:
    append WaitSatisfied if still unresolved
```

Signals fan out. One wait resolves once. A signal is not globally consumed.

### 8.3 Deadline resolution

The runtime admits a structural timeout resolution through the same authority lane. When signal and deadline are both eligible, the earlier committed resolution wins. Wall-clock sampling may identify eligibility, but WAL order establishes history.

### 8.4 Resume delivery

A `WaitSatisfied` record creates a pending `ResumeContext`. `AttemptStarted` atomically marks that context assigned to the new attempt. If the process dies after assignment but before handler execution is known, ordinary attempt uncertainty/lease expiry rules apply; the context remains linked to the attempt lineage and is not silently delivered to two active attempts.

### 8.5 Cross-instance outbox example

```text
WorldInterface store:
    EffectIntent E
    DispatchOutbox O(E)

Dispatcher:
    ensure_task(
        admission_key = "wi/effect/E",
        digest = canonical target task
    )

ActionQueue:
    Created(T) or AlreadyExists(T)

WorldInterface:
    record T
    mark O delivered
```

If the response is lost, retry is safe. ActionQueue does not need a transaction with the WorldInterface store.

---

## 9. Test migration and new acceptance inventory

### 9.1 Existing acceptance tests

Existing tests should be classified and rewritten rather than mechanically copied.

| Existing test family | Target treatment |
|---|---|
| Once/repeat accounting, retry cap, misfire | Retain and run against target store/handler |
| Crash recovery, WAL/snapshot corruption, concurrent mutation boundary | Retain and expand around compound records |
| Lease expiry, cancellation, concurrency keys | Retain; add awaiting and stale-disposition cases |
| Handler output roundtrip | Replace with `AttemptDisposition` and resume-context round trips |
| DAG ordering/failure/cycle/hierarchy | Retain; add compound child admission and parent wait atomicity |
| Dynamic submission/coordinator tests | Rewrite entirely; no submission channel |
| Suspend/resume | Retain for budget preemption; explicitly contrast with `Awaiting` |
| Subscription/custom-event tests | Keep typed internal event cases; replace custom external event with durable signal tests |
| Actor capability matching | Rename and reframe as executor-trait matching |
| Actor crash and department routing | Retain under target result envelope |
| Platform tenant/RBAC/ledger tests | Retain; add signal/wait/control permissions and attribution |

### 9.2 New acceptance tests

Recommended root test names:

```text
acceptance_idempotent_admission
acceptance_admission_conflict
acceptance_compound_admission_crash
acceptance_signal_deduplication
acceptance_signal_conflict
acceptance_signal_before_wait
acceptance_wait_before_signal
acceptance_duplicate_signal_single_wake
acceptance_wait_timeout_race
acceptance_wait_cancellation
acceptance_awaiting_lease_release
acceptance_awaiting_concurrency_policy
acceptance_checkpoint_resume
acceptance_resume_context_crash
acceptance_stale_disposition_fence
acceptance_compound_disposition_crash
acceptance_transactional_child_admission
acceptance_parent_wait_child_atomicity
acceptance_budget_blocked_resume
acceptance_cross_tenant_signal_isolation
acceptance_control_mutation_attribution
acceptance_target_store_rejects_legacy
acceptance_backup_restore_projection
acceptance_projection_digest_equivalence
acceptance_developmental_reference_neutrality
acceptance_developmental_origin_replay
acceptance_developmental_retry_not_replication
acceptance_developmental_completion_not_quality
acceptance_developmental_no_semantic_outputs
```

### 9.3 Invariant traceability

| Invariant | Minimum proof |
|---|---|
| `AQ-H2 WAL authority` | no durable projection mutation outside authority; concurrent mutation test |
| `AQ-H3 No lost wakeups` | full order/crash matrix |
| `AQ-H4 Atomic wait establishment` | compound record and crash boundary test |
| `AQ-H5 Single replay-stable wake` | duplicate signal and recovery tests |
| `AQ-H6 Admission is knowable` | durable response and lost-response retry |
| `AQ-H7 Conflict detection` | canonical digest matrix |
| `AQ-H8 Parent wait implies durable child` | transactional child crash tests |
| `AQ-H9 Complete disposition` | invalid combination and atomic replay tests |
| `AQ-H10/11 Attribution not authorization` | causal propagation plus host-attestation boundary tests |
| `AQ-H12 Routing traits not capabilities` | type/API and actor routing tests |
| `AQ-H13/14 Awaiting not failure; uncertainty external` | attempt accounting and WorldInterface reference conformance |
| `AQ-H15 Control attribution` | every mutating endpoint/control record |
| `AQ-H16 Version evolution` | manifest, reject-old, backup/restore, future-version tests |
| `AQ-H17 Identity refs opaque` | domain-boundary checks and no verifier dependency |
| `AQ-H18 Agenda review is ordinary work` | scheduled-work and no-quiescent-state workload |
| `AQ-H19 Developmental correlation is neutral` | pairwise identical-task tests varying only correlation/origin refs; scheduler and authorization equivalence |
| `AQ-H20 Execution is not performance judgment` | API/schema/inspection checks plus higher-layer rejected-output scenario |

---

## 10. CI and quality gates

The current repository already runs formatting, clippy, core tests, serde tests, and a broad feature matrix. The target should keep that rigor while separating fast correctness checks from crash-heavy suites.

### 10.1 Proposed CI jobs

**`style-and-api`**

```text
cargo +nightly fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
contract-boundary and forbidden-symbol checks
public API docs build
```

**`core-storage`**

```text
actionqueue-core unit/property tests
actionqueue-storage replay/serialization/corruption tests
projection digest tests
```

**`feature-matrix`**

Retain explicit combinations that have distinct behavior:

```text
default
workflow
budget
workflow,budget
actor
platform
actor,platform
all features
```

Serialize test binaries where the existing runtime still requires it. Do not remove serialization until the blocking-pool behavior has been independently fixed and stress-tested.

**`acceptance-fast`**

- all deterministic acceptance tests excluding process-kill matrix;
- target store rejection;
- API/CLI black-box tests.

**`conformance`**

- execute `conformance/aq-cont-1` drivers against embedded runtime and daemon;
- verify fixture hashes and contract revision.

**`chaos-recovery`**

- deterministic crash-point matrix;
- WAL/snapshot truncation;
- stale executor and concurrent admission races.

**`benchmark-regression`**

- compile and run reproducible benchmark fixtures;
- record results as artifacts;
- fail on algorithmic bounds or agreed regression thresholds, not noisy absolute internet-runner timing.

### 10.2 Pull-request gates by phase

- AQ-01–AQ-03: style, core/storage, current retained feature matrix.
- AQ-04–AQ-08: add admission/continuation acceptance and crash subset.
- AQ-09–AQ-11: add all feature combinations and remote/platform tests.
- AQ-12: add daemon/CLI black-box tests.
- AQ-13 onward: complete conformance and chaos gates mandatory.

### 10.3 Static checks

Add repository checks for:

```text
forbidden target symbols:
    HandlerOutput
    TaskSubmissionPort
    ActorCapabilities
    required_capabilities
    EventFilter::Custom
    ActionQueueEvent::CustomEvent

forbidden domain ownership in public AQ types:
    Vessel
    Commitment
    EffectIntent
    Receipt
    Narrative
    EntityID verifier
    AuthorizationEnvelope
```

The check should allow quoted examples in architecture documentation while rejecting code-level ownership.

---

## 11. Security and abuse-oriented implementation tasks

### 11.1 Admission-key poisoning

- tenant-scope every key where tenancy is enabled;
- store digest and task identity permanently according to task-history retention;
- return conflict details without echoing sensitive payloads;
- rate-limit conflicting attempts at daemon/platform layer.

### 11.2 Signal forgery and replay

- accept signals only through host-authenticated control surfaces;
- store host-attested source/control context separately from opaque payload claims;
- deduplicate structurally;
- enforce bounded namespaces, kinds, correlation IDs, and payload references;
- never treat a signature reference as authority inside ActionQueue.

### 11.3 Cross-tenant wake-up

- tenant is part of signal and wait matching key;
- a missing tenant in platform mode is invalid, not wildcard;
- tests must attempt identical correlation IDs across tenants.

### 11.4 Wait explosion and broad matching

- one active wait per run;
- bounded total waits per tenant/store;
- strict mode requires exact correlation or source plus lower-bound cursor;
- index candidate counts and emit metrics when broad filters are used;
- reject unbounded custom predicates.

### 11.5 Compound-record amplification

- cap child admissions, dependencies, emitted signals, inline bytes, and total serialized record;
- validate size before WAL append;
- use external artifact references for large state;
- expose rejection reasons without storing rejected payloads.

### 11.6 Stale executor commits

- every result includes lease fence and attempt ID;
- authority validates both before any child/wait/signal/checkpoint mutation;
- remote and local paths share the same validation code.

### 11.7 Checkpoint confidentiality

- inline bytes never appear in default logs;
- external locator is redacted by default;
- ActionQueue does not retrieve external content;
- retention documentation makes application responsibility explicit.

---

## 12. Observability and operator questions

The target must answer these questions without replaying application meaning:

```text
Was admission key K ever committed?
Which task and digest does it identify?
Why did a repeated admission conflict?
Why is run R not dispatchable?
Is it Awaiting, Suspended, Ready-but-budget-blocked, or leased?
Which wait is active and what structural filter does it use?
Which signal satisfied it, or which deadline resolved it?
Was the resume context assigned to an attempt?
Which checkpoint was produced and consumed?
Which child tasks were atomically admitted by an attempt?
Which principal/requester references caused this task or control mutation?
Which exact correlation and origin references were attached at admission?
Did explicit priority, budget, deadline, constraint, or executor-trait fields differ between structurally related tasks?
How many physical attempts occurred, and were they retries of one run rather than new application samples?
Did recovery reconcile anything before opening dispatch?
```

A trace view should show structural lineage:

```text
Admission A
  → Task T
    → Run R
      → Attempt A1
        → Wait W + Checkpoint K
          ← Signal S
      → Attempt A2 (resume W/S/K)
        → Complete
```

It should not claim why an application considered the signal meaningful. For developmental work, it also must not claim which arm won, whether a benchmark improved, which constraint was binding, whether the frontier saturated, or whether a candidate should be accepted.

---

## 13. Risk register

| Risk | Failure mode | Mitigation / gate |
|---|---|---|
| Dispatch loop remains a monolith | continuation logic becomes unreviewable and order-sensitive | split by authority lane responsibilities during AQ-04–AQ-08; one mutation owner remains |
| Compound events become a hidden general transaction system | unbounded complexity and replay ambiguity | only admission and attempt disposition are compound in `AQ-CONT-1` |
| Canonical digest changes accidentally | idempotent retries conflict after upgrade | version canonical representation and algorithm; golden vectors in conformance package |
| Early signal matches stale history | wrong run resumes | correlation plus lower-bound cursor; strict defaults; stale-signal tests |
| Retention removes needed evidence | wait cannot reconcile or history becomes unexplained | pins, conservative horizon, retention conformance fixtures |
| Snapshot omits an index | restart behavior differs from live behavior | projection digest and WAL-only equivalence tests for every fixture |
| Feature combinations diverge | budget/actor/platform path violates core semantics | explicit feature matrix and shared authority code |
| Temporary legacy API survives release | two conceptual paths and downstream confusion | forbidden-symbol gate and AQ-14 release blocker |
| Remote actor can commit after lease loss | duplicate or contradictory subordinate state | lease fence validated before compound mutation |
| Resume context delivered twice | duplicate application continuation | atomic assignment to attempt plus lineage tests |
| Broad signals turn AQ into a message broker | unbounded routing/retention scope | narrow matching grammar; no arbitrary subscriptions or consumer acknowledgments |
| Domain concepts leak from downstream | AQ becomes coupled to Exoskeleton/WorldInterface | contract-boundary lints and opaque-reference tests |
| Performance degrades when continuations unused | general task engine pays specialist tax | common-path benchmarks and lazy/empty indexes |
| Clean break becomes schema indiscipline | future releases repeat incompatibility | target manifest, upgrade policy, backup/restore from first release |
| Campaign reference becomes scheduler backchannel | protected or self-improvement work receives hidden priority, budget, routing, or authority | opaque bounded refs only; ordinary explicit queue fields; `AQ-H19` equivalence tests |
| Queue completion becomes evaluation | execution state is reported as task correctness, arm victory, saturation, or candidate acceptance | `AQ-H20`; no semantic fields; higher-layer verification examples and schema checks |
| Physical retries are counted as independent samples | infrastructure instability biases higher-layer evaluation | preserve attempt/run lineage; explicitly document that sample semantics are application-owned |
| Developmental IDs leak through telemetry | sensitive workload or customer information appears in metrics | redacted traces/inspection; no high-cardinality developmental metric labels |

---

## 14. Definition of implementation complete

`AQ-CONT-1` is implementation-complete only when all of the following are true.

### Core and API

- [ ] `Awaiting` exists and has only the contract transitions.
- [ ] `Suspended` remains preemption and cannot carry a wait.
- [ ] `AttemptDisposition` is the only handler result type.
- [ ] `ensure_task` is durable, idempotent, and conflict-detecting.
- [ ] durable signals have stable identity, sequence, and replay.
- [ ] one active wait per run is durably indexed.
- [ ] signal-before-wait and wait-before-signal both wake once.
- [ ] checkpoint and resume context survive restart.
- [ ] child admissions can commit atomically with parent waiting.
- [ ] causal and control refs propagate without authorizing anything.
- [ ] executor traits have no authority semantics.
- [ ] campaign and intervention-arm attribution uses only existing bounded causal seams.
- [ ] no developmental domain type, metadata map, or campaign-specific scheduler branch exists.

### Persistence and recovery

- [ ] target stores carry a recognized manifest.
- [ ] pre-contract stores are rejected untouched.
- [ ] WAL-only and snapshot-plus-tail recovery produce the same projection digest.
- [ ] every compound crash point produces all-or-none projected facts.
- [ ] recovery reconciles waits/signals/deadlines before dispatch opens.
- [ ] backup and restore are tested.
- [ ] future format evolution policy and fixtures are established.

### Workflow and features

- [ ] fire-and-forget child submission is deleted.
- [ ] DAG and hierarchy semantics remain correct.
- [ ] budget suspension and continuation are distinct and integrated.
- [ ] custom external events are deleted.
- [ ] remote actor results are fenced and continuation-aware.
- [ ] platform controls are tenant-isolated and attributable.

### Operations

- [ ] embedded, daemon, and CLI surfaces expose target semantics.
- [ ] health/readiness reflect recovery reconciliation.
- [ ] inspection explains admissions, waits, signals, checkpoints, and resume lineage.
- [ ] metrics avoid high-cardinality or sensitive labels.
- [ ] authorized inspection can filter structural history by exact correlation or origin reference and displays explicit scheduling-field differences.
- [ ] inspection states that queue outcomes are execution facts and exposes no score, winner, binding-constraint, saturation, or activation judgment.

### Verification and release

- [ ] all target conformance fixtures pass.
- [ ] all eighteen developmental campaign acceptance cases pass with replay and crash variants.
- [ ] neutral-reference equivalence proves campaign attribution changes neither scheduling nor authority.
- [ ] queue terminal and timing facts remain distinct from higher-layer verification and acceptance.
- [ ] all crash matrices pass.
- [ ] all feature combinations pass.
- [ ] forbidden legacy symbols are absent.
- [ ] no old-format runtime reader or live compatibility path exists.
- [ ] release manifest pins contract, conformance, toolchain, and store versions.
- [ ] WorldInterface reference integration passes the black-box package.

---

## 15. Immediate issue set for AQ-01

The first development slice can be opened as the following issues before code changes:

1. **Freeze and tag baseline** — create `actionqueue/pre-aq-cont-1`, verify commit, archive metadata.
2. **Create integration branch and protection rules** — target all implementation PRs to `aq-cont-1`.
3. **Add contract manifest** — `conformance/aq-cont-1/manifest.yaml`, contract revision, fixture schema.
4. **Create ADR queue** — add the eighteen ADR files with recommended defaults and status.
5. **Classify existing acceptance tests** — retain/replace/reject table committed beside the test taxonomy.
6. **Capture baseline fixtures** — selected WAL/snapshot/crash examples and hashes.
7. **Capture reproducible baseline measurements** — no public performance promises; document environment.
8. **Add domain-leakage check** — prevent downstream ontology in ActionQueue public types.
9. **Add staged forbidden-symbol check** — begin with terminology targets; tighten as removal PRs land.
10. **Add clean-break store test scaffold** — fixture proving pre-contract data is not silently accepted once AQ-03 lands.
11. **Document downstream freeze** — WorldInterface and Exoskeleton remain pinned to old ActionQueue until target conformance is released; development adapters use exact Git revisions only.
12. **Open AQ-02 implementation issues** — IDs, state, transition table, causal refs, executor traits, bounded values.
13. **Commit developmental execution profile** — add the additive profile and eighteen-case acceptance matrix under the target conformance package.
14. **Add semantic-neutrality checks** — forbid queue-owned campaign/evaluation types, free-form metadata, campaign-specific scheduler branches, and semantic evaluation outputs.

AQ-01 is complete when the repository can destroy and replace internal implementations without losing the evidence needed to prove the target is better.

---

## 16. Downstream handoff package for WorldInterface

The ActionQueue release should include a small, deliberately domain-neutral example that demonstrates the exact pattern WorldInterface will use:

```text
1. An application stores its own authoritative intent and outbox.
2. It calls ensure_task with a stable AdmissionKey.
3. A lost response is recovered with AlreadyExists.
4. The handler returns Awaiting with checkpoint and exact correlated WaitSpec.
5. The application stores an authoritative external outcome.
6. Its signal outbox admits a durable SignalEnvelope.
7. ActionQueue resumes the task with ResumeContext.
```

The package must make these boundaries explicit:

- ActionQueue does not store the application's authoritative intent or receipt.
- The signal payload may reference that record but does not replace it.
- ActionQueue does not decide whether an external outcome is true or uncertain.
- ActionQueue does not validate an external authorization envelope.
- The application owns the outbox and downstream identity mapping.
- Developmental campaign and arm references, when present, use ordinary bounded causal fields and remain attribution only.
- ActionQueue completion, failure, retry, duration, and recovery facts are evidence inputs; they do not establish task quality, intervention gain, saturation, or candidate acceptance.

WorldInterface may begin its implementation against AQ-04 APIs for outbox/admission prototyping, but it must not claim `WI-FABRIC-2` until AQ-13 and AQ-14 are complete. Exoskeleton may rely on the developmental profile only after the `AQ-DD-*` matrix is published with the pinned conformance revision.

---

## Appendix A — PR dependency and gate matrix

| PR | Depends on | Primary contract unlocked | Required gate before merge |
|---|---|---|---|
| AQ-01 | — | Development contract | Baseline green; contract and archive checks |
| AQ-02 | AQ-01 | Pure target vocabulary | State/property/serde tests; terminology removal |
| AQ-03 | AQ-02 | Target durable lineage | Replay, reject-old, backup/restore, projection equality |
| AQ-04 | AQ-03 | Idempotent admission | conflict/race/crash suite |
| AQ-05 | AQ-03 | Durable signal facts | dedup/conflict/replay/retention suite |
| AQ-06 | AQ-05 | Durable continuation | no-lost-wakeup and timeout/cancel matrix |
| AQ-07 | AQ-06 | Checkpoint/resume | crash and exactly-once delivery suite |
| AQ-08 | AQ-04, AQ-06, AQ-07 | Complete handler result | lease-fence and compound disposition suite |
| AQ-09 | AQ-08 | Safe dynamic workflows | child atomicity/DAG/recovery suite |
| AQ-10 | AQ-06, AQ-08 | Budget/reactivity separation | feature and budget/resume suite |
| AQ-11 | AQ-08, AQ-10 | Remote/platform target contract | tenant/fence/control-attribution suite |
| AQ-12 | AQ-04–AQ-11 | Operational surface and neutral developmental inspection | API parity, auth, structural origin/correlation inspection, and no semantic outputs |
| AQ-13 | AQ-12 | Published conformance behavior | full model/chaos/feature/performance gates plus eighteen developmental-neutrality cases |
| AQ-14 | AQ-13 | Downstream stable release | release blockers, published developmental profile, and manifest verification |

## Appendix B — Suggested issue labels

```text
contract:AQ-CONT-1
area:core
area:storage
area:engine
area:executor
area:runtime
area:workflow
area:budget
area:actor
area:platform
area:daemon-cli
type:adr
type:breaking-api
type:persistence
type:conformance
type:crash-test
invariant:AQ-H1 ... invariant:AQ-H20
risk:high
blocks:WI-FABRIC-2
```

## Appendix C — Forbidden legacy inventory at release

The release gate should search source, examples, public docs, and generated API output for:

```text
HandlerOutput
TaskSubmissionPort
SubmissionChannel
ActorCapabilities
required_capabilities
with_capabilities
EventFilter::Custom
ActionQueueEvent::CustomEvent
WAL v5 reader
snapshot schema v8 reader
legacy handler adapter
compatibility admission path
```

Historical archive files may contain these names. Active target crates and runtime documentation may not.

## Appendix D — Planning assumptions to revisit only with evidence

- The existing eleven-crate DAG remains serviceable through `AQ-CONT-1`.
- One active wait per run is sufficient for first-release workloads.
- Exact correlation plus lower-bound cursor covers the required external continuation cases.
- Purpose-built compound records are sufficient; no general transaction language is needed.
- Release-while-awaiting is the correct concurrency default.
- Conservative signal retention is acceptable until measured workloads justify compaction complexity.
- The current Rust toolchain can implement the contract.
- Serialized mutation authority remains preferable to distributed mutation ownership.
- WorldInterface and Exoskeleton can carry all domain meaning through opaque refs and their own stores.

A failed assumption creates an ADR and contract review. It does not justify an undocumented local workaround.
