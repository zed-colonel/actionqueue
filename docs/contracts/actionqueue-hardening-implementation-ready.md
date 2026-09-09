# ActionQueue Hardening: Durable Continuations, Idempotent Admission, and Causal Execution

**Status:** Implementation-ready clean-break architecture baseline; neutral developmental-campaign execution clarification integrated  
**Target repository:** `zed-colonel/actionqueue`  
**Baseline reviewed:** public `main` at commit [`97c9dc26c19c697dbfb204ed503e82c5f053394f`](https://github.com/zed-colonel/actionqueue/commit/97c9dc26c19c697dbfb204ed503e82c5f053394f), June 9, 2026  
**Design-set contract:** `AQ-CONT-1`  
**Companion designs:** `WorldInterface v2: Accountable Boundary Fabric, Portable Agency, and Deterministic Flow` (`WI-FABRIC-2`); `Exoskeleton v3: Constitutional Vessel, Governed Initiative, Communication, Narrative Identity, and Delegated Agency` (`EXO-V3`)  
**Date:** August 10, 2026  
**Scope:** ActionQueue lifecycle, mutation authority, task admission, continuations, durable signals, checkpoint delivery, causal and identity-reference propagation, neutral developmental-campaign attribution, persistence versioning and recovery, and contracts with downstream systems  
**Working thesis:** *Opaque work remains opaque. ActionQueue durably establishes what may run, what it is waiting for, and what caused it—without learning what the work means. A developmental label remains attribution, never authority, scheduling policy, or performance judgment.*

**Review posture:** This proposal is grounded in a static review of the pinned public repository and the two companion redesigns. The current repository is a frozen experimental baseline, not a compatibility target. Proposed Rust APIs are illustrative until compiled against the codebase and proven by target-only conformance and crash suites. The OHIO-derived developmental profile is an additive diagnostic and conformance clarification; it does not change the `AQ-CONT-1` protocol, persistence model, scheduler, or authority boundary.

---

## 0. Coordinated design-set contract

**Canonical contract revision:** `STACK-2026-07-20-CLEAN-1`  
**Canonical machine-readable source:** `constitutional-stack-implementation-contracts.yaml`
**Developmental diagnostics profile:** `developmental-diagnostics-cross-stack-profile.yaml` (`STACK-DEVELOPMENTAL-DIAGNOSTICS-1`; no lower-layer protocol change)

The following matrix is synchronized semantically across the three planning packages. Repository-specific documents may append local security rows, but shared rule ownership and cross-layer proof obligations must remain consistent. The labels name architectural behavior contracts, not crate semantic versions.

| Contract | Repository | Normative responsibility | Depends on |
|---|---|---|---|
| `AQ-CONT-1` | ActionQueue | Durable opaque execution, compound idempotent admission, run/attempt lifecycle, `Awaiting`, durable signals, checkpoints, opaque causal-reference propagation, and recovery. | Nothing in the design set. |
| `WI-FABRIC-2` | WorldInterface | Verified operational agency, portable-identity bindings, canonical resource and audience resolution, authorized ingress bindings, final executable authorization, accountable observations and effects, proof bundles, delivery, reconciliation, receipts, and deterministic boundary workflows. | `AQ-CONT-1`. |
| `EXO-V3` | Exoskeleton | Vessel identity, mission, governed initiative, interaction, communication, cognitive faculties, constitutions, commitments, planning, narrative continuity, external personas, delegation, verification, and epistemic acceptance. | `WI-FABRIC-2` and `AQ-CONT-1`. |

```text
AQ-CONT-1 → WI-FABRIC-2 → EXO-V3
```

A portable identity foundation such as EntityID, KERI, a W3C controlled-identifier profile, certificates, or a future equivalent is an **optional adjacent foundation**. It integrates through WorldInterface identity and proof interfaces; it is not a mandatory fourth runtime layer and does not replace internal Vessel identity.

```text
Portable identity foundation
  control lineage · signatures · attestations · optional transparency
                         │
                         ▼
AQ-CONT-1 ─────────→ WI-FABRIC-2 ─────────→ EXO-V3
```

### 0.1 Clean-break implementation posture

The pinned repositories are pre-production experimental baselines. They are retained as source history, characterization fixtures, and evidence of failure modes—not as runtime compatibility obligations.

The target implementation follows six shared rules:

1. **Pre-contract implementations are evidence, not dependencies.**
2. **One authoritative path exists at every boundary.** Offline fixture comparison is allowed; live dual authority is not.
3. **Pre-contract state is never silently promoted into target commitments, identity, relationships, authorization, receipts, narrative, or queue history.**
4. **Target conformance outranks source compatibility.** Intentional guarantees are re-proved under the target contracts.
5. **Future stability begins with the target release.** Each target establishes explicit versioning, backup, restore, and supported migration policy for releases that follow it.
6. **Repository tags and archives are the legacy archive.** The active runtime does not carry obsolete readers, adapters, aliases, fallback paths, or downgrade support merely because the old code exists.

### 0.2 Shared handoff rule

Every cross-store or cross-process handoff follows one pattern:

```text
source authoritative domain record + source outbox
    → idempotent downstream admission
    → downstream authoritative record + downstream signal outbox
    → opaque ActionQueue wake-up or task reference
```

An ActionQueue signal wakes work. It is never the authoritative message, approval, worker result, identity event, receipt, effect outcome, or narrative fact.

### 0.3 Ownership and identity boundaries

| Identity or record | Owner | Meaning |
|---|---|---|
| `AdmissionKey`, `TaskId`, `RunId`, `AttemptId`, `WaitId`, `SignalId` | ActionQueue | Durable admission and opaque execution lifecycle. |
| `IngressBindingId`, `IngressObservationId`, `DeliveryId`, `EffectIntentId`, `AuthorizationId`, `ReceiptId` | WorldInterface | Accountable boundary admission, executable authority, effect/observation lifecycle, and evidence. |
| `VesselId`, `CommitmentId`, `DecisionId`, `WorkOrderId`, `CommunicationIntentId`, `CognitiveEpisodeId`, `NarrativeVersionId` | Exoskeleton | Enduring identity, purpose, local constitutional decisions, interaction, cognition, delegation, and interpretation. |
| provider idempotency key | WorldInterface connector protocol | External replay protection. It is never an `AttemptId`. |
| external identifier and key-state reference | External foundation / WorldInterface binding | Portable control lineage and provenance evidence; never unique personhood, semantic truth, or automatic authority. |

Illustrative opaque references use one non-normative syntax throughout the set:

```text
ref://<system>/<kind>/<id>
ref://exoskeleton/commitment/C
ref://worldinterface/effect-intent/E
ref://actionqueue/run/R
```

Consumers treat these as bounded opaque references unless their own domain explicitly owns the referenced namespace.

### 0.4 Cross-stack invariant matrix

| Shared rule | ActionQueue | WorldInterface | Exoskeleton | Required cross-layer proof |
|---|---|---|---|---|
| Store authoritative state before wake | `AQ-H3`, `AQ-H4`, `AQ-H5` | `W26` | `C36` | Crash between domain commit and signal delivery loses neither fact nor wake-up. |
| Attribution is not authorization | `AQ-H10`, `AQ-H11` | `W4`, `W5` | `C13`, `C14` | Forged caller metadata cannot create executable authority. |
| Routing or execution ability is not resource authority | `AQ-H12` | `W15` | `C4`, `C5` | Executor or connector eligibility cannot widen a grant. |
| Unknown consequential effects fail closed | — | `W12` | `C6` | Unclassified consequential work cannot dispatch. |
| Intent is durable before consequential dispatch | — | `W3` | `C16` | A crash before dispatch leaves an inspectable intent, not an unaccounted effect. |
| Ambiguity is first-class and never blind retry | `AQ-H13`, `AQ-H14` | `W13` | `C17` | Queue waiting, effect uncertainty, and Vessel acceptance remain separate states. |
| Generated content is not delivery | — | `W29` | `C31` | Model output cannot create provider acceptance or delivery evidence. |
| Endogenous or self-improvement origin grants no authority | `AQ-H11` | `W41`, `W54` | `C43`, `C73` | A self-generated goal or candidate uses the ordinary authorization path. |
| Genesis precedes external commissioning | — | `W44` | `C55`, `C58` | External setup cannot create or rewrite internal Vessel origin. |
| Acting capacity is explicit | — | `W45` | `C60`, `C67` | Multiple roles cannot ambiently union authority. |
| Access permission is not information-use permission | — | `W46` | `C68` | Read access cannot silently imply retention, derivation, disclosure, or reuse. |
| Candidate is not active runtime | — | `W48`, `W49` | `C71`, `C78` | Candidate production cannot activate or overwrite the running Vessel. |
| Evidence and evaluation are not acceptance | — | `W50` | `C72`, `C74` | A candidate or provider cannot certify its own acceptance. |
| Schema meaning is versioned | — | `W51` | `C69` | Policy replay pins exact semantic schema versions. |
| Restore is not fork | — | `W52` | `C63` | Migration, restore, succession, and branch creation remain distinguishable. |
| Temporal provenance is not semantic truth | — | `W55` | `C65` | Occurrence, observation, receipt, admission, and validity times remain distinct. |
| Developmental correlation is attribution, not authority or priority | `AQ-H19` | `W56` | `C96`, `C104` | Campaign and intervention-arm references cannot widen authority, route work, set priority, or create acceptance. |
| Boundary and execution evidence are not performance judgment | `AQ-H20` | `W57` | `C100`, `C101` | Queue completion and boundary receipts remain evidence inputs to Exoskeleton evaluation. |
| Experimental work follows ordinary policy | — | `W58` | `C100`, `C101` | A research, trial, benchmark, or self-improvement label cannot bypass resolution, information-use, approval, authorization, or reconciliation. |
| Comparative evidence pins its execution envelope | — | `W59` | `C96`, `C100` | Material resource, policy, connector, environment, artifact, timing, and uncertainty differences remain visible as intended changes or confounds. |
| Clean replacement has one authoritative path | `CLEAN-2` | `CLEAN-2` | `CLEAN-2` | No runtime old/new dual writes, dual reducers, or fallback authority. |

### 0.5 Conformance packages and release order

Each repository owns a target-only conformance package under its own source tree:

```text
actionqueue/conformance/aq-cont-1
worldinterface/conformance/wi-fabric-2
exoskeleton/conformance/exo-v3
```

Each package carries:

- contract label and integer contract revision;
- fixture schema version;
- executable black-box tests and crash scenarios;
- a manifest of required upstream conformance revisions;
- immutable fixture hashes.

A fixture change that only adds coverage increments the package revision. A change that alters a normative behavior contract requires an explicit contract amendment. Downstream repositories pin exact upstream conformance revisions in development and release manifests.

Development may use path or pinned Git dependencies. Published breaking releases occur only in this order:

```text
1. AQ-CONT-1 ActionQueue release
2. WI-FABRIC-2 WorldInterface release built against that ActionQueue contract
3. EXO-V3 Exoskeleton release built against both published upstream contracts
```

No compatibility release is published merely to keep the experimental stack compiling.


## Document map

0. [Coordinated design-set contract](#0-coordinated-design-set-contract)
1. [Executive decision](#1-executive-decision)
2. [Baseline and diagnosis](#2-baseline-and-diagnosis)
3. [Why the dependent systems create new pressure](#3-why-the-dependent-systems-create-new-pressure)
4. [Goals and non-goals](#4-goals-and-non-goals)
5. [Hardening invariants](#5-hardening-invariants)
6. [Target architecture](#6-target-architecture)
7. [Domain vocabulary](#7-domain-vocabulary)
8. [Run lifecycle and the `Awaiting` state](#8-run-lifecycle-and-the-awaiting-state)
9. [Durable signals](#9-durable-signals)
10. [Race-free wait registration and wake-up](#10-race-free-wait-registration-and-wake-up)
11. [Checkpoints and resume context](#11-checkpoints-and-resume-context)
12. [Attempt dispositions and compound durability](#12-attempt-dispositions-and-compound-durability)
13. [Task admission and dynamic submission](#13-task-admission-and-dynamic-submission)
14. [Cross-engine handoff and the outbox contract](#14-cross-engine-handoff-and-the-outbox-contract)
15. [Causal context and control attribution](#15-causal-context-and-control-attribution)
16. [Routing traits are not authority](#16-routing-traits-are-not-authority)
17. [Subscriptions and durable signals](#17-subscriptions-and-durable-signals)
18. [Budgets, cancellation, leases, and concurrency](#18-budgets-cancellation-leases-and-concurrency)
19. [Workflow, DAG, and hierarchy integration](#19-workflow-dag-and-hierarchy-integration)
20. [Remote actors and platform integration](#20-remote-actors-and-platform-integration)
21. [Persistence, WAL, snapshots, and recovery](#21-persistence-wal-snapshots-and-recovery)
22. [Crate and module changes](#22-crate-and-module-changes)
23. [Embedded, daemon, CLI, and API surfaces](#23-embedded-daemon-cli-and-api-surfaces)
24. [Observability and inspection](#24-observability-and-inspection)
25. [Security model](#25-security-model)
26. [Performance and capacity](#26-performance-and-capacity)
27. [Clean-break replacement and release strategy](#27-clean-break-replacement-and-release-strategy)
28. [Verification plan](#28-verification-plan)
29. [Risks and architectural decisions](#29-risks-and-architectural-decisions)
30. [Definition of done](#30-definition-of-done)
31. [Implementation sequence](#31-implementation-sequence)
32. [Worked examples](#32-worked-examples)

---

## 1. Executive decision

ActionQueue should be **hardened, not reconceived**.

The existing project has the correct foundational abstraction: it turns opaque durable intent into scheduled, leased, retried, recoverable execution while retaining exclusive mutation authority over its WAL-backed state. The Exoskeleton and WorldInterface redesigns do not require ActionQueue to understand cognition, agency, external effects, or organizational meaning. They require ActionQueue to become more rigorous at a smaller set of generic coordination problems that already belong inside a durable task engine.

The hardening should center on three seams.

### 1.1 Durable continuations

A run must be able to deliberately finish its current attempt, release its lease, persist a checkpoint, establish a durable wait condition, and later resume when a matching signal arrives.

The current `Suspended` state is useful for preemption and budget-driven pause. It is not a precise model for:

- waiting for a delegated worker;
- waiting for an operator response;
- waiting for a remote callback;
- waiting for effect reconciliation;
- waiting for another Vessel, service, or workflow;
- waiting for any externally originated condition whose occurrence must survive restart.

The proposal adds a distinct `Awaiting` lifecycle state, durable signals, durable wait registrations, and a resume context delivered to the next attempt.

### 1.2 Durable and idempotent admission

A caller must be able to establish that a task exists durably—or learn that the same admission already succeeded—without depending on an in-memory fire-and-forget channel.

The current dynamic-submission path explicitly permits silent loss if its channel closes, gives the handler no rejection path, and commits task creation, run derivation, and dependencies as separate mutations. That is acceptable as an early coordinator convenience, but not as the basis for long-horizon delegation or cross-engine handoff.

The proposal adds:

- an idempotent `ensure_task` admission contract;
- a validated `AdmissionPlan` that includes task, derived runs, parentage, dependencies, and causal context;
- compound durability for critical admission and attempt-yield transitions;
- a deliberately deferred, separately reviewed mid-attempt admission option if attempt-end dispositions later prove insufficient;
- a transactional end-of-attempt submission path.

### 1.3 Generic causal propagation

Tasks and control-plane mutations must preserve enough structural attribution to reconstruct causal lineage across parent tasks, workflows, remote actors, delegated workers, and separate ActionQueue instances.

ActionQueue should preserve opaque references such as:

- trace and correlation identifiers;
- causation links;
- submitting principal reference;
- requesting actor reference;
- purpose reference;
- authorization reference;
- external origin reference.

It must not interpret those references or treat them as authority. WorldInterface, Exoskeleton, or another application owns their meaning.

### 1.4 What does not change

This proposal preserves the project's central contract:

- task payloads remain opaque;
- the WAL remains authoritative;
- snapshots remain derived acceleration artifacts;
- the dispatch loop or storage mutation authority remains the only persistence mutation lane;
- state transitions remain typed, monotonic, and validated;
- leases and concurrency gates remain core-enforced;
- retries remain attempts of the same run;
- ActionQueue does not promise magical exactly-once external side effects;
- embedded and daemon modes retain the same execution semantics;
- ActionQueue remains useful without Exoskeleton or WorldInterface.

### 1.5 The review sentence

Every change proposed here should pass this test:

> **Does this make opaque asynchronous work more durable, race-free, attributable, and inspectable without teaching ActionQueue what the work means?**

If the answer is no, the change belongs in a downstream system.

---

## 2. Baseline and diagnosis

### 2.1 Existing strengths

The pinned baseline is already a substantial execution substrate. It contains:

- a pure core domain model;
- validated `TaskSpec`, `RunInstance`, constraints, and run policies;
- canonical states `Scheduled`, `Ready`, `Leased`, `Running`, `RetryWait`, `Suspended`, `Completed`, `Failed`, and `Canceled`;
- a single mutation-authority contract between engine intent and storage durability;
- WAL v5 using postcard serialization and CRC-32 framing;
- snapshot schema v8;
- deterministic recovery from WAL or snapshot plus WAL tail;
- lease expiry and uncertainty-clause re-eligibility;
- exact run accounting and bounded attempt retry;
- concurrency keys;
- workflow DAGs, parent-child hierarchy, cron, and dynamic task submission;
- per-task budgets and handler cancellation;
- durable subscription records for reactive promotion;
- remote actor registration, heartbeat, and routing;
- tenant, RBAC, ledger, and approval primitives;
- embedded runtime, daemon, CLI, metrics, acceptance tests, and chaos recovery tests.

The proposal depends on those strengths. It is not an attempt to replace them with a generalized distributed workflow framework.

### 2.2 The current lifecycle is execution-centric

The canonical transition table represents execution and retry well:

```text
Scheduled → Ready → Leased → Running → Completed
                                  ├──→ RetryWait → Ready
                                  ├──→ Suspended → Ready
                                  ├──→ Failed
                                  └──→ Canceled
```

`Suspended` is explicitly defined as preemption, including budget exhaustion. A suspended attempt does not count against `max_attempts`, and a later resume returns the run to `Ready`.

What the lifecycle lacks is a durable expression of this statement:

> The attempt completed normally, but the run cannot make progress until a named future fact exists.

Using `Suspended` for that case obscures why the run stopped, how it should wake, and what input must be delivered when it resumes.

### 2.3 Current subscriptions are durable triggers over ephemeral tick events

The baseline has a valuable beginning:

- subscription creation, trigger, and cancellation are WAL-backed;
- filters can observe task completion, run-state changes, budget thresholds, and custom keys;
- active subscriptions are reconstructed after recovery;
- matching can promote a scheduled run.

However, the event being matched is an in-process `ActionQueueEvent` evaluated during a dispatch tick. The custom event form is a single string key. There is no durable, deduplicated signal inbox carrying correlation, source, payload reference, tenant, or external identity. There is also no binding between a subscription and a suspended attempt checkpoint.

The result is a reactive scheduling primitive, not yet a general continuation protocol.

### 2.4 Current dynamic submission is intentionally best-effort

The workflow submission channel is documented as fire-and-forget:

- a closed channel silently drops the submission from the handler's perspective;
- processing occurs on a later tick;
- the handler cannot assume visibility before returning;
- invalid parentage or dependency cycles are logged and dropped;
- the handler receives no durable acknowledgment.

The dispatch loop then commits a dynamically submitted task through separate steps:

```text
TaskCreate
→ one or more RunCreate mutations
→ DependencyDeclare
→ in-memory hierarchy registration
```

The single-writer mutation boundary remains intact, which is good. The weakness is that the entire admission is not represented as one durable proposition. A handler can believe it delegated work when no durable child exists, and a crash can occur between admission steps.

### 2.5 Handler output does not yet encode continuation semantics

The baseline handler returns success, retryable failure, terminal failure, timeout, or suspension with optional opaque output bytes. `HandlerInput` contains the original payload, attempt metadata, and cancellation context. It does not contain:

- a prior checkpoint as a distinct concept;
- the signal that caused resumption;
- a wait identifier;
- immutable causal context;
- an acknowledged record of child admission.

Opaque output bytes are useful, but their role is overloaded: final result, partial state, and application reference all share the same storage shape.

### 2.6 The external-effect reality clause is correct

The existing idempotency guidance is sound: `RunId` is the stable key for external idempotency; `AttemptId` is lineage. A crash after a remote side effect but before durable completion may cause redispatch.

The redesigns do not require ActionQueue to solve that external uncertainty. WorldInterface owns effect intent, receipt, idempotency class, and reconciliation. ActionQueue only needs a durable way to wait for reconciliation without treating uncertain confirmation as an ordinary retryable failure.

### 2.7 The term “capability” is overloaded

Remote actors declare free-form strings such as `compute`, `review`, or `approve`, and task constraints require matching strings. These values are routing qualifications: they describe what an executor claims it can handle.

In the companion architecture, “capability” has a stricter meaning: bounded authority to act against a resource. Keeping the same term for both creates a dangerous ambiguity. An executor routing label must never be mistaken for authorization.

### 2.8 The storage model is ready for extension

The WAL already records typed semantic events, validates monotonic sequence admission, reconstructs in-memory projections, and supports schema-versioned snapshots. That is exactly the right foundation for durable waits and signals.

The main storage challenge is not inventing a new database. It is defining compound events and replay rules that preserve the existing single-authority model while closing lost-wakeup and partial-admission windows.

---

## 3. Why the dependent systems create new pressure

### 3.1 Exoskeleton needs asynchronous continuity without held cognitive leases

The Exoskeleton redesign separates durable commitments from transient cognition. A cognitive attempt may decide to delegate work or request an external effect, then stop. The result may arrive minutes, hours, or days later.

Keeping the original handler running would:

- consume a worker slot;
- hold or repeatedly renew a lease;
- couple cognitive timeout policy to external latency;
- increase duplicate-execution risk around lease expiry;
- make process restart recovery unnecessarily complicated.

The correct pattern is:

```text
attempt establishes durable subordinate work
→ attempt yields with checkpoint and wait predicate
→ run becomes Awaiting
→ external completion becomes a durable signal
→ run returns to Ready
→ next attempt receives checkpoint and wake signal
```

That pattern is not specific to AI. It is a general durable continuation.

Communication is a flagship acceptance case rather than a new queue concept:

```text
WorldInterface stores BoundaryObservation and subscriber delivery
    → Exoskeleton stores CommunicationEvent and local wake outbox
    → ActionQueue admits a durable signal or episode task
    → a busy or restarted Vessel processes it without lost wakeup
```

ActionQueue carries only structural signal and causal references. It does not store or interpret the message, conversation, attention policy, or response obligation.

### 3.2 WorldInterface needs callback and reconciliation waits

WorldInterface may submit an effect, receive a remote asynchronous job identifier, and wait for a webhook. It may also record an effect as uncertain and schedule reconciliation.

Neither case should be represented as a handler sleeping or as blind retry. ActionQueue needs to durably represent the wait while WorldInterface retains the semantic state.

### 3.3 Separate ActionQueue engines require idempotent handoff

Exoskeleton currently uses separate cognitive and tool ActionQueue instances. A centralized WorldInterface platform may use another instance. No local WAL can atomically commit across those boundaries.

The dependent system must therefore use an outbox:

```text
local domain transaction
    creates intent + outbox record

outbox dispatcher
    repeatedly ensures remote task exists

remote ActionQueue
    returns Created or AlreadyExists
```

ActionQueue does not own the outbox, but it should provide a durable idempotent admission endpoint that makes the pattern reliable.

### 3.4 NetCorp raises attribution and tenant-boundary pressure

A centralized organizational deployment may involve:

- many authenticated principals;
- multiple vessels;
- human operators;
- workflows acting on behalf of organizations;
- delegated workers;
- remote actors;
- separate tenants and resource owners.

ActionQueue should not own organizational semantics, but it must not erase the causal references supplied by those systems. It also must not let a signal or admission in one tenant satisfy a wait in another.

### 3.5 The same primitives benefit ordinary applications

The required changes are not special accommodations for Exoskeleton:

- a payment workflow can await a settlement callback;
- a CI coordinator can await a remote test runner;
- a document pipeline can await human review;
- a batch system can await an upstream data arrival signal;
- a SaaS workflow can idempotently ensure jobs across services;
- an embedded application can resume from a content-addressed checkpoint after restart.

That generality is the reason these changes belong in ActionQueue.

---


### 3.6 Governed initiative, narrative continuity, and portable identity are ordinary workloads

The final Exoskeleton design adds scheduled agenda review, durable quiescence, narrative-synthesis episodes, and external-persona/key-lifecycle operations. None requires ActionQueue to understand mission, goals, autobiography, identifiers, signatures, or trust.

ActionQueue's responsibilities are structural:

- schedule an agenda review at a durable time;
- admit exactly one cognitive episode for a stable review identity;
- preserve a checkpoint while narrative synthesis awaits evidence;
- carry an opaque identity-state or proof reference through a task lineage;
- wake a coordinator after WorldInterface stores a verified identity, signature, attestation, or publication result;
- retain causal attribution across restart.

`Quiescent`, `Mission`, `NarrativeVersion`, `ExternalPersona`, and `EntityID` are not ActionQueue run states or task kinds. They remain downstream domain concepts.

## 4. Goals and non-goals

### 4.1 Goals

The hardening must:

1. Add a first-class durable continuation state distinct from preemption.
2. Make signal ingestion durable, replayable, idempotent, and tenant-scoped.
3. Prevent lost wakeups whether a signal arrives before, during, or after wait registration.
4. Deliver a checkpoint and wake reason to the resumed handler.
5. Make critical attempt completion, wait registration, and run transition one semantic durability boundary.
6. Make task admission acknowledged and idempotent.
7. Make parent-child submission durably knowable to the parent.
8. Preserve opaque causal references across tasks, runs, attempts, children, signals, and control mutations.
9. Clarify that executor routing traits are not authority.
10. Preserve deterministic replay and existing run-accounting guarantees.
11. Preserve embedded and daemon semantic parity.
12. Remain useful as an independent task engine.
13. Establish a fresh, explicitly versioned target persistence contract and precise rejection of pre-`AQ-CONT-1` formats.
14. Extend acceptance and chaos testing around every new crash boundary.
15. Support timer-driven agenda review, narrative consolidation, and identity/proof callbacks as ordinary opaque workloads.
16. Preserve bounded external identity and proof references without interpreting or validating them.
17. Carry developmental campaign and intervention-arm attribution through existing bounded causal seams without creating new queue-domain primitives.
18. Make queue execution evidence usable by higher-layer evaluation while never treating queue completion, failure, timing, or retry counts as performance judgment.

### 4.2 Non-goals

This proposal does not add:

- Exoskeleton faculties, commitments, constitutions, or Vessels;
- WorldInterface effect intents, receipts, resource capabilities, or uncertainty semantics;
- a generalized human-approval ontology;
- distributed consensus or multi-writer WAL;
- cross-instance transactions;
- exactly-once external effects;
- arbitrary payload predicate evaluation inside signal matching;
- a message broker replacement;
- an event-sourcing framework for arbitrary application state;
- a universal artifact store;
- semantic interpretation of causal references;
- workflow business logic inside the ActionQueue core;
- authority derived from actor routing declarations;
- mandatory hierarchical budgets or global multi-tenant fairness in the first tranche;
- resolve external identifiers, validate key lineage, or verify cryptographic statements;
- decide whether a signed assertion is true;
- interpret mission, initiative, quiescence, self-narrative, reputation, or bonded commitments;
- define campaigns, intervention arms, task distributions, capability profiles, representation profiles, binding constraints, benchmark scores, saturation, or candidate acceptance;
- add a free-form experiment metadata channel or infer scheduler policy from opaque campaign references.

### 4.3 Explicit anti-goal: domain leakage

The following names should not appear in ActionQueue core APIs merely to satisfy the companion systems:

```text
Vessel
Faculty
Commitment
DelegatedAgent
WorkOrder
EffectIntent
Receipt
Constitution
ApprovalMeaning
Relationship
Conversation
AttentionDemand
ResponseObligation
Belief
```

Downstream payloads and opaque references may contain those concepts. ActionQueue itself should not.

---

## 5. Hardening invariants

These invariants extend rather than replace the existing charter and invariant-boundaries policy.

### AQ-H1 — Opaque meaning

ActionQueue may inspect structural scheduling fields, identifiers, equality-match keys, resource accounting, and lifecycle state. It must not interpret application payload meaning.

### AQ-H2 — WAL authority

Any fact required after restart must be represented in the WAL or in an immutable external artifact referenced by a WAL fact. In-memory registries remain derived projections.

### AQ-H3 — No lost wakeups

Once a signal has been durably admitted, a compatible wait registered later must be able to match it according to the wait's declared eligibility window. Once a wait has been durably registered, a compatible later signal must be able to satisfy it. Crash timing must not change that truth.

### AQ-H4 — Wait establishment is atomic

An attempt cannot durably finish in a waiting disposition unless its checkpoint, wait specification, and `Running → Awaiting` transition are committed as one semantic mutation.

### AQ-H5 — Wake-up is single and replay-stable

A wait may be satisfied at most once. Duplicate signals or replay may not create multiple resumptions. When several signals could match, deterministic WAL ordering selects the winner unless the wait explicitly requires a set.

### AQ-H6 — Admission is knowable

A caller using acknowledged admission must receive one of:

- durable creation;
- durable prior existence with matching content;
- explicit conflict;
- explicit rejection;
- transport uncertainty that can be safely retried with the same admission key.

Silent loss is not an allowed outcome for the acknowledged API.

### AQ-H7 — Idempotent admission detects semantic conflict

The same admission key and same canonical admission digest are idempotent. The same key with different content is a conflict, never an overwrite.

### AQ-H8 — Parent wait implies durable child

A parent attempt may not enter `Awaiting` for a child task created in the same disposition unless that child's admission is part of the same durable semantic commit.

### AQ-H9 — Attempt disposition is complete

The dispatch loop must validate the entire disposition before applying any mandatory component. A malformed child, invalid wait, illegal state transition, or tenant violation rejects the disposition as a unit.

### AQ-H10 — Causal context is immutable attribution

Causal context attached at admission is preserved through runs and attempts. Child derivation may extend causation but may not silently replace the accountable ancestry supplied by the parent.

### AQ-H11 — Attribution is not authorization

An `authorization_ref`, `principal_ref`, or actor identity carried by ActionQueue is an opaque reference. It grants no permission inside ActionQueue or downstream systems.

### AQ-H12 — Routing traits are not capabilities

Executor declarations participate only in dispatch eligibility. They cannot authorize effects, access resources, mutate policy, or override tenant boundaries.

### AQ-H13 — Awaiting is not failure

An attempt that deliberately yields to `Awaiting` does not consume the retry cap. A timeout may later resume, fail, or cancel according to explicit generic policy, but waiting itself is not an error.

### AQ-H14 — External uncertainty remains external

ActionQueue represents that a run is waiting. It does not add an `UncertainEffect` state or infer whether an external effect committed. The application determines the semantic signal that resumes the run.

### AQ-H15 — Control operations are attributable

Submit, ensure, cancel, suspend, resume, reprioritize, signal-ingest, and administrative mutations must be capable of carrying a host-attested caller reference in daemon or platform deployments.

### AQ-H16 — Version evolution is explicit

The target release starts a new supported persistence lineage. Pre-`AQ-CONT-1` stores fail with a precise unsupported-format error. From the first stable target release forward, supported upgrades must migrate deterministically or fail without modifying the source. Historical state is never silently reinterpreted.

---


### AQ-H17 — External identity and proof references remain opaque

An identity-binding reference, key-state reference, authentication-context reference, signed-statement reference, or proof-bundle reference is preserved exactly as bounded metadata. Its presence establishes no identity, truth, trust, or authority inside ActionQueue.

### AQ-H18 — Agenda review is scheduled work; quiescence is not a queue lifecycle state

ActionQueue may schedule or wake an agenda-review task. Whether the owning Vessel is quiescent, whether a mission justifies new work, and whether a goal may be adopted remain application-domain decisions.

### AQ-H19 — Developmental correlation is neutral attribution

Campaign, intervention-arm, evaluation-plan, task-distribution, and component-fingerprint references may be carried through existing bounded causal fields. Their presence or values do not alter authorization, admission, executor eligibility, priority, budget, deadlines, retry behavior, retention, or acceptance.

### AQ-H20 — Execution outcome is not performance judgment

Queue lifecycle and timing facts describe execution only. ActionQueue never declares task correctness, arm preference, performance gain, a binding constraint, saturation, candidate acceptance, or activation eligibility.

## 6. Target architecture

The target remains a single durable execution kernel with optional feature layers:

```text
Application / Downstream Domain
    │
    │ TaskSpec + AdmissionKey + CausalContext
    │ SignalEnvelope
    │ ControlMutationContext
    ▼
┌────────────────────────────────────────────────────────────┐
│ ActionQueue Runtime                                        │
│                                                            │
│ admission planner · scheduler · dispatch · wait matching   │
│ retry · leases · concurrency · budgets · hierarchy         │
│                                                            │
│                 single mutation authority                  │
└──────────────────────────────┬─────────────────────────────┘
                               │ typed semantic commands
                               ▼
┌────────────────────────────────────────────────────────────┐
│ ActionQueue Storage                                        │
│                                                            │
│ WAL · compound event validation · replay projection        │
│ snapshots · idempotency index · signal/wait index          │
└──────────────────────────────┬─────────────────────────────┘
                               │ handler execution
                               ▼
┌────────────────────────────────────────────────────────────┐
│ Executor                                                    │
│                                                            │
│ HandlerInput                                                │
│   original payload                                          │
│   attempt metadata                                          │
│   causal context                                            │
│   optional ResumeContext                                    │
│                                                            │
│ AttemptDisposition                                          │
│   outcome / checkpoint / wait / children / signals          │
└────────────────────────────────────────────────────────────┘
```

The core dependency statement is:

```text
ActionQueue core/storage/runtime
    know only structural execution concepts.

Exoskeleton
    maps commitments and delegations into tasks, refs, and signals.

WorldInterface
    maps effects, receipts, and reconciliation into tasks, refs, and signals.
```

### 6.1 Primary control paths

#### Ordinary synchronous completion

```text
Task admitted
→ Run ready
→ Lease acquired
→ Attempt started
→ Handler succeeds
→ Attempt disposition committed
→ Run completed
```

#### Durable continuation

```text
Task admitted
→ Run ready
→ Lease acquired
→ Attempt started
→ Handler yields checkpoint + wait
→ compound disposition committed
→ Run awaiting
→ signal durably admitted
→ wait satisfied
→ Run ready
→ next attempt receives resume context
```

#### Idempotent cross-instance admission

```text
Outbox retries ensure_task(key, plan)
→ Created(plan digest)
   or AlreadyExists(same digest)
   or Conflict(different digest)
```

#### Transactional parent-child handoff

```text
Parent attempt returns:
    child AdmissionPlan
    parent checkpoint
    parent WaitSpec correlated to child

ActionQueue commits:
    parent attempt yielded
    child task and runs created
    parent-child relation created
    dependencies declared
    wait registered
    parent state Awaiting
```

---

## 7. Domain vocabulary

The hardening should introduce a small generic vocabulary.

### 7.1 `AdmissionKey`

A caller-provided, tenant-scoped idempotency identity for task admission. It is distinct from `TaskId` so callers can retry admission without guessing whether a generated task identifier was accepted.

### 7.2 `AdmissionDigest`

A canonical content hash over the structurally relevant admission plan. It detects reuse of an admission key with different content.

### 7.3 `AdmissionPlan`

A fully validated proposition containing:

- `TaskSpec`;
- derived or explicitly supplied runs;
- dependencies;
- parent task reference;
- tenant;
- causal context;
- optional initial subscriptions or waits where supported.

### 7.4 `WaitId`

A stable identifier for one durable continuation wait.

### 7.5 `WaitSpec`

A structural predicate, deadline, and timeout policy describing what may resume an awaiting run.

### 7.6 `SignalId`

A producer-assigned idempotency identity for a durable signal.

### 7.7 `SignalEnvelope`

A durable, tenant-scoped, correlation-bearing notification carrying only bounded structural fields and an optional opaque payload reference.

### 7.8 `SignalCursor`

A monotonic position in the admitted signal stream used to define a wait's eligible observation window where exact correlation is insufficient.

### 7.9 `CheckpointRef`

An immutable reference to partial handler state. Small checkpoints may be inline; larger checkpoints use an external content-addressed reference.

### 7.10 `ResumeContext`

The checkpoint, wait identity, and wake reason delivered to the next attempt.

### 7.11 `AttemptDisposition`

The complete proposition returned by a handler at the end of an attempt: terminal outcome, retry outcome, suspension, or awaiting continuation, plus optional outputs, budget consumption, child admissions, and emitted signals.

### 7.12 `CausalContext`

Immutable structural attribution attached to a task and propagated to runs and attempts.

### 7.13 `ControlMutationContext`

Attribution for the caller requesting a queue-control mutation. It is separate from the task's causal context because an operator canceling a task may not be the same actor that originally submitted it.

### 7.14 `ExecutorTraits`

The renamed free-form routing qualifications currently called actor capabilities.

---

## 8. Run lifecycle and the `Awaiting` state

### 8.1 Why `Suspended` must remain distinct

`Suspended` means that execution was preempted or administratively paused. Examples include:

- budget exhaustion;
- operator pause;
- temporary executor unavailability;
- cooperative preemption.

The condition for resumption is generally a control-plane change: replenish budget, explicitly resume, or restore an execution condition.

`Awaiting` means that the attempt intentionally yielded after naming a future signal. Examples include:

- a callback correlated to a request;
- completion of another task or remote job;
- availability of an input artifact;
- an approval response represented by the application;
- a reconciliation result.

The distinction improves inspection:

```text
Suspended:
    “Execution has been paused.”

Awaiting:
    “Execution is inactive because wait W has not yet been satisfied.”
```

### 8.2 Proposed state machine

```text
Scheduled ───────────────→ Ready ───────────────→ Leased
   │                        │                        │
   └────────→ Canceled      └────────→ Canceled     ├──→ Ready      (lease expiry)
                                                    ├──→ Canceled   (control)
                                                    └──→ Running

Running ─────────→ Completed
   │  ├──────────→ Failed
   │  ├──────────→ RetryWait ─────────→ Ready
   │  ├──────────→ Suspended ─────────→ Ready
   │  ├──────────→ Awaiting ──────────→ Ready
   │  │                  ├────────────→ Failed
   │  │                  └────────────→ Canceled
   │  └──────────→ Canceled
   │
RetryWait ───────→ Failed / Canceled
Suspended ───────→ Canceled
```

### 8.3 Valid transitions added

The minimal additions are:

```text
Running  → Awaiting
Awaiting → Ready
Awaiting → Failed
Awaiting → Canceled
```

`Awaiting → Ready` occurs only through a durably recorded wait satisfaction or timeout policy that resumes the handler.

`Awaiting → Failed` occurs only through an explicit timeout policy or validated administrative action. It is not a retry decision.

### 8.4 Attempt accounting

A yielded attempt is a real attempt in lineage but not a failed attempt for retry-cap purposes.

The attempt result taxonomy should distinguish:

```rust
pub enum AttemptResultKind {
    Success,
    Failure,
    Timeout,
    Suspended,
    Awaiting,
}
```

Both `Suspended` and `Awaiting` are excluded from the effective failure-attempt count. They remain visible in attempt history.

This preserves two truths:

- the handler executed and produced a durable disposition;
- the wait is not consuming failure retries.

### 8.5 Lease behavior

An awaiting run owns no execution lease.

The transition from `Running` to `Awaiting` must release the current lease as part of the same semantic disposition. No heartbeat continues while waiting. On wake-up, the run returns to `Ready` and competes normally for a new lease.

### 8.6 Concurrency-key behavior

A task constraint should declare whether an awaiting run retains its concurrency key:

```rust
pub enum ConcurrencyKeyWaitPolicy {
    ReleaseWhileAwaiting,
    HoldWhileAwaiting,
}
```

Recommended default: `ReleaseWhileAwaiting`.

Holding a key for hours or days can block unrelated useful work. It is appropriate only when the wait represents an externally active critical section whose collision domain must remain fenced.

This policy is separate from the existing retry-wait hold policy.

### 8.7 Deadlines

A run-level execution deadline and a wait deadline are different:

- execution timeout limits one attempt's active runtime;
- wait deadline limits how long a continuation may remain awaiting.

A wait deadline must therefore live in `WaitSpec`, not overload `timeout_secs`.

### 8.8 Cancellation

Cancellation of an awaiting run must durably:

1. transition the run to `Canceled`;
2. mark its active wait canceled;
3. prevent later matching signals from waking it;
4. retain the wait and signal history for inspection;
5. optionally expose the cancellation through an application callback or emitted internal signal without interpreting its meaning.

### 8.9 Recovery

On restart, recovery reconstructs:

- every awaiting run;
- its active wait;
- its checkpoint reference;
- all retained signals;
- any wait already satisfied but not yet promoted;
- deadlines due before the next dispatch tick.

The post-recovery matcher then deterministically completes any pending `WaitSatisfied → Ready` transition.


---

## 9. Durable signals

### 9.1 Signal purpose

A signal is a durably admitted structural fact that may satisfy one or more waits.

It is not:

- an application domain event store;
- a command to execute arbitrary code;
- a substitute for a task;
- proof that an external claim is true;
- authority to perform an action;
- an ephemeral tracing event.

A producer is responsible for the meaning and authenticity of the signal. ActionQueue is responsible for admission identity, tenant isolation, durable retention, deterministic matching, and replay.

### 9.2 Proposed envelope

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalEnvelope {
    pub signal_id: SignalId,
    pub tenant_id: Option<TenantId>,

    pub namespace: SignalNamespace,
    pub kind: SignalKind,

    pub correlation_id: Option<CorrelationId>,
    pub causation_id: Option<CausationId>,
    pub source_ref: Option<OpaqueRef>,

    pub payload: Option<DataRef>,
    pub payload_hash: Option<ContentHash>,

    pub occurred_at: Option<u64>,
    pub received_at: u64,

    pub control_context: Option<ControlMutationContext>,
}
```

The envelope should remain bounded:

- namespace and kind have maximum lengths and normalized syntax;
- opaque references have maximum lengths;
- inline payloads have a strict byte cap;
- large payloads use immutable references;
- no unbounded metadata map is accepted in the first version.

### 9.3 Signal identity and deduplication

Signal admission is idempotent by `(tenant_id, signal_id)`.

```text
same SignalId + same canonical digest
    → AlreadyAccepted

same SignalId + different canonical digest
    → Conflict

new SignalId
    → Accepted
```

A producer that receives transport uncertainty can safely retry the same envelope.

The signal digest should include every field that can affect matching or resumed input. `received_at` may be assigned by ActionQueue and therefore excluded from caller-side canonicalization.

### 9.4 Matching grammar

The first implementation should use a deliberately small closed grammar:

```rust
pub struct SignalFilter {
    pub tenant_id: Option<TenantId>,
    pub namespace: SignalNamespace,
    pub kind: SignalKind,
    pub correlation_id: Option<CorrelationId>,
    pub source_ref: Option<OpaqueRef>,
}
```

Matching is structural equality plus optional wildcard omission.

The matcher must not support in the first tranche:

- regular expressions;
- unrestricted globbing;
- JSONPath or payload inspection;
- user-provided code;
- natural-language predicates;
- network lookups;
- mutable external state.

Those features would make replay expensive, unsafe, and potentially non-deterministic. Applications that need semantic filtering should normalize their domain event into a structural signal before admission.

### 9.5 Correlation should be preferred over broad matching

The safest wait names a unique correlation identifier:

```text
namespace: worker
kind: terminal
correlation_id: delegated-run/4f1b...
```

This avoids stale or unrelated signals satisfying a new wait.

Broad filters such as `namespace=inventory, kind=updated` may still be useful for reactive workflows, but they require an explicit signal cursor or eligibility window.

### 9.6 Signal positions and ordering

Every admitted signal receives a monotonic `SignalSequence` derived from WAL order.

Where more than one retained signal matches a `FirstMatch` wait, the signal with the lowest eligible sequence wins. The selected `signal_id` and sequence are recorded in `WaitSatisfied`.

Determinism must not depend on:

- wall-clock ties;
- hash-map iteration order;
- arrival-thread scheduling;
- actor identity ordering;
- payload serialization accidents.

### 9.7 Delivery is fan-out, not queue consumption

Signals should be immutable observations, not consumable messages.

A single signal may satisfy multiple compatible waits. Each wait may be satisfied at most once.

This prevents one wait from stealing a signal another wait legitimately observes. Exclusive work distribution remains the responsibility of tasks, leases, actors, and concurrency gates.

### 9.8 Initial wait policies

The first release should support only:

```rust
pub enum WaitMatchPolicy {
    FirstMatch,
}
```

Later extensions may add `All` or quorum semantics, but only after their retention, timeout, ordering, and replay behavior is specified formally.

Avoid beginning with a general Boolean expression tree. The common coordination cases can be decomposed into tasks and multiple continuation attempts.

### 9.9 Retention

Durable pre-registration matching requires signals to remain available beyond the tick in which they arrive.

The runtime should define:

- a minimum retention interval;
- a minimum retained sequence horizon;
- pinning while a signal is referenced by a satisfied wait or resume context;
- compaction rules once no durable object references the signal;
- operator-configurable archival for audit-heavy deployments.

A signal must not be deleted while:

- it is the selected wake signal for an unconsumed resume context;
- a historical attempt record directly references it and the configured history policy requires local availability;
- a snapshot requires it for deterministic reconstruction.

Payload artifacts may have a separate retention lifecycle, but the signal record and content hash remain.

### 9.10 Signal sources

Signals may originate from:

- the dispatch loop itself after a task transition;
- an embedded application API;
- the daemon API;
- an authenticated remote actor gateway;
- a timer synthesized from a wait deadline;
- a bridge from another ActionQueue instance;
- a downstream system's outbox dispatcher.

All sources converge on the same mutation-authority path.

### 9.11 Internal events versus durable signals

The current `ActionQueueEvent` remains useful for transient internal coordination and metrics. It should be renamed or documented as an internal event stream and should not be the durability contract for external continuation.

The distinction should be explicit:

```text
InternalEvent
    process-local notification derived during a tick

SignalEnvelope
    durably admitted structural input with identity and replay semantics

WalEvent
    authoritative persistence record of queue state mutation
```

An internal event may cause the runtime to propose a durable signal where configured. It is not itself the durable signal.

### 9.12 Signal admission result

```rust
pub enum SignalAdmissionOutcome {
    Accepted {
        signal_id: SignalId,
        sequence: SignalSequence,
    },
    AlreadyAccepted {
        signal_id: SignalId,
        sequence: SignalSequence,
    },
}

pub enum SignalAdmissionError {
    Conflict {
        signal_id: SignalId,
        existing_digest: ContentHash,
        submitted_digest: ContentHash,
    },
    InvalidEnvelope(SignalValidationError),
    TenantMismatch,
    UnauthorizedIngress,
    Storage(StorageError),
}
```

`UnauthorizedIngress` is relevant only where the hosting surface performs authentication. The core may receive a prevalidated `ControlMutationContext` from its host.

---

## 10. Race-free wait registration and wake-up

### 10.1 The lost-wakeup failure

A naïve continuation implementation does this:

```text
1. Request external work.
2. External work completes quickly.
3. Completion event is emitted.
4. Handler registers a subscription.
5. Handler suspends.
6. No future event occurs.
7. Run waits forever.
```

Persisting the subscription does not solve the problem if the event itself was ephemeral.

The reverse race also matters:

```text
1. Handler records “I will wait.”
2. Process crashes before the run state and checkpoint are durable.
3. Signal arrives and is matched inconsistently.
4. Recovery cannot reconstruct whether the handler yielded.
```

### 10.2 The two-part solution

ActionQueue must provide both:

1. a durable signal inbox that retains pre-registration signals; and
2. a compound attempt disposition that atomically records checkpoint, wait, lease release, attempt finish, and `Running → Awaiting`.

Either part without the other leaves a correctness gap.

### 10.3 Wait specification

```rust
pub struct WaitSpec {
    pub wait_id: WaitId,
    pub filter: SignalFilter,
    pub match_policy: WaitMatchPolicy,
    pub eligible_from: SignalEligibility,
    pub deadline_at: Option<u64>,
    pub timeout_policy: WaitTimeoutPolicy,
}

pub enum SignalEligibility {
    AnyRetained,
    After(SignalSequence),
}

pub enum WaitTimeoutPolicy {
    ResumeWithTimeout,
    FailRun { code: BoundedCode },
    CancelRun,
}
```

For externally correlated operations, `AnyRetained` is acceptable only when a sufficiently unique correlation identifier is present. Broad filters should use `After(sequence)`.

### 10.4 Atomic wait establishment

The storage authority validates the complete awaiting disposition against a scratch projection before WAL append.

The resulting durable semantic event should establish, as one indivisible fact:

- the attempt's `Awaiting` result;
- the checkpoint reference;
- the wait specification;
- lease release;
- run transition from `Running` to `Awaiting`;
- any child admissions that must exist before waiting;
- any signals emitted by the disposition;
- budget consumption for the attempt.

A practical first implementation should use a purpose-built compound event rather than immediately inventing a general multi-event transaction language:

```rust
WalEvent::AttemptDispositionCommitted {
    sequence,
    run_id,
    attempt_id,
    disposition,
    timestamp,
}
```

The reducer applies every contained structural effect or none.

If later unrelated features require arbitrary atomic mutation groups, this compound-event pattern can be generalized deliberately.

### 10.5 Match after registration

After the compound event is durable and applied, the matcher checks retained eligible signals.

If a match already exists, the runtime appends `WaitSatisfied` and promotes the run to `Ready`. It is valid for an awaiting state to be short-lived or visible only in WAL history.

### 10.6 Match after signal arrival

When a new signal is admitted, the matcher queries active waits indexed by tenant, namespace, kind, and correlation. Matching waits receive a proposed `WaitSatisfyCommand` through mutation authority.

The durable event records:

```rust
pub struct WaitSatisfaction {
    pub wait_id: WaitId,
    pub run_id: RunId,
    pub signal_id: SignalId,
    pub signal_sequence: SignalSequence,
    pub satisfied_at: u64,
}
```

The same command replayed or retried is idempotent.

### 10.7 Wake promotion

Satisfying a wait and promoting the run should be one compound semantic transition:

```text
Wait active + Run Awaiting
    → Wait satisfied by Signal S
    → ResumeContext recorded
    → Run Ready
```

This prevents recovery from seeing a satisfied wait with no way to deliver the signal, or a ready run with no recorded wake reason.

### 10.8 Resume consumption

The `ResumeContext` remains attached to the run until an attempt starts with it. Attempt start records the resume-context identity it consumed.

If the process crashes after leasing but before attempt start, the context remains.

If it crashes after attempt start, uncertainty follows the normal attempt recovery rules. The new attempt history shows which signal was supplied.

### 10.9 Timeout handling

A wait deadline is evaluated by the scheduler, not by a sleeping executor.

At or after `deadline_at`, the runtime produces a durable timeout transition according to policy:

- `ResumeWithTimeout` records a synthetic wake reason and returns the run to `Ready`;
- `FailRun` records a terminal failure with a bounded machine-readable code;
- `CancelRun` records cancellation.

A real matching signal and a timeout racing in the same tick are ordered by WAL admission. The first valid mutation wins; the second becomes an idempotent no-op or typed `WaitAlreadyResolved` result.

### 10.10 Multiple waiters

One signal may satisfy many waits. The matcher should batch lookup but submit independently attributable wait satisfactions.

A failure while satisfying waiter B must not invalidate the already durable signal or waiter A's successful transition.

### 10.11 Deterministic recovery algorithm

On bootstrap:

1. replay tasks, runs, attempts, signals, waits, satisfactions, and cancellations;
2. load snapshot indexes if compatible;
3. rebuild the active-wait index;
4. rebuild the retained-signal index;
5. identify active waits that already have eligible matching signals;
6. order matches by signal sequence and wait identifier for deterministic processing;
7. submit missing satisfaction transitions through mutation authority;
8. evaluate expired deadlines;
9. only then begin normal dispatch.

Recovery must not synthesize a match that cannot be justified from durable signal and wait records.

---

## 11. Checkpoints and resume context

### 11.1 Separate result from continuation state

Final handler output and continuation checkpoint are different concepts:

```text
Output
    Result of a completed attempt or run.

Checkpoint
    Partial state needed to continue a nonterminal run.
```

Both may be opaque, but they have different lifecycle and inspection semantics.

### 11.2 Data reference model

```rust
pub enum DataRef {
    Inline(InlineData),
    External(ExternalDataRef),
}

pub struct InlineData {
    pub content_type: Option<BoundedString>,
    pub bytes: Vec<u8>,
    pub hash: ContentHash,
}

pub struct ExternalDataRef {
    pub scheme: BoundedString,
    pub locator: BoundedString,
    pub hash: ContentHash,
    pub size_bytes: Option<u64>,
    pub content_type: Option<BoundedString>,
}

pub struct CheckpointRef {
    pub checkpoint_id: CheckpointId,
    pub data: DataRef,
    pub created_by_attempt: AttemptId,
}
```

ActionQueue does not fetch or interpret external checkpoint contents. The executor host or application resolves the reference.

### 11.3 Inline limits

The runtime should set conservative limits, for example:

- handler final inline output: configurable, bounded;
- checkpoint inline bytes: lower default than final output;
- signal inline payload: lower still;
- compound disposition total serialized size: hard cap.

Large model contexts, source archives, receipts, and worker transcripts belong in an artifact store.

### 11.4 Resume context

```rust
pub struct ResumeContext {
    pub wait_id: WaitId,
    pub checkpoint: Option<CheckpointRef>,
    pub wake: WakeReason,
    pub resumed_at: u64,
}

pub enum WakeReason {
    Signal {
        signal_id: SignalId,
        signal_sequence: SignalSequence,
        envelope: SignalEnvelope,
    },
    Deadline {
        wait_id: WaitId,
        deadline_at: u64,
    },
    AdministrativeResume {
        control_context: Option<ControlMutationContext>,
    },
}
```

`AdministrativeResume` applies to `Suspended`, not an unresolved `Awaiting` wait. An operator wishing to bypass a wait must use an explicit wait-resolution control mutation that is separately visible.

### 11.5 Handler input

```rust
pub struct HandlerInput {
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub payload: Vec<u8>,
    pub metadata: AttemptMetadata,
    pub causal_context: CausalContext,
    pub resume_context: Option<ResumeContext>,
    pub cancellation_context: CancellationContext,
}
```

The original payload remains stable across attempts. The resume context carries only what changed due to continuation.

### 11.6 Checkpoint immutability

A checkpoint reference is immutable once committed. A later attempt may create a new checkpoint but cannot mutate the earlier record.

This supports:

- deterministic history;
- debugging across attempts;
- content-addressed deduplication;
- detection of a backend returning different bytes for the same reference.

### 11.7 Checkpoint availability failure

If the resumed handler cannot resolve an external checkpoint, ActionQueue should not reinterpret the domain failure. It reports a handler failure under normal retry policy.

The operator surfaces should make the structural cause visible:

```text
run resumed from wait W
checkpoint ref C supplied
handler reported checkpoint unavailable
```

Applications requiring stronger guarantees should use an artifact backend with retention tied to ActionQueue history.

### 11.8 Privacy and retention

Checkpoints may contain sensitive application state. ActionQueue should:

- avoid logging inline bytes;
- expose hashes and content types by default;
- allow operator APIs to redact locators;
- support configurable history retention;
- never treat opaque checkpoint content as searchable metadata.

---

## 12. Attempt dispositions and compound durability

### 12.1 Replace isolated output with a complete disposition

The current `HandlerOutput` maps directly to retry and terminal outcomes. The target contract should make all end-of-attempt proposals explicit.

```rust
pub struct AttemptDisposition {
    pub outcome: AttemptOutcome,
    pub output: Option<DataRef>,
    pub checkpoint: Option<CheckpointRef>,
    pub wait: Option<WaitSpec>,
    pub child_admissions: Vec<ChildAdmission>,
    pub emitted_signals: Vec<SignalProposal>,
    pub consumption: Vec<BudgetConsumption>,
}

pub enum AttemptOutcome {
    Complete,
    RetryableFailure { error: BoundedError },
    TerminalFailure { error: BoundedError },
    Timeout { error: BoundedError },
    Suspended { reason: Option<BoundedString> },
    Awaiting,
}
```

The exact Rust shape may use enum-associated fields instead of a struct. The essential rule is that invalid combinations are unrepresentable or rejected before durability.

### 12.2 Combination rules

Examples:

```text
Complete
    may carry output
    must not carry wait or checkpoint

Awaiting
    must carry exactly one WaitSpec
    may carry checkpoint
    may carry child admissions and emitted signals

Suspended
    may carry checkpoint
    must not carry WaitSpec

RetryableFailure / TerminalFailure / Timeout
    carry bounded error
    do not carry child admissions by default
```

A later ADR may permit “commit children even if the parent fails,” but the first version should reject that ambiguity.

### 12.3 Compound attempt command

```rust
pub struct AttemptDispositionCommitCommand {
    pub expected_sequence: u64,
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub expected_state: RunState,
    pub expected_lease: LeaseFence,
    pub disposition: AttemptDisposition,
    pub timestamp: u64,
}
```

The storage authority validates:

- attempt is currently active;
- run is `Running`;
- lease fence matches;
- outcome combination is valid;
- child admissions are valid;
- tenant and parentage constraints hold;
- dependencies exist or are included in the same disposition;
- no dependency cycle is introduced;
- wait filter and deadline are valid;
- emitted signals are valid and non-conflicting;
- serialized compound event is within size limits;
- target run transition is legal.

Only then is the compound event appended and synced according to durability policy.

### 12.4 Why a purpose-built compound event is preferred initially

The current mutation API submits one typed command at a time. A general transaction mechanism would require decisions about:

- multi-frame commit markers;
- sequence ranges;
- snapshot cut positions;
- replay of uncommitted batches;
- partial truncation;
- cross-command error reporting;
- interleaving constraints.

The hardening has two concrete atomic groups: task admission and attempt disposition. Purpose-built compound events solve the immediate correctness problem with a smaller state-space.

Generalize only after a third independent use case proves the need.

### 12.5 Target handler contract

`AttemptDisposition` replaces `HandlerOutput` as a breaking API boundary. The target executor accepts one handler contract only:

```text
Success / failure / timeout / suspension / continuation
    → one validated AttemptDisposition
```

No blanket or pre-contract handler adapter and no deprecation window are part of `AQ-CONT-1`. Frozen baseline handlers may be exercised in an offline characterization harness, but they do not compile into the target runtime. This keeps continuation semantics explicit and prevents two result models from becoming concurrent authority paths.

### 12.6 Attempt numbers and lineage

The projection should retain:

- physical attempt ordinal;
- failure-attempt ordinal;
- result kind;
- consumed resume-context ID;
- checkpoint produced;
- wait created;
- child task IDs admitted;
- signals emitted;
- budget consumption;
- causal context snapshot or reference.

This removes the need to infer effective retry count by repeatedly scanning and subtracting suspended attempts.

A projected field such as `failure_attempt_count` can be derived deterministically and snapshotted.

### 12.7 Disposition rejection

A rejected disposition is a runtime/integration failure, not a partially successful attempt.

The engine should:

1. retain the run in a recoverable state;
2. release or expire the lease deterministically;
3. record a bounded internal failure event;
4. surface the exact validation error;
5. apply a configured engine policy: fail the run, retry the handler, or halt dispatch for invariant violation.

For production defaults, semantic invalidity produced by a handler should terminally fail the run rather than crash the entire daemon. Storage corruption or impossible projection mismatch may still halt.

### 12.8 Emitted signals

Signals emitted in the disposition are committed atomically with the attempt. This is useful for internal coordination:

```text
child completed
→ child disposition emits structural signal
→ parent wait can match after commit
```

The emitted signal's source and causation are filled by the runtime, not trusted from arbitrary handler text.

---

## 13. Task admission and dynamic submission

### 13.1 Admission should be one semantic operation

Task admission is more than inserting `TaskSpec`. It establishes:

- durable intent;
- exact run derivation;
- tenant identity;
- parentage;
- dependencies;
- causal context;
- optional routing traits and budgets;
- an idempotency record.

The target representation is:

```rust
pub struct AdmissionPlan {
    pub admission_key: AdmissionKey,
    pub task_spec: TaskSpec,
    pub runs: Vec<RunInstance>,
    pub dependencies: Vec<TaskId>,
    pub parent_task_id: Option<TaskId>,
    pub causal_context: CausalContext,
    pub digest: AdmissionDigest,
}
```

Run derivation remains an engine responsibility. Callers should not normally construct `runs`; the planner creates and validates the complete plan before mutation submission.

### 13.2 `ensure_task`

```rust
pub enum EnsureTaskOutcome {
    Created {
        task_id: TaskId,
        admission_key: AdmissionKey,
        digest: AdmissionDigest,
    },
    AlreadyExists {
        task_id: TaskId,
        admission_key: AdmissionKey,
        digest: AdmissionDigest,
    },
}

pub fn ensure_task(
    &mut self,
    request: EnsureTaskRequest,
) -> Result<EnsureTaskOutcome, AdmissionError>;
```

The durable admission index maps tenant-scoped admission keys to task ID and digest.

### 13.3 Canonical digest

Canonicalization must be specified and versioned. It should include:

- task payload bytes and content type;
- run policy;
- constraints;
- structural metadata fields that affect execution;
- parent task;
- normalized dependency set;
- tenant;
- causal context fields intended to be immutable;
- admission-schema version.

Human-readable descriptions may either be included or explicitly excluded. The choice must be stable and documented.

The digest must not depend on map iteration order or platform-specific serialization.

### 13.4 Admission conflict

```rust
pub enum AdmissionError {
    Conflict {
        admission_key: AdmissionKey,
        existing_task_id: TaskId,
        existing_digest: AdmissionDigest,
        submitted_digest: AdmissionDigest,
    },
    InvalidTask(TaskSpecError),
    InvalidParent { parent: TaskId },
    TerminalParent { parent: TaskId },
    UnknownDependency { task_id: TaskId },
    DependencyCycle,
    TenantMismatch,
    Derivation(DerivationError),
    TooLarge,
    Storage(StorageError),
}
```

A conflict is not resolved by overwriting or returning the old task as success. It signals a caller bug, stale outbox, or idempotency-key collision.

### 13.5 Compound admission commit boundary

A task admission must become replay-visible through one **semantic commit boundary** covering:

```rust
AdmissionCommit {
    admission_key,
    digest,
    task_spec,
    derived_runs,
    dependencies,
    parentage,
    causal_context,
    timestamp,
}
```

The reducer applies the task, runs, dependency edges, hierarchy edge, and admission index atomically. Recovery must observe either the complete admission or none of it.

This requirement does **not** prematurely mandate one physically large WAL frame. Viable implementations include:

- one bounded compound WAL event;
- a framed mutation batch followed by a durable commit marker;
- another transaction envelope whose replay semantics are all-or-nothing.

The physical representation is an ADR and capacity decision. The invariant is semantic atomicity, bounded record size, deterministic replay, and no externally visible intermediate state between `TaskCreate`, `RunCreate`, and `DependencyDeclare`.

### 13.6 Parent-child submission at attempt completion

The preferred coordinator pattern is transactional:

```rust
AttemptDisposition::Awaiting {
    checkpoint,
    wait,
    child_admissions: vec![...],
}
```

The parent does not claim that delegation exists until the entire disposition commits.

The returned or later queryable disposition record contains the admitted child IDs.

### 13.7 Mid-attempt admission is deferred from the initial contract

The initial `AQ-CONT-1` implementation should prefer attempt-end compound dispositions. A handler can preallocate child `TaskId` values, refer to them in its checkpoint and wait specification, and return the child admissions with the disposition that ends the attempt.

A synchronous submission call from inside a handler is not part of the first tranche. It can deadlock or create re-entrancy hazards if the executor is waiting for the handler while the same dispatch authority must durably acknowledge the request.

If a later, demonstrated use case requires mid-attempt admission, the API must be:

- asynchronous or serviced by an independently progressing mutation authority;
- explicitly forbidden while a dispatch-state lock is held;
- idempotent through `AdmissionKey` and digest;
- cancellation-aware;
- proven by deadlock, crash, and backpressure tests;
- incapable of treating an in-memory channel enqueue as durable success.

Until those conditions are met, long-horizon coordinators use attempt-end transactional admission or an application outbox.

### 13.8 Deletion of the fire-and-forget port

The current `TaskSubmissionPort` is removed from the target runtime. It is not renamed, deprecated in place, or preserved as an alternate admission path.

All durable coordinator work uses one of two mechanisms:

```text
attempt-end compound child admission
application domain outbox → ensure_task(AdmissionKey, digest)
```

The baseline silent-drop behavior becomes a negative regression fixture. New examples, APIs, metrics, and docs contain no best-effort submission port.

### 13.9 Admission limits

To protect the single writer and WAL:

- maximum child admissions per disposition;
- maximum runs derived per admitted task in one event;
- maximum dependency count;
- maximum aggregate serialized bytes;
- maximum hierarchy depth;
- maximum causal-reference length;
- maximum inline payload and metadata size.

Large fan-out should be staged across coordinator attempts rather than committed in one enormous frame.

### 13.10 Task ID policy

`TaskId` remains caller-supplied in the core model, but high-level admission APIs may:

- accept an explicit ID;
- deterministically derive an ID from tenant and admission key;
- generate an ID and return it after durable admission.

For cross-instance outbox use, deterministic IDs are helpful but not sufficient. The admission key and digest remain the authoritative idempotency contract.

---

## 14. Cross-engine handoff and the outbox contract

### 14.1 No cross-WAL transaction

Two ActionQueue instances cannot atomically share a local mutation without introducing distributed consensus or a transactional external database. This proposal intentionally does not add that machinery.

The owning application uses a durable outbox.

### 14.2 Required downstream pattern

```text
Local durable transaction:
    domain intent I created
    outbox record O(I) created

Dispatcher:
    construct stable admission key K(I)
    call remote ensure_task(K, plan)

Remote response:
    Created or AlreadyExists

Local durable transaction:
    record remote TaskId / RunId
    mark outbox delivered
```

If the process crashes after remote creation but before local delivery is recorded, the same `ensure_task` call returns `AlreadyExists`.

### 14.3 ActionQueue's obligation

ActionQueue guarantees only:

- stable idempotent admission result;
- conflict detection;
- durable task identity once admitted;
- queryability by admission key;
- causal context preservation supplied in the admission.

It does not guarantee that the local outbox marks itself delivered.

### 14.4 Completion return path

A downstream system may return completion through:

- polling the remote task/run API;
- a durable signal emitted by a bridge;
- an application-level callback converted to a signal;
- a parent-child relationship inside one ActionQueue instance.

ActionQueue does not require one topology.

### 14.5 Cross-instance signal identity

A bridge should derive stable signal identity from the remote durable event:

```text
signal_id = hash(remote_instance_id, remote_event_sequence)
correlation_id = local_wait_correlation
source_ref = remote task/run reference
```

Retrying the bridge is safe.

### 14.6 Handoff failure matrix

| Failure point | Expected behavior |
|---|---|
| Local intent committed; remote call never made | Outbox retries. |
| Remote rejects invalid plan | Outbox records explicit rejection; application decides. |
| Remote creates task; response lost | Retry returns `AlreadyExists`. |
| Remote returns conflict | Caller halts that outbox item; no silent substitution. |
| Local records delivery; remote later unavailable | Remote WAL remains source of remote task truth. |
| Completion signal emitted twice | Signal dedup returns `AlreadyAccepted`. |
| Completion arrives before local wait commits | Durable signal is retained and matches after wait registration. |

### 14.7 Cross-document identity mapping

The coordinated design set uses four distinct identities that must never be collapsed:

| Identity | Owner | Question answered |
|---|---|---|
| `AdmissionKey` | ActionQueue caller/queue | Has this exact task proposition already been durably admitted to this queue? |
| `TaskId` / `RunId` / `AttemptId` | ActionQueue | Which durable task, scheduled occurrence, and execution attempt is this? |
| `EffectIntentId` | WorldInterface | Which accountable proposed boundary effect is being advanced? |
| `WorkOrderId` / `CommitmentId` | Exoskeleton | Which delegated objective or enduring undertaking gives the work purpose? |

A common WorldInterface dispatch may therefore use an admission key derived from an `EffectIntentId`, create an ActionQueue task and run, and carry the Exoskeleton work order or commitment only as an opaque purpose reference. External provider idempotency remains a WorldInterface decision and is not inferred by ActionQueue from these identifiers.

### 14.8 Why `RunId` still matters

Admission idempotency prevents duplicate tasks. `RunId` remains the stable execution identity for downstream side-effect deduplication. The two keys solve different problems:

```text
AdmissionKey
    “Did this durable task proposition already enter this queue?”

RunId
    “Is this the same scheduled occurrence being attempted again?”
```

---

## 15. Causal context and control attribution

### 15.1 Purpose

ActionQueue should make it possible to answer:

- which task or external origin caused this task;
- which trace and correlation chain it belongs to;
- which opaque principal and actor references the caller supplied;
- which purpose and authorization records the application associated with it;
- which control-plane actor later canceled or resumed it.

It should not answer what those references mean.

### 15.2 Proposed causal context

```rust
pub struct CausalContext {
    pub trace_id: TraceId,
    pub correlation_id: CorrelationId,
    pub causation: Option<CausationLink>,

    pub submitting_principal_ref: Option<OpaqueRef>,
    pub requesting_actor_ref: Option<OpaqueRef>,
    pub purpose_ref: Option<OpaqueRef>,
    pub authorization_ref: Option<OpaqueRef>,

    // Optional references produced by an authenticated host.
    // ActionQueue does not resolve or verify them.
    pub identity_context_ref: Option<OpaqueRef>,
    pub signed_statement_ref: Option<OpaqueRef>,
    pub proof_context_ref: Option<OpaqueRef>,

    pub origin_ref: Option<OpaqueRef>,
}

pub struct CausationLink {
    pub parent_task_id: Option<TaskId>,
    pub parent_run_id: Option<RunId>,
    pub parent_attempt_id: Option<AttemptId>,
    pub external_ref: Option<OpaqueRef>,
}
```

The exact number of references should stay small. Avoid a free-form metadata map that becomes an unversioned domain dumping ground. `identity_context_ref`, `signed_statement_ref`, and `proof_context_ref` are intentionally generic: they may point to EntityID-derived state, a controlled-identifier snapshot, a certificate chain, a platform authentication record, or another verified scheme.

### 15.3 Propagation rules

For a child admitted by a running attempt:

- `trace_id` is inherited;
- `correlation_id` is inherited by default but may be explicitly forked;
- causation points to the creating task/run/attempt;
- principal and purpose refs inherit unless the host supplies an attested override; ActionQueue does not validate the domain meaning of that override;
- authorization ref may inherit, attenuate, or change according to the downstream application, but ActionQueue records only the supplied reference;
- origin ref is preserved.

The engine fills parent run and attempt IDs; handlers do not forge them.

### 15.4 Task, run, and attempt storage

The canonical context lives on the task admission record. Runs reference the task context. Attempts record the context version or digest supplied at start, plus their consumed resume context.

Because context is immutable, a task does not silently change principal or purpose mid-lifecycle.

A new task should be admitted when materially different attribution is required.

### 15.5 Control mutation context

```rust
pub struct ControlMutationContext {
    pub caller_ref: OpaqueRef,
    pub host_session_ref: Option<OpaqueRef>,
    pub request_id: Option<OpaqueRef>,
    pub reason_code: Option<BoundedCode>,
}
```

It attaches to actions such as:

- task cancellation;
- run cancellation;
- engine pause/resume;
- run suspend/resume;
- wait bypass or cancellation;
- signal admission;
- actor registration/deregistration;
- task admission;
- budget replenishment;
- future reprioritization.

The daemon or platform host constructs this context after whatever authentication it performs. ActionQueue records the host-attested references but does not authenticate the represented principal or interpret its authority. Embedded callers may omit the fields or provide references trusted within that host.

### 15.6 Attribution does not authorize

This must be prominent in code documentation:

```text
An authorization_ref is not an authorization decision.
A principal_ref is not proof of identity.
A purpose_ref is not validation of purpose.
```

ActionQueue persists these fields so the system that owns the semantics can correlate history. WorldInterface still validates its own `AuthorizationEnvelope` before an effect.

### 15.7 Indexing

Useful indexes include:

- trace ID → task IDs;
- correlation ID → tasks, runs, waits, and signals;
- admission key → task ID;
- caller ref → control mutations where enabled;
- origin ref → task IDs.

Opaque principal and authorization references should be indexed only when configured, because they may have privacy or cardinality costs.

### 15.8 Bounded references

`OpaqueRef` should have:

- a maximum UTF-8 byte length;
- no control characters;
- stable exact comparison;
- optional normalized URI-like syntax;
- redacted display support;
- no automatic dereference.

### 15.9 Observability propagation

Tracing spans should automatically include:

```text
trace_id
correlation_id
TaskId
RunId
AttemptId
WaitId when relevant
SignalId when relevant
```

Opaque principal and authorization refs should not be placed in high-volume logs by default.

### 15.10 Developmental campaign attribution profile

A higher layer may use the existing fields to preserve developmental execution lineage:

```text
trace_id       → structural execution tree
correlation_id → caller-defined arm, paired block, or campaign execution set
causation      → parent task/run/attempt or external cause
origin_ref     → one immutable application-owned campaign execution context
```

The referenced application record may identify a campaign, intervention arm, frozen evaluation plan, task distribution, component fingerprint, or replication block. It is not an ActionQueue type. ActionQueue does not dereference it, copy it into a free-form metadata map, or infer scheduling policy from it.

Any priority, budget, deadline, constraint, or executor difference between arms must use the ordinary explicit queue field. This makes the difference inspectable as execution policy rather than hiding it inside an opaque reference.

A physical retry remains an `AttemptId`, not an experimental replication. Queue terminal states remain execution facts, not evaluation or acceptance. The full clarification and conformance cases are in `aq-cont-1-developmental-campaign-execution-profile.md` and `aq-cont-1-developmental-campaign-acceptance-matrix.yaml`.

---

## 16. Routing traits are not authority

### 16.1 Rename the concept

The current `ActorCapabilities` and `required_capabilities` should be renamed to one of:

- `ExecutorTraits` and `required_executor_traits`;
- `RoutingLabels` and `required_routing_labels`;
- `ExecutionQualifiers` and `required_execution_qualifiers`.

This proposal recommends **ExecutorTraits** because it states both the subject and limited purpose.

### 16.2 Semantics

An executor trait says:

> This executor advertises that it can process tasks requiring this structural qualification.

Examples:

```text
runtime:wasm
runtime:node
arch:x86_64
accelerator:gpu
region:us-west
queue:review
```

It does not say:

> This executor is authorized to access a customer database or deploy production software.

### 16.3 Breaking terminology replacement

The target public API and target persistence schema use `ExecutorTraits` and `required_executor_traits` exclusively. `ActorCapabilities` aliases, duplicate response fields, old serde tags, and one-release deprecation bridges are intentionally omitted.

The frozen baseline tag preserves the old spelling for historical inspection. The target starts a new versioned wire and storage contract, and CI rejects reintroduction of the old routing term outside baseline documentation and fixtures.

### 16.4 Self-declaration risk

Executor traits may be self-declared for routing convenience. Therefore they cannot cross a security boundary without host validation.

The actor gateway or platform control plane may restrict which traits an authenticated actor is allowed to register. That remains routing-policy enforcement, not external-resource authorization.

### 16.5 RBAC terminology

The platform crate also has typed `Capability` grants for queue-control RBAC. That concept is closer to authority than actor routing traits, but still local to ActionQueue's administrative surface.

Documentation should distinguish three meanings:

```text
ExecutorTrait
    routing eligibility

ActionQueue RBAC Capability
    permission to call an ActionQueue control operation

Downstream Resource Capability
    bounded authority over an external resource or effect
```

No automatic conversion exists between them.

As part of the rename, core comments and examples should also remove product-specific executor language such as "Caelum Vessel" or "organization hub." Those are valid downstream deployments, not ActionQueue ontology.


---

## 17. Subscriptions and durable signals

Current subscriptions contain a useful idea—durably start or promote work when a queue-observable condition occurs—but the target does not preserve the old serialized filters or transient custom-event path.

### 17.1 Three distinct mechanisms

```text
Reactive subscription
    Starts or promotes a task when a durable queue condition occurs.

Continuation wait
    Resumes a specific nonterminal run with checkpoint and wake context.

Internal event listener
    Observes process-local events for metrics or projections only.
```

They may share matching code, but never share ambiguous lifecycle semantics.

### 17.2 Target reactive subscriptions

Target subscriptions are expressed over typed structural queue evidence such as task terminality, run-state transition, or budget threshold. They are admitted and replayed under the new schema. Existing `EventFilter` bytes are not loaded into the target store.

### 17.3 Durable signals replace transient custom events

The old custom string-key event is deleted as an externally usable coordination mechanism. External or cross-process wake-up uses `SignalEnvelope` admission with identity, deduplication, correlation, payload reference, retention, and replay semantics.

No fallback from durable signals to custom events is permitted.

### 17.4 Queue events need not be duplicated

A task or run transition is already durable in the WAL. A wait may use either:

```rust
pub enum WakeEvidence {
    AdmittedSignal(SignalId),
    QueueEvent(WalEventRef),
}
```

This avoids appending a second signal record for every internal lifecycle event while preserving uniform resume context.

### 17.5 Ownership

General wait, signal, subscription, and deadline infrastructure belongs in a continuation subsystem, not in the budget feature. Budget code emits structural evidence through that subsystem but does not own the registry.

### 17.6 Public API

Use separate methods whose semantics are obvious:

```text
create_subscription(...)
register_wait(...)        // normally only through AttemptDisposition
admit_signal(...)
```

No universal `subscribe()` or `emit_custom()` API hides whether the caller is starting work, continuing work, or merely observing telemetry.

### 17.7 Required deletion tests

The target suite proves that:

- a signal admitted before wait registration remains matchable;
- a wait registered before signal admission wakes exactly once;
- duplicate signal admission is idempotent;
- no process-local registry is required for correctness;
- no old custom-event key can satisfy a target continuation;
- a restart between signal admission and wake promotion preserves both.

## 18. Budgets, cancellation, leases, and concurrency

### 18.1 Budget interaction

Awaiting runs consume no executor time and hold no lease. They should not consume `TimeSecs` merely because wall-clock time passes unless a future budget-account policy explicitly defines that behavior.

Attempt resource consumption is committed with the attempt disposition.

### 18.2 Budget exhaustion during execution

Current behavior remains:

```text
running handler receives cancellation signal
→ handler cooperatively returns Suspended
→ checkpoint may be recorded
→ run becomes Suspended
→ replenishment or explicit resume returns it to Ready
```

A handler should not return `Awaiting` merely because its budget ended. That would falsely claim an external condition is being awaited.

### 18.3 Budget exhaustion before wake

An awaiting run may have its wait satisfied while its task budget is exhausted.

The correct state flow is:

```text
Awaiting
→ wait satisfied and ResumeContext recorded
→ Ready, but budget gate prevents lease acquisition
```

The wake fact is not lost or postponed. The run is ready but ineligible for dispatch until budget is replenished.

Inspector output should distinguish:

```text
wait: satisfied
dispatch: blocked by budget
```

### 18.4 Hierarchical budgets are deferred

The companion systems may eventually require budgets across organizations, Vessels, commitments, delegations, and child tasks. That suggests generic budget accounts rather than only per-task budgets.

This is a legitimate future enhancement, but it is not required to implement durable continuations. It should be a separate ADR and hardening tranche after real usage data exists.

### 18.5 Cancellation races

Cancellation may race with:

- handler completion;
- wait registration;
- signal admission;
- wait satisfaction;
- timeout;
- child admission.

The single mutation authority determines ordering. Each command includes expected state or wait status. Once cancellation commits:

- later attempt dispositions are rejected by lease/state fence;
- active waits are canceled;
- later signals remain durable but cannot wake the canceled run;
- admitted children follow existing hierarchy cascade policy.

### 18.6 Lease fencing

Compound attempt commits must carry the lease owner and expiry/fence observed by the worker result. A stale worker cannot commit an awaiting disposition after its lease expired and another attempt began.

The storage authority validates:

```text
run == Running
active lease owner == result owner
lease fence == expected fence
attempt == active attempt
```

This is especially important when the disposition creates children or emits signals.

### 18.7 Concurrency-key release

The wait policy controls whether an awaiting run retains its concurrency key. The key gate must reconstruct this state from durable run and constraint records after recovery.

Recommended defaults:

| State | Default key behavior |
|---|---|
| Running | Hold |
| RetryWait | Existing `HoldDuringRetry` default |
| Suspended | Preserve current policy/behavior |
| Awaiting | Release |
| Terminal | Release |

### 18.8 Deadline and cancellation signaling to handlers

A wait deadline does not cancel a sleeping handler because no handler exists. It creates a durable transition.

An active-attempt timeout continues to use the executor cancellation mechanism and retry taxonomy.

Keeping these two mechanisms distinct avoids misrepresenting timer behavior.

---

## 19. Workflow, DAG, and hierarchy integration

### 19.1 Keep DAG dependencies as first-class gates

A dependency edge is not merely an event subscription. It defines structural eligibility and failure propagation across a workflow graph.

The redesign should preserve:

- cycle detection;
- terminal-success dependency satisfaction;
- transitive failure propagation;
- snapshot and WAL reconstruction;
- parent-child lifecycle semantics.

Do not rewrite the dependency gate as a pile of signals.

### 19.2 Where continuations complement DAGs

DAG dependencies answer:

> May this task's run become eligible because prerequisite tasks succeeded?

Continuation waits answer:

> May this already-started run resume, and what wake evidence should its next attempt receive?

Both are needed.

A deterministic FlowSpec usually compiles static ordering to DAG dependencies. A coordinator or long-lived worker supervisor may use an awaiting continuation after dynamically creating children.

### 19.3 Child terminal convenience filters

The continuation subsystem may provide typed convenience filters backed by durable queue events:

```rust
pub enum QueueWaitTarget {
    TaskCompleted { task_id: TaskId },
    TaskTerminal { task_id: TaskId },
    RunState { run_id: RunId, state: RunState },
    Signal(SignalFilter),
}
```

These are structural and generic.

`TaskTerminal` should deliver whether completion, failure, or cancellation occurred rather than forcing the parent to poll child snapshots.

### 19.4 Parent completion gating

Existing hierarchy semantics may require a parent not to be considered fully complete until children terminate. The new attempt disposition must not accidentally bypass that gate.

A parent handler returning `Complete` while nonterminal children exist follows one explicit policy:

- reject completion;
- enter an internally derived awaiting-children state;
- or retain current coordinator semantics.

The first hardening release should preserve current behavior and add a separate ADR before changing parent completion semantics.

### 19.5 Cascade cancellation

When a parent is canceled:

- child tasks are canceled according to hierarchy policy;
- parent and child active waits are canceled;
- signals remain in history;
- admission idempotency records remain;
- no orphan child admission is permitted after the parent becomes terminal.

A compound parent disposition validates parent nonterminal state before child admission.

### 19.6 Dynamic fan-out

For large fan-out:

```text
coordinator attempt
    admits bounded batch of children
    checkpoints progress
    awaits batch terminal signal
    resumes and admits next batch
```

This prevents a single WAL compound event from becoming unbounded.

### 19.7 Workflow crash recovery

New acceptance tests must kill the process at every point around:

- parent attempt finish;
- child admission compound event append;
- wait registration;
- signal or child-terminal event;
- wait satisfaction;
- parent promotion;
- resumed attempt start.

After restart, the parent must be in exactly one explainable state with no duplicate children.

---

## 20. Remote actors and platform integration

### 20.1 Remote result protocol

Remote actors must eventually report an `AttemptDisposition`, not only success/failure.

The wire protocol needs:

- run and attempt identity;
- lease fence;
- handler output or checkpoint reference;
- wait specification;
- bounded child admissions where permitted;
- budget consumption;
- emitted signal proposals;
- executor diagnostic metadata outside the durable semantic disposition.

### 20.2 Resume delivery

When a remote actor claims a resumed run, the lease response includes `ResumeContext`. The actor does not fetch continuation state from an unrelated control endpoint unless the protocol explicitly supports content-addressed resolution.

### 20.3 Actor registration

Actor registration uses `ExecutorTraits`. A platform host may allow only approved traits per actor identity.

A registered actor cannot emit arbitrary tenant signals or submit children merely because it advertises a matching trait. Those operations require queue-local RBAC and lease-context validation.

### 20.4 Tenant isolation

All of these objects are tenant-scoped where tenancy is enabled:

- admission key;
- task and run;
- signal identity;
- wait;
- correlation lookup;
- actor;
- causal-context indexes;
- control mutation.

A wait must not match a signal from another tenant even if every textual field is identical.

### 20.5 Queue-control RBAC additions

The platform control plane will likely need typed permissions such as:

```text
TaskEnsure
TaskCancel
RunResume
WaitInspect
WaitResolve
SignalAdmit
SignalInspect
CheckpointInspectMetadata
TraceInspect
```

These are ActionQueue administrative permissions. They remain distinct from downstream resource capabilities.

### 20.6 Multi-tenant fairness is deferred

The current priority/FIFO selection and concurrency model may eventually be insufficient for a centralized NetCorp platform. Potential future work includes:

- weighted fair queues;
- tenant quotas;
- reserved capacity;
- starvation bounds;
- principal-level rate limits;
- connector or executor-pool concurrency classes.

These are scheduling-policy extensions and should not be bundled into the continuation hardening unless measurements show immediate need.

### 20.7 Cross-node reality

ActionQueue remains single-node and single-WAL. Remote actors do not make the queue itself distributed. They execute leased work from the authoritative node.

A high-availability platform may place consensus or active-passive failover around ActionQueue later. This proposal does not weaken the charter's explicit non-goal.

---

## 21. Persistence, WAL, snapshots, and recovery

The target retains the baseline's most valuable persistence principles—WAL authority, deterministic replay, CRC-protected framing, derived snapshots, and one validated mutation lane—while starting a new persistence lineage.

### 21.1 Fresh target format

`AQ-CONT-1` defines a new WAL and snapshot schema. Exact numeric versions are selected during implementation; they are not described as an in-place `v5 → v6` or `v8 → v9` migration.

Pre-contract stores receive a precise error:

```text
Unsupported pre-AQ-CONT-1 storage format.
The experimental format is retained in repository history and fixtures only.
```

The target daemon never silently rewrites an old store and never loads old and new event families into one projection.

### 21.2 Target durable event families

```rust
AdmissionCommitted { /* key, digest, task, runs, edges, context */ }
AttemptDispositionCommitted { /* attempt, fence, disposition, target state */ }
SignalAccepted { /* sequence, envelope, digest */ }
WaitSatisfied { /* wait, wake evidence, resume context */ }
WaitCanceled { /* wait, control context */ }
WaitTimedOut { /* wait, timeout policy */ }
```

The physical transaction representation may be one bounded compound frame or a framed batch with a durable commit marker. Replay semantics are all-or-nothing.

### 21.3 Projection and snapshot state

The target projection includes:

- admission key → task ID and digest;
- task/run/attempt state including `Awaiting`;
- active and resolved waits;
- retained signal identity, sequence, and structural indexes;
- pending resume contexts;
- causal context and control attribution;
- physical and failure-attempt counts;
- executor-trait routing state.

Snapshots are acceleration artifacts and record the exact WAL and signal sequence they cover.

### 21.4 Validation and replay

Every compound command is validated against a scratch projection before append. Recovery observes either the complete semantic command or none of it. A truncated or corrupt tail follows an explicit repair policy; impossible internal state halts rather than being guessed through.

A deterministic projection digest excludes volatile metrics and map iteration order. WAL-only replay and snapshot-plus-tail replay of the same target history must produce the same digest.

### 21.5 Retention

Compaction or archival preserves:

- active tasks, runs, waits, and resume contexts;
- signals eligible for active waits;
- admission keys for at least the producer retry horizon;
- causal records required by declared retention;
- control attribution required for audit.

Large checkpoint and signal payloads remain immutable artifact references with recorded hashes.

### 21.6 Future evolution policy

The clean break applies only to the pre-contract experiment. The first stable target release establishes ordinary product responsibilities:

- explicit format and schema versions;
- supported upgrade ranges;
- source-preserving migration tools for supported target releases;
- backup and restore tests;
- precise unsupported-version errors;
- fixtures from every supported released format;
- no downgrade unless explicitly declared.

`actionqueue storage inspect` reports format, tail health, object counts, and supported upgrade paths. A future `storage upgrade` writes a new destination or uses another source-preserving protocol; it never mutates the only copy before verification.

## 22. Crate and module changes

### 22.1 Recommended crate structure

The existing eleven-crate DAG should remain recognizable.

A twelfth crate is justified once the continuation interfaces stabilize:

```text
actionqueue-continuation
    active wait registry
    retained signal index
    deterministic matcher
    deadline index
```

However, extraction should occur after the types and runtime behavior are proven. The first implementation may use modules in `actionqueue-engine` and `actionqueue-runtime` to avoid premature crate boundaries.

### 22.2 `actionqueue-core`

Add:

```text
ids:
    AdmissionKey / AdmissionId as appropriate
    WaitId
    SignalId
    SignalSequence
    CheckpointId
    TraceId / CorrelationId

run:
    Awaiting state
    new transitions

admission:
    AdmissionPlan domain shape
    AdmissionDigest

continuation:
    SignalEnvelope
    SignalFilter
    WaitSpec
    ResumeContext
    CheckpointRef

causal:
    CausalContext
    ControlMutationContext
    OpaqueRef

executor:
    ExecutorTraits terminology

mutation:
    compound command contracts
```

Keep core free of I/O and matching indexes.

### 22.3 `actionqueue-storage`

Add:

- target `AQ-CONT-1` WAL event encoding/decoding;
- compound-event admission and replay;
- new reducer indexes;
- target `AQ-CONT-1` snapshot mapping;
- format inspection and future-version upgrade hooks;
- corruption and size-limit checks;
- projection digest tooling.

### 22.4 `actionqueue-engine`

Add:

- admission planner;
- wait eligibility and deadline promotion;
- deterministic matcher interfaces;
- `Awaiting` transition logic;
- concurrency-key wait policy;
- scratch validation helpers where they remain pure.

### 22.5 `actionqueue-executor-local`

Add:

- the new handler trait and `AttemptDisposition` contract;
- `AttemptDisposition` conversion;
- `ResumeContext` delivery;
- checkpoint reference support;
- lease-fenced result envelope;
- acknowledged durable submission port;
- tests for yielded attempts and stale result rejection.

### 22.6 `actionqueue-runtime`

Add:

- signal ingress lane;
- wait matcher integration;
- compound disposition processing;
- idempotent admission API;
- recovery reconciliation before dispatch;
- metrics and inspection queries;
- removal of continuation registry ownership from budget integration.

The runtime remains the composition root and mutation coordinator.

### 22.7 `actionqueue-workflow`

Change:

- dynamic submission to use `AdmissionPlan`;
- transactional child admissions in attempt dispositions;
- no synchronous mid-attempt admission in the initial contract; preserve an extension point only after deadlock and re-entrancy proof;
- convenience wait targets for child terminal state;
- deletion of the best-effort submission channel.

### 22.8 `actionqueue-budget`

Change:

- retain budget tracker, gate, consumption, and cancellation behavior;
- remove ownership of general subscription registry;
- emit structural budget events through the continuation integration;
- add tests for wait-satisfied-but-budget-blocked runs.

### 22.9 `actionqueue-actor`

Change:

- rename actor capabilities to executor traits;
- extend remote result protocol;
- deliver resume context;
- enforce tenant and lease-fence rules for signal and child proposals.

### 22.10 `actionqueue-platform`

Change:

- add queue-control RBAC operations for signal and wait APIs;
- index control mutation attribution where configured;
- preserve tenant isolation across admission keys, waits, and signals.

### 22.11 `actionqueue-daemon` and CLI

Expose the APIs and operator surfaces described below. Network-mutating endpoints remain disabled by default unless control mode is enabled.

---

## 23. Embedded, daemon, CLI, and API surfaces

### 23.1 Embedded API

Illustrative methods:

```rust
impl ActionQueueEngine {
    pub fn ensure_task(
        &mut self,
        request: EnsureTaskRequest,
    ) -> Result<EnsureTaskOutcome, AdmissionError>;

    pub fn admit_signal(
        &mut self,
        envelope: SignalEnvelope,
    ) -> Result<SignalAdmissionOutcome, SignalAdmissionError>;

    pub fn get_wait(&self, wait_id: WaitId) -> Option<WaitView>;

    pub fn get_task_by_admission_key(
        &self,
        tenant: Option<TenantId>,
        key: &AdmissionKey,
    ) -> Option<TaskView>;

    pub fn trace(&self, trace_id: TraceId) -> TraceView;
}
```

### 23.2 Daemon API

A versioned surface might include:

```text
POST /api/v2/admissions:ensure
GET  /api/v2/admissions/{key}

POST /api/v2/signals
GET  /api/v2/signals/{signal_id}

GET  /api/v2/waits/{wait_id}
GET  /api/v2/runs/{run_id}/continuation
POST /api/v2/waits/{wait_id}:cancel
POST /api/v2/waits/{wait_id}:resolve   // privileged explicit bypass

GET  /api/v2/traces/{trace_id}
GET  /api/v2/correlations/{correlation_id}
```

The exact REST style may change. Idempotency and conflict response semantics may not.

### 23.3 HTTP status guidance

```text
Created                 201
AlreadyExists           200
Admission conflict      409
Signal accepted         202 or 201
Signal duplicate        200
Signal conflict         409
Invalid structural data 422
Tenant/auth violation   403
Unknown object          404
Storage unavailable     503
```

### 23.4 Mutating endpoint posture

The current daemon assumes trusted-network deployment and has control disabled by default. New signal and admission endpoints increase mutation power.

Recommended rule:

> Network-accessible signal admission, task ensuring, and wait resolution are disabled unless the daemon is explicitly configured with control enabled and an authentication/authorization adapter or trusted ingress declaration.

A reverse proxy may still provide authentication, but ActionQueue should have a hook to receive a host-attested caller reference rather than treating every request as anonymous. The queue does not convert that reference into domain authority.

### 23.5 CLI

```text
actionqueue ensure --admission-key K --spec task.json
actionqueue admission show K

actionqueue signal emit --file signal.json
actionqueue signal show SIGNAL_ID

actionqueue wait show WAIT_ID
actionqueue wait cancel WAIT_ID --reason-code OPERATOR_CANCEL

actionqueue run continuation RUN_ID
actionqueue trace TRACE_ID

actionqueue storage inspect
actionqueue storage backup --output backup-dir
actionqueue storage restore --input backup-dir --verify
```

JSON output should expose structural IDs and statuses, not dump opaque payload bytes by default.

### 23.6 Query consistency

A successful durable admission response must be immediately queryable from the same engine instance because the authority applies the projection before returning.

A daemon may return only after the requested durability policy is satisfied.

### 23.7 Backpressure

Signal and admission APIs must use bounded channels or direct authority calls with backpressure. Do not repeat the unbounded fire-and-forget channel pattern for these critical surfaces.

---

## 24. Observability and inspection

### 24.1 Operator questions

The hardened system should answer directly:

- Why is this run not dispatching?
- Is it scheduled, dependency-blocked, budget-blocked, suspended, or awaiting?
- What exact wait is active?
- Which signal or queue event satisfied it?
- Did the next attempt consume the resume context?
- Which admission key created this task?
- Was an admission newly created or deduplicated?
- Which parent attempt admitted this child?
- Which actor or caller requested cancellation?
- Is a stale executor result being rejected?
- Which retained signals have no matching waits?
- Which waits are approaching deadline?

### 24.2 Run inspection

A run view should include:

```text
state
scheduled_at
lease state
attempt lineage
failure-attempt count
active wait or last wait
checkpoint metadata
pending resume context
budget gate
concurrency-key gate
DAG gate
causal context references
last control mutation
```

### 24.3 Admission inspection

```text
admission key
digest version and value
task ID
created sequence/time
caller/control reference
idempotent replay count
conflict count
parent and dependencies
```

### 24.4 Signal inspection

```text
signal ID
sequence
tenant
namespace/kind
correlation
source ref
payload hash/ref metadata
admission caller
matching waits
retention status
```

### 24.5 Metrics

Recommended counters and gauges:

```text
actionqueue_runs_awaiting
actionqueue_waits_active
actionqueue_waits_satisfied_total
actionqueue_waits_timed_out_total
actionqueue_waits_canceled_total
actionqueue_wait_duration_seconds
actionqueue_signals_admitted_total
actionqueue_signals_deduplicated_total
actionqueue_signal_conflicts_total
actionqueue_signal_match_latency_seconds
actionqueue_admissions_created_total
actionqueue_admissions_deduplicated_total
actionqueue_admission_conflicts_total
actionqueue_disposition_rejections_total
actionqueue_stale_result_rejections_total
actionqueue_resume_context_pending
```

High-cardinality IDs belong in traces, not metric labels.

### 24.6 Tracing spans

Span hierarchy:

```text
admission.ensure
signal.admit
wait.match
wait.satisfy
run.resume
attempt.execute
attempt.disposition.commit
control.cancel
```

Attach structural IDs and outcome codes. Redact opaque payloads and sensitive references.

### 24.7 Health and readiness

Readiness should fail or degrade when:

- WAL cannot append or sync;
- signal/wait indexes cannot reconcile with projection;
- the storage version is unsupported or requires an unavailable supported-target upgrade;
- a pending compound mutation cannot be decoded;
- the control API is enabled without required ingress protection under strict mode.

Large numbers of expired waits or orphaned external artifact refs should alert but not necessarily fail readiness.

### 24.8 Historical explanation

A concise structural timeline might render:

```text
10:14:02 Task admitted under key order-42/settlement
10:14:03 Run leased to executor-7
10:14:04 Attempt 1 started
10:14:05 Attempt 1 yielded Awaiting
         wait: payment/settled correlation=txn-991
         checkpoint: sha256:...
10:17:31 Signal admitted id=sig-88 sequence=4912
10:17:31 Wait satisfied by sig-88
10:17:31 Run promoted Ready
10:17:32 Attempt 2 started with wait W and signal sig-88
10:17:33 Run completed
```

No application payload interpretation is needed to provide this explanation.

### 24.9 Developmental execution inspection

When authorized, the same structural inspector should support filtering by `trace_id`, `correlation_id`, and exact `origin_ref`. It should show:

- admission and physical attempt lineage;
- explicit queue-field differences in priority, budgets, constraints, deadlines, and executor traits;
- waits, signals, checkpoints, retries, cancellation, and recovery events;
- an explicit notice that campaign references are opaque and queue completion is not task-quality or candidate-acceptance evidence.

Campaign, arm, task-distribution, and fingerprint identifiers are high-cardinality and potentially sensitive. They belong in authorized inspection or traces under redaction policy, never in metric labels. ActionQueue exposes no arm-winner, score, binding-constraint, saturation, or activation field.


---

## 25. Security model

### 25.1 Security boundary

ActionQueue's core correctness boundary is still the single mutation authority. Security around who may submit a command belongs to the embedding host, daemon, or platform layer.

The hardening adds new mutation surfaces, so the host contract must become clearer:

```text
Core validates structural invariants.
Host authenticates caller and applies queue-control policy.
Downstream system validates domain authority.
```

### 25.2 Threat: forged signals

An attacker able to admit a matching signal could wake protected work prematurely.

Mitigations:

- network signal ingress disabled by default;
- host-attested caller context in daemon/platform mode;
- tenant scope enforced in the core matcher;
- unguessable correlation IDs for sensitive waits;
- namespace-specific ingress policy in the host;
- optional source-ref restrictions;
- signal ID conflict detection;
- audit of caller and request identity.

ActionQueue does not determine whether `payment.settled` is a truthful claim. The application should admit that signal only after its own verifier or authenticated adapter accepts the source.

### 25.3 Threat: replay

A producer or attacker may submit the same signal repeatedly.

Mitigations:

- stable `SignalId`;
- canonical digest;
- idempotent duplicate result;
- conflict on altered content;
- wait satisfaction at most once.

### 25.4 Threat: stale signal matches new work

A broad retained signal may accidentally satisfy a later wait.

Mitigations:

- require exact correlation for `AnyRetained` in strict mode;
- allow `After(SignalSequence)` eligibility;
- generate fresh correlation IDs per external operation;
- surface broad-filter warnings in validation and CLI;
- keep default matcher grammar narrow.

### 25.5 Threat: cross-tenant wake-up

Mitigation is a hard invariant: tenant is part of both the wait and signal match domain. `None` and `Some(tenant)` do not match in platform mode.

The matcher should partition indexes by tenant before any other key.

### 25.6 Threat: admission-key poisoning

An attacker may preemptively use another caller's predictable admission key with different content.

Mitigations:

- tenant and authenticated principal namespaces at the host boundary;
- sufficiently scoped admission keys;
- optional host-derived admission-key namespace;
- conflict surfaced rather than hidden;
- no public unauthenticated `ensure_task` endpoint.

The core's admission key can remain tenant-scoped; a platform may include organization and principal scope in the key derivation.

### 25.7 Threat: payload and compound-event amplification

Signals or attempt dispositions could exhaust memory, disk, or replay time.

Mitigations:

- strict field and aggregate byte limits;
- bounded child count and dependency count;
- bounded inline payloads;
- external artifact references for large data;
- admission rate limits in the host;
- per-tenant quotas where platform is enabled;
- reject before WAL append;
- metrics for rejected oversized inputs.

### 25.8 Threat: wait explosion

A malicious task could create many active waits or broad filters.

Mitigations:

- one active continuation wait per run in the first version;
- bounded total active waits per tenant/engine;
- broad-filter policy controls;
- deadlines or explicit indefinite-wait permission at the host layer;
- no arbitrary Boolean matcher;
- cancellation and GC tooling.

### 25.9 Threat: stale executor commits

A remote or slow local worker may return a disposition after lease expiry.

Mitigations:

- attempt ID and lease fence in every result;
- storage-authority validation against active lease;
- stale result rejected before any child, signal, or wait mutation;
- metric and audit record for stale result attempts.

### 25.10 Threat: causal-reference forgery

A handler might claim a different principal or authorization reference.

Mitigations:

- task causal context set at admission by the authenticated host;
- runtime fills parent run/attempt causation;
- handler cannot replace immutable context;
- child overrides pass through validated admission policy;
- documentation states that references are attribution, not proof.

### 25.11 Threat: checkpoint locator leakage

External checkpoint locators may reveal secrets or internal topology.

Mitigations:

- locator redaction in logs and default APIs;
- separate metadata-inspection permission;
- short-lived signed locators supplied only to authorized executors;
- content hashes retained even when locators are hidden;
- no automatic dereference by the daemon inspector.

### 25.12 Threat: control-plane ambiguity

An operator may resolve or cancel a wait without a durable attribution record.

Mitigation: every privileged control mutation accepts `ControlMutationContext` and WAL events carry it where configured.

### 25.13 Trusted-network baseline

The current security posture assumes localhost/trusted network and no built-in HTTP authentication. That is acceptable for embedded development but not for centralized signal ingress.

Before NetCorp or any shared platform deployment, authentication and queue-control authorization become mandatory host responsibilities. The hardened core should provide the context hooks and tenant invariants needed for that host, without embedding a particular identity provider.

---


### 25.14 Threat: identity or signature references are treated as authorization

A caller or downstream handler may mistakenly treat the presence of `identity_context_ref` or `signed_statement_ref` as proof that the task is authorized or that a claim is true.

Controls:

- the host authenticates and authorizes every ActionQueue control mutation independently;
- ActionQueue documentation labels these fields as attribution only;
- downstream effect executors re-resolve identity and authorization according to their own policy;
- inspection surfaces show the reference source and explicitly state that ActionQueue did not verify semantic truth;
- tests prove that changing or omitting an identity/proof reference does not alter queue scheduling eligibility unless the authenticated host changes the task itself.

## 26. Performance and capacity

### 26.1 Index design

Active wait lookup should be indexed in descending specificity:

```text
TenantId
→ SignalNamespace
→ SignalKind
→ CorrelationId or wildcard bucket
→ SourceRef or wildcard bucket
→ WaitId
```

A newly admitted signal should not scan every active wait.

Retained signal lookup uses a similar key and ordered sequence collection.

### 26.2 Common-case complexity

Target common cases:

```text
ensure_task existing key       O(1) index lookup + digest compare
signal duplicate               O(1) signal-id lookup
specific signal matching       O(log n + matches)
wait registration with exact correlation
                               O(log n + retained matches)
wait deadline promotion        O(log n) heap/index operation
trace lookup                   O(log n + result count)
```

Exact data structures may use hash indexes plus ordered sequence sets.

### 26.3 WAL frame size

Compound events reduce partial-state risk but can create large frames.

Set explicit defaults, for example conceptually:

```text
maximum compound frame bytes
maximum children per disposition
maximum derived runs per admission
maximum dependencies per task
maximum inline checkpoint bytes
maximum inline signal bytes
```

The actual numbers should be established through benchmarks and documented as configuration with safe hard ceilings.

### 26.4 Matching batches

When one signal matches many waits, the runtime may batch lookup and validation, but each wait satisfaction remains independently durable or belongs to a bounded compound satisfaction batch with clear replay semantics.

Do not let one pathological fan-out create an unbounded WAL frame.

### 26.5 Deadline scheduling

Use a deadline heap or ordered index rather than scanning all waits each tick. Snapshot and replay reconstruct it from active wait records.

### 26.6 Retention pressure

Signal retention can grow without bound if defaults are careless.

The implementation needs measurements for:

- signals per second;
- percentage matching active waits;
- average retention time;
- payload-reference size;
- active waits;
- trace-index cardinality;
- admission-key lifetime.

Retention policy should separate:

- correctness minimum;
- active-history window;
- archival/audit policy.

### 26.7 Admission digest cost

Digesting a large inline task payload is unavoidable for semantic conflict detection. Applications with large payloads should use content-addressed payload references so the admission plan remains small.

Canonicalization must be streaming where possible.

### 26.8 Recovery time

Benchmark recovery with:

- millions of completed target runs;
- large active-wait populations;
- many retained unmatched signals;
- many admission keys;
- snapshots at different cut positions;
- an incomplete final compound frame;
- a large but valid compound event.

The target snapshot should materially reduce signal/wait index reconstruction time without becoming a second source of truth.

### 26.9 Backpressure and fairness

Critical ingress uses bounded queues. When saturated:

- embedded APIs return typed backpressure errors;
- daemon APIs return retryable status;
- callers retry with the same idempotency key;
- no request is reported as accepted before durability.

### 26.10 Benchmark acceptance targets

The proposal should not invent performance numbers before measurement. It should require no material regression in:

- ordinary task admission;
- ready-run selection;
- handler dispatch;
- target WAL replay;
- completion throughput when continuation features are unused.

Feature-off or no-active-wait overhead should remain minimal.

### 26.11 Developmental-profile overhead

The developmental execution profile adds no queue-domain record, WAL family, projection index, scheduler branch, or matching grammar. It reuses existing bounded causal fields and ordinary inspection indexes. The common execution path therefore has no campaign-specific processing cost.

Do not add per-campaign metric labels or mandatory indexes over application-owned context internals. Exact `origin_ref` lookup may use the existing bounded-reference index when explicitly enabled; otherwise trace and correlation lookup are sufficient.

---

## 27. Clean-break replacement and release strategy

`AQ-CONT-1` is a breaking API, lifecycle, and persistence contract. Implementation is replacement work, not live migration.

### 27.1 Freeze the experimental baseline

Before destructive changes:

```text
tag: actionqueue/pre-aq-cont-1
archive: selected WAL/snapshot fragments, coordinator fixtures,
         subscription fixtures, crash cases, and performance baselines
```

Classify baseline behavior as **retain**, **replace**, or **reject**. WAL authority and deterministic recovery are retained. Generic waiting through `Suspended`, transient custom events, and fire-and-forget child submission are rejected or replaced.

### 27.2 One target path

The target runtime contains only:

```text
AttemptDisposition
AdmissionPlan / ensure_task
Awaiting + WaitSpec
SignalEnvelope
ExecutorTraits
AQ-CONT-1 WAL and snapshot schemas
```

It contains no legacy handler adapter, old custom-event fallback, compatibility alias, dual response field, old-format reader, or live shadow admission path.

### 27.3 Construction order

1. Introduce target IDs, bounded references, and pure domain types.
2. Implement the new state machine and target persistence schema.
3. Implement compound admission and `AttemptDisposition` durability.
4. Implement durable signal admission, retained indexes, wait registration, and wake promotion.
5. Deliver checkpoint and resume context to local and remote executors.
6. Rebuild workflow, budget, actor, platform, daemon, and CLI surfaces on the target contracts.
7. Delete old dynamic submission, custom-event, actor-capability, and handler APIs.
8. Run the complete replay, crash, race, security, and performance suites.

No stage makes two paths authoritative. Incomplete work stays on development branches or compile-time scaffolding.

### 27.4 Offline differential testing only

Frozen fixtures may be evaluated by the old and new implementations to understand intentional behavior changes. The comparison harness:

- runs offline;
- cannot mutate a target store;
- cannot choose runtime authority;
- is removed when its diagnostic purpose is complete.

### 27.5 Pre-contract data

Pre-`AQ-CONT-1` WALs and snapshots are not production migration inputs. They remain in tags and fixtures. A one-off forensic converter may be written for a particular investigation, but it is not linked into the runtime or advertised as a supported upgrade path.

### 27.6 Conformance package

ActionQueue owns `conformance/aq-cont-1`. It includes reusable black-box drivers for:

- idempotent admission and digest conflict;
- atomic wait registration;
- early and duplicate signals;
- checkpoint/resume delivery;
- child admission and parent wait atomicity;
- crash replay at every semantic boundary;
- tenant and control-attribution isolation.

The package manifest records contract revision, fixture hashes, and minimum feature set. WorldInterface pins and passes an exact revision before claiming `WI-FABRIC-2` continuation semantics.

### 27.7 Release order

ActionQueue is published first as a breaking target release. WorldInterface then publishes against the exact target version and conformance revision. Exoskeleton follows only after both upstream packages pass in composition. Path and pinned Git dependencies are used during development; no transitional compatibility crate is published.

### 27.8 Future upgrade discipline

After the target release, supported ActionQueue versions must have explicit upgrade fixtures and source-preserving migration behavior. The clean break is not permission to repeat pre-production schema indiscipline; it defines the point from which stability begins.

## 28. Verification plan

### 28.1 Verification philosophy

Every new guarantee must be proven at three levels:

1. pure domain/property tests;
2. storage/replay tests;
3. end-to-end acceptance and kill-recovery tests.

A happy-path integration test is not sufficient for continuation correctness.

### 28.2 State-machine tests

Add exhaustive transition tests for:

```text
Running → Awaiting
Awaiting → Ready
Awaiting → Failed
Awaiting → Canceled
```

Reject all other transitions, including:

```text
Scheduled → Awaiting
Ready → Awaiting
Awaiting → Running
Awaiting → Completed
Completed → Awaiting
```

Property tests should verify terminal finality and monotonic transition rules over generated sequences.

### 28.3 Signal admission tests

- new signal is accepted;
- exact duplicate is idempotent;
- same ID with changed kind conflicts;
- same ID with changed payload hash conflicts;
- tenant-scoped identical IDs do not collide where allowed;
- oversized fields reject before WAL append;
- malformed namespace rejects;
- replay reconstructs sequence and digest;
- incomplete tail signal frame follows repair policy.

### 28.4 No-lost-wakeup matrix

Prove all orderings:

| Ordering | Expected result |
|---|---|
| Wait commits, then signal | One wake. |
| Signal commits, then wait | One wake. |
| Signal commits while handler executes, then wait commits | One wake. |
| Duplicate signal before wait | One wake. |
| Duplicate signal after wait | One wake. |
| Crash after signal append before matching | Recovery wakes. |
| Crash after wait append before matching | Recovery wakes if signal exists. |
| Crash after satisfaction append before dispatch | Recovery sees Ready with resume context. |
| Signal and timeout same tick | WAL order deterministically selects one. |

### 28.5 Wait matching tests

- exact tenant/namespace/kind/correlation match;
- source-ref match;
- wildcard omission behavior;
- cross-tenant rejection;
- stale signal excluded by cursor;
- lowest eligible signal sequence wins;
- one signal fans out to multiple waits;
- one wait resolves once;
- canceled wait never wakes;
- resolved wait cannot be resolved differently.

### 28.6 Checkpoint and resume tests

- inline checkpoint round-trip;
- external reference round-trip;
- hash preserved;
- resumed handler receives exact wait and signal IDs;
- process crash before attempt start preserves resume context;
- attempt start records context consumption;
- next retry after handler failure follows defined resume-context policy;
- locator redaction in default inspection;
- missing external checkpoint produces handler-visible failure, not projection corruption.

### 28.7 Attempt-disposition tests

- valid complete disposition;
- valid awaiting disposition;
- awaiting without wait rejected;
- complete with wait rejected;
- failure with child admissions rejected;
- stale lease fence rejected with no nested effects;
- invalid child causes entire disposition rejection;
- duplicate signal proposal conflicts and rejects entire disposition;
- budget consumption and state transition co-commit;
- suspended and awaiting attempts excluded from failure retry count;
- physical attempt lineage remains accurate.

### 28.8 Admission tests

- first `ensure_task` returns Created;
- retry returns AlreadyExists;
- changed payload under same key conflicts;
- changed dependency under same key conflicts;
- normalized dependency order yields same digest;
- unknown parent rejects with no task created;
- terminal parent rejects;
- cycle rejects;
- task, runs, dependencies, parentage, context, and key appear atomically after replay;
- incomplete compound frame creates none of them;
- large repeat derivation respects size limits;
- same admission key in different tenants follows configured namespace rule.

### 28.9 Dynamic coordinator tests

- parent disposition atomically admits child and enters awaiting;
- parent cannot await nonexistent child from the same disposition;
- duplicate coordinator attempt does not create duplicate child;
- parent cancellation cascades and cancels wait;
- child completes before parent wait matching pass and still wakes parent;
- child fails and parent receives terminal evidence;
- crash at every compound append/sync/apply boundary recovers one child and one wait.

### 28.10 Cross-instance outbox tests

Use two independent temporary ActionQueue data directories:

- local outbox retries after remote response loss;
- remote returns AlreadyExists;
- remote conflict is stable;
- completion bridge emits duplicate signal safely;
- signal arrives before local wait;
- both engines restart and continuation completes;
- no shared transaction or in-memory shortcut is used.

### 28.11 Tenant and security tests

- signal cannot cross tenant;
- admission key cannot resolve across tenant;
- actor cannot report result for another tenant;
- stale actor lease cannot commit child or signal;
- unauthorized daemon signal endpoint denies;
- control mutation records preserve the host-attested caller reference;
- payload and reference limits resist amplification;
- broad wait rejected under strict policy without correlation/cursor.

### 28.12 Clean-break and future-evolution tests

- a pre-`AQ-CONT-1` store fails with the precise unsupported-format error and is not modified;
- the target runtime contains no old handler, custom-event, fire-and-forget, or actor-capability path;
- archived baseline fixtures can be inspected by offline tools without becoming target replay requirements;
- snapshot-plus-tail and WAL-only target replay produce the same projection digest;
- a supported post-target format upgrade preserves source, validates destination, and produces an equivalent target projection;
- backup and restore succeed for every supported target release fixture.

### 28.13 Chaos tests

Extend kill recovery with deterministic kill points around:

```text
compound admission serialization
WAL header write
WAL payload write
CRC write
fsync
projection apply
signal index update
wait index update
wait satisfaction append
resume-context projection
snapshot temp write
snapshot rename
```

The assertion is always structural:

- the compound fact exists completely or not at all;
- no duplicate task, wait, or wake;
- no run stranded in `Running` without a valid lease;
- no `Ready` resumed run lacking wake context;
- no active wait attached to a terminal run.

### 28.14 Model checking or state exploration

The signal/wait/cancel/timeout race is small enough for exhaustive state exploration. A lightweight model or proptest state machine should enumerate command orderings and crashes.

Key state variables:

```text
run state
attempt active
lease active
wait status
signal status
resume context status
cancel status
timeout status
```

### 28.15 Downstream conformance tests

WorldInterface conformance:

- effect handler yields awaiting callback;
- callback signal resumes it;
- uncertain effect waits for reconciliation;
- no `Uncertain` ActionQueue state appears.

Exoskeleton conformance:

- cognitive task admits delegated work via outbox/remote ensure;
- cognitive run awaits completion signal;
- signal-before-wait race is safe;
- causal refs survive both engines;
- ActionQueue never parses commitment or effect payloads.

---


### 28.16 Governed-initiative and quiescence workload

- a Vessel-domain agenda review is scheduled durably;
- the queue restarts before the review time;
- exactly one review becomes eligible;
- an unrelated communication signal arriving first admits separate work without deleting or duplicating the review;
- the application may record continued quiescence and schedule a later review without adding a new ActionQueue lifecycle state;
- a stable review admission key prevents duplicate episodes after producer uncertainty.

### 28.17 Narrative and portable-identity workload

- a narrative-synthesis coordinator waits for evidence and resumes exactly once after the owning application stores it;
- identity rotation, attestation verification, or proof-publication callbacks arrive as opaque signal payload references;
- ActionQueue replay preserves identity/proof references byte-for-byte;
- no signature verification or principal-class inference occurs in ActionQueue;
- forged or unauthorized signal admission is rejected by the authenticated host before the signal enters the queue.

### 28.18 Developmental campaign execution conformance

Run `aq-cont-1-developmental-campaign-acceptance-matrix.yaml` with ordinary, replay, and crash variants. At minimum prove:

- campaign and arm references survive exactly as bounded opaque attribution;
- changing only those references cannot alter admission, priority, budget, routing, deadline, retention, or authority;
- a stable arm admission key deduplicates a lost response, while changed digest-bearing execution meaning conflicts;
- fan-out, waits, checkpoints, signals, cancellation, and recovery preserve structural lineage;
- physical retries are not labeled replications;
- no API, projection, metric, trace renderer, or inspector declares a winning arm, benchmark gain, binding constraint, saturation state, or candidate acceptance;
- no free-form experimental metadata map or developmental domain type enters queue core.

## 29. Risks and architectural decisions

### 29.1 Risk: turning signals into a message broker

Mitigation:

- signals are immutable fan-out observations;
- no consumer offsets or arbitrary stream processing in the first version;
- narrow structural matcher;
- tasks remain the unit of executable work;
- retention is bounded and purpose-specific.

### 29.2 Risk: state-machine complexity

Adding `Awaiting` increases every transition, query, snapshot, and UI surface.

Mitigation: the semantic distinction from `Suspended` is worth the cost. Avoid additional waiting states such as `PendingApproval`, `WaitingForChild`, or `Reconciling`; applications express those meanings through wait filters and causal refs.

### 29.3 Risk: compound WAL frames become too large

Mitigation:

- hard limits;
- bounded child batches;
- external data references;
- purpose-built compound events;
- staged fan-out.

### 29.4 Risk: purpose-built events duplicate mutation logic

Mitigation: implement reusable validation/apply helpers shared by target compound admission, attempt disposition, and other validated target commands. Generalize to transactions only after repeated pressure.

### 29.5 Risk: signal retention undermines storage simplicity

Mitigation:

- separate correctness minimum from audit archive;
- snapshot active indexes;
- content-addressed external payloads;
- explicit compaction policy and metrics.

### 29.6 Risk: idempotency horizon is too short

Mitigation: default admission records to task-history lifetime. Make destructive idempotency-record GC an explicit operator action or policy with warnings.

### 29.7 Risk: causal context becomes an ontology dump

Mitigation:

- fixed small fields;
- bounded opaque refs;
- no free-form map in core;
- ActionQueue never dereferences or interprets refs;
- downstream details remain in payload or external records.

### 29.8 Risk: routing terminology leaks back through downstream adapters

Mitigation:

- use `ExecutorTraits` exclusively in target code, storage, API, and docs;
- reject `ActorCapabilities` outside frozen baseline fixtures;
- keep queue-local RBAC capability terminology explicitly distinct from WorldInterface resource authority;
- add repository-wide naming tests and cross-stack conformance checks.

### 29.9 Risk: resume context is redelivered after handler crash

This is expected under the uncertainty clause. The same `RunId`, wait ID, signal ID, and checkpoint are delivered with a new `AttemptId`.

Handlers must be idempotent with respect to resumed external effects just as ordinary handlers are idempotent by `RunId`.

### 29.10 Risk: `Awaiting` holds a concurrency key forever

Mitigation: default release and explicit hold policy. Inspector highlights long-held keys.

### 29.11 Risk: authenticated ingress is treated as domain truth

Mitigation: documentation states authentication proves caller identity only. The embedding application decides whether the caller is permitted to assert the signal's meaning.

### 29.12 Risk: campaign attribution becomes a scheduler or evaluation backchannel

A downstream implementation may be tempted to parse `origin_ref`, arm names, or correlation IDs to prioritize protected benchmarks, select executors, infer replications, or report a winning candidate.

Mitigation:

- enforce `AQ-H19` and `AQ-H20` in public API documentation and conformance;
- keep campaign state behind one bounded application-owned reference;
- prohibit free-form queue metadata and campaign-specific scheduler branches;
- require all actual scheduling differences through ordinary explicit queue fields;
- expose only structural execution evidence;
- keep evaluation, saturation, and acceptance in Exoskeleton or the specialized application.

### 29.13 Required ADRs

Before implementation, record decisions for:

1. exact `Awaiting` transition table;
2. signal matching grammar and broad-filter validation;
3. signal retention and compaction defaults;
4. compound event versus general transaction mechanism;
5. admission digest canonicalization;
6. checkpoint inline limits and external-ref contract;
7. resume-context behavior after a resumed handler fails;
8. concurrency-key wait policy default;
9. task-context inheritance and override rules;
10. new crate extraction timing;
11. daemon ingress authentication hook;
12. target reactive-subscription grammar and queue-event bridging;
13. parent completion behavior with active children;
14. WAL and snapshot target versions.

### 29.14 Recommended decisions

This proposal recommends:

```text
Awaiting distinct from Suspended                 YES
One active continuation wait per run             YES
FirstMatch only in first release                 YES
Signals fan out; no global consumption           YES
Exact correlation preferred                      YES
Purpose-built compound events first              YES
General mutation transactions now                NO
Admission keys retained for task-history life    YES
Awaiting releases concurrency key by default     YES
External effect uncertainty in ActionQueue       NO
Causal refs interpreted by ActionQueue           NO
Executor traits grant authority                   NO
```

---

## 30. Definition of done

`AQ-CONT-1` is done when the new contract—not an adapter layer around the old one—is proven.

### Lifecycle and continuation

- [ ] `Awaiting` is distinct from `Suspended` and owns no lease.
- [ ] Checkpoint, wait registration, state transition, and lease release share one semantic commit boundary.
- [ ] Early, late, duplicate, and replayed signals produce one deterministic wake result.
- [ ] Resume context reaches local and remote handlers exactly as specified.
- [ ] Timeouts, cancellation, budgets, and concurrency keys have explicit race-tested semantics.

### Admission and attempts

- [ ] `ensure_task` returns `Created`, `AlreadyExists`, or digest conflict deterministically.
- [ ] Task, runs, dependencies, parentage, admission index, and causal context become visible atomically.
- [ ] A parent cannot enter `Awaiting` for a child that was not durably admitted.
- [ ] `AttemptDisposition` is the sole handler result contract.
- [ ] The fire-and-forget submission port is absent from target code and public docs.

### Signals and subscriptions

- [ ] Durable signals are deduplicated, tenant-scoped, replayable, and correlation-aware.
- [ ] Reactive subscriptions, continuation waits, and internal listeners are distinct.
- [ ] The transient custom-event path and any process-local correctness registry are absent.
- [ ] Queue lifecycle evidence may satisfy waits without redundant event duplication.

### Attribution and neutrality

- [ ] Causal and control contexts propagate as bounded opaque references.
- [ ] Attribution never authorizes an external effect.
- [ ] `ExecutorTraits` cannot be confused with resource capability.
- [ ] No Vessel, faculty, effect, receipt, conversation, initiative, narrative, EntityID, or bond type enters ActionQueue ontology.

### Persistence and operations

- [ ] The target has one new explicit WAL/snapshot lineage and deterministic replay.
- [ ] Pre-contract stores fail precisely and remain untouched.
- [ ] Backup, restore, projection digest, corruption, truncation, and compaction tests pass.
- [ ] Future target-release upgrade hooks and supported-version policy are documented and tested.
- [ ] CLI and API can inspect admission, waits, signals, resume context, causal lineage, and control attribution.

### Developmental campaign neutrality

- [ ] `AQ-H19` proves campaign, arm, task-distribution, evaluation-plan, and fingerprint references are opaque attribution only.
- [ ] `AQ-H20` proves queue completion, failure, timing, retry, and recovery facts are never exposed as task-quality, arm-preference, saturation, or candidate-acceptance judgments.
- [ ] No campaign, intervention, evaluation, benchmark, binding-constraint, or saturation type enters ActionQueue core, persistence, or scheduler ontology.
- [ ] No free-form experiment metadata map or campaign-specific scheduler branch exists.
- [ ] Authorized inspection exposes structural lineage and explicit queue-field differences without high-cardinality developmental metric labels.
- [ ] `aq-cont-1-developmental-campaign-acceptance-matrix.yaml` passes with replay and crash variants.

### Cross-stack readiness

- [ ] `conformance/aq-cont-1` is versioned, hashed, executable, and documented.
- [ ] WorldInterface passes the pinned conformance revision.
- [ ] Agenda review, communication ingress, delegated worker completion, and effect reconciliation workloads pass without ActionQueue learning their domain meaning.
- [ ] No live compatibility, dual-write, fallback, or shadow-authority path exists.

## 31. Implementation sequence

The PR sequence is intentionally breaking and target-first.

### PR 1 — Freeze evidence and establish target contract

- tag `pre-aq-cont-1`;
- archive selected fixtures and failure cases;
- add canonical contract and domain-leakage checks;
- commit the developmental campaign execution profile and acceptance matrix;
- prohibit developmental domain types and free-form experiment metadata in queue core;
- define target format and unsupported-pre-contract error.

### PR 2 — Core IDs, state machine, and terminology

- add admission/wait/signal/checkpoint/causal IDs;
- add `Awaiting` and transitions including `Leased → Canceled`;
- replace actor capability routing with `ExecutorTraits`.

### PR 3 — Target persistence skeleton

- new WAL/snapshot schemas;
- compound record framing and scratch validation;
- deterministic projection digest, backup, restore, and corruption tests.

### PR 4 — Idempotent compound admission

- `AdmissionPlan`, digest, index, `ensure_task`;
- task/run/dependency/parentage atomicity;
- conflict and replay tests.

### PR 5 — `AttemptDisposition`

- sole handler result model;
- compound attempt commit;
- failure-attempt accounting and lease fencing.

### PR 6 — Durable signals and waits

- signal admission/deduplication;
- wait registry, retained indexes, deadline handling;
- early-signal and no-lost-wakeup matrix.

### PR 7 — Checkpoints and resume context

- immutable data references;
- local and remote delivery;
- resume consumption and crash tests.

### PR 8 — Workflow and child coordination

- attempt-end transactional child admissions;
- child-terminal wait helpers;
- delete fire-and-forget dynamic submission.

### PR 9 — Budget, actor, platform, and control attribution

- move subscription ownership;
- budget/wait interaction;
- tenant/RBAC controls for signal and wait APIs;
- remote actor target protocol.

### PR 10 — Runtime, daemon, CLI, and inspection

- protected admission/signal/wait endpoints;
- trace, correlation, and exact-origin structural inspection with redaction;
- explicit display of ordinary scheduling-field differences and the non-interpretation boundary;
- backup, restore, and health surfaces;
- no old API or storage readers.

### PR 11 — Conformance and chaos package

- publish `conformance/aq-cont-1` inside the repository;
- fixture hashes and contract revision;
- full crash/race/retention/performance suite;
- execute the developmental campaign acceptance matrix with replay and crash variants;
- downstream integration guide.

### PR 12 — Breaking release

- remove development scaffolding and offline comparison harnesses;
- verify no forbidden legacy symbols or developmental semantic backchannels remain;
- publish the target release, exact conformance manifest, and additive developmental execution profile for WorldInterface and Exoskeleton.

## 32. Worked examples

### 32.1 Signal arrives before wait registration

A handler starts a remote operation through its application. The remote system completes immediately and a bridge admits:

```json
{
  "signal_id": "sig-7",
  "namespace": "remote-job",
  "kind": "terminal",
  "correlation_id": "job-91"
}
```

The signal is durable at sequence 700.

The handler then returns:

```rust
AttemptDisposition::awaiting(
    checkpoint_ref,
    WaitSpec::first_match(
        SignalFilter::new("remote-job", "terminal")
            .with_correlation("job-91"),
    ),
)
```

The compound disposition commits at sequence 704. The matcher finds sequence 700, commits wait satisfaction at 705, and returns the run to `Ready` with `sig-7` in its resume context.

No event was lost and no handler remained active.

### 32.2 Transactional child delegation

A coordinator attempt needs two subordinate tasks.

```rust
AttemptDisposition {
    outcome: AttemptOutcome::Awaiting,
    checkpoint: Some(progress_checkpoint),
    wait: Some(WaitSpec::for_task_set_terminal(batch_id)),
    child_admissions: vec![child_a, child_b],
    ..Default::default()
}
```

ActionQueue validates both children, the parent state, dependencies, tenant, hierarchy depth, wait, and lease. One compound event establishes:

```text
parent attempt yielded
child A admitted
child B admitted
parent-child edges established
wait W registered
parent → Awaiting
```

A crash before the frame completes creates none of those facts. A crash after the frame completes creates all of them.

### 32.3 Cross-engine outbox

A local application stores:

```text
outbox key: effect-intent/123
remote admission key: tool-engine/effect-intent/123
```

It calls remote `ensure_task`. The remote queue creates task `T9`, but the response is lost. The local dispatcher retries:

```text
ensure_task(tool-engine/effect-intent/123, same digest)
→ AlreadyExists(T9)
```

No duplicate task is created.

### 32.4 External effect reconciliation

A WorldInterface handler cannot confirm whether a remote operation committed. WorldInterface records its domain state as uncertain and returns an ActionQueue awaiting disposition:

```text
wait namespace: wi.reconciliation
kind: resolved
correlation: effect-intent/123
```

A separate reconciliation task later admits a signal. ActionQueue resumes the original run. ActionQueue never gains an `UncertainEffect` state and never interprets the receipt.

### 32.5 Exoskeleton delegated worker

Exoskeleton attaches opaque refs:

```text
principal_ref: vessel/alpha
purpose_ref: ref://exoskeleton/commitment/42
authorization_ref: delegation-grant/18
origin_ref: constitutional-decision/99
```

ActionQueue preserves those refs while admitting a worker-control task. When a completion bridge admits a correlated signal, the resumed task retains the same causal context.

ActionQueue does not know what a Vessel, commitment, or grant is.

### 32.6 Wait satisfied while budget is exhausted

A signal satisfies wait W. The run moves from `Awaiting` to `Ready` and receives a resume context. The budget gate prevents leasing.

Inspection says:

```text
state: Ready
resume context: pending, signal sig-10
blocked by: CostCents budget exhausted
```

After replenishment, the same pending resume context is delivered. The signal is not required to occur again.

### 32.7 Cancellation races with signal

Two commands reach the single authority lane:

```text
sequence 900: TaskCancel
sequence 901: WaitSatisfy
```

The cancellation applies first and cancels the wait. Satisfaction at 901 is rejected as already resolved/canceled. The signal remains durable.

If the order reverses, the run becomes `Ready` with resume context at 900 and cancellation at 901 transitions it to `Canceled`. The attempt never starts.

Both histories are deterministic and inspectable.

### 32.8 Stale remote result

Actor A's lease expires. Actor B leases the run. Actor A later submits an awaiting disposition that would create a child.

The command carries A's old lease fence. Storage rejects it before appending any child, wait, checkpoint, or signal.

### 32.9 Target handler continuation

```text
Handler attempt A1
  receives original payload and no resume context
  produces AttemptDisposition::Awaiting(checkpoint K, wait W)

Signal S satisfies W
  ActionQueue durably records WaitSatisfied and ResumeContext R

Handler attempt A2
  receives original payload + R + immutable checkpoint K
  produces Complete
```

There is no adapter from the old handler result model in the target runtime.

### 32.10 Quiescent Vessel agenda review

```text
Exoskeleton stores:
  QuiescenceRecord Q
  next_review_at T

Host admits:
  task agenda-review/Q/T
  admission key vessel/V/agenda-review/Q/T

At T:
  ActionQueue runs the opaque review handler.
  The application either admits one cognitive episode or records
  continued quiescence and a new review time.

ActionQueue does not know what quiescence or mission means.
```

### 32.11 Identity verification callback

```text
WorldInterface stores:
  IdentityBinding B
  KeyStateSnapshot K
  verification result V
  signal-outbox entry O

ActionQueue admits:
  namespace: identity
  kind: verification-complete
  correlation: B
  payload_ref: opaque reference to V

The waiting application resumes exactly once.
The queue does not verify the signature, infer personhood, or authorize an effect.
```

# Appendix A — Consolidated Rust sketch

The following is illustrative and intentionally omits derives, validation details, and error plumbing.

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

pub struct SignalEnvelope {
    pub signal_id: SignalId,
    pub tenant_id: Option<TenantId>,
    pub namespace: SignalNamespace,
    pub kind: SignalKind,
    pub correlation_id: Option<CorrelationId>,
    pub causation_id: Option<CausationId>,
    pub source_ref: Option<OpaqueRef>,
    pub payload: Option<DataRef>,
    pub payload_hash: Option<ContentHash>,
    pub occurred_at: Option<u64>,
    pub received_at: u64,
    pub control_context: Option<ControlMutationContext>,
}

pub struct WaitSpec {
    pub wait_id: WaitId,
    pub filter: SignalFilter,
    pub match_policy: WaitMatchPolicy,
    pub eligible_from: SignalEligibility,
    pub deadline_at: Option<u64>,
    pub timeout_policy: WaitTimeoutPolicy,
}

pub struct ResumeContext {
    pub wait_id: WaitId,
    pub checkpoint: Option<CheckpointRef>,
    pub wake: WakeReason,
    pub resumed_at: u64,
}

pub struct HandlerInput {
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub payload: Vec<u8>,
    pub metadata: AttemptMetadata,
    pub causal_context: CausalContext,
    pub resume_context: Option<ResumeContext>,
    pub cancellation_context: CancellationContext,
}

pub struct AttemptDisposition {
    pub outcome: AttemptOutcome,
    pub output: Option<DataRef>,
    pub checkpoint: Option<CheckpointRef>,
    pub wait: Option<WaitSpec>,
    pub child_admissions: Vec<EnsureTaskRequest>,
    pub emitted_signals: Vec<SignalEnvelope>,
    pub consumption: Vec<BudgetConsumption>,
}

pub trait ExecutorHandler: Send + Sync + 'static {
    fn execute(&self, input: HandlerInput) -> AttemptDisposition;
}
```

---

# Appendix B — State transition table

| From | To | Cause | Retry count impact |
|---|---|---|---|
| Scheduled | Ready | time/dependency/subscription eligibility | none |
| Scheduled | Canceled | control | none |
| Ready | Leased | dispatch | none |
| Ready | Canceled | control | none |
| Leased | Running | attempt start | physical attempt begins |
| Leased | Ready | lease expiry before attempt | none |
| Leased | Canceled | control | none |
| Running | Completed | complete disposition | none |
| Running | RetryWait | retryable failure/timeout with attempts left | failure count +1 |
| Running | Failed | terminal failure or retries exhausted | failure count as applicable |
| Running | Suspended | preemption | physical attempt only |
| Running | Awaiting | continuation disposition | physical attempt only |
| Running | Canceled | control/cooperative cancel | none |
| RetryWait | Ready | backoff elapsed | none |
| RetryWait | Failed | terminal policy | none |
| RetryWait | Canceled | control | none |
| Suspended | Ready | replenishment/explicit resume | none |
| Suspended | Canceled | control | none |
| Awaiting | Ready | wait satisfied or resume timeout | none |
| Awaiting | Failed | timeout policy | none |
| Awaiting | Canceled | control/timeout policy | none |

Terminal states remain final.

---

# Appendix C — Semantic record matrix

| Fact | Durable record | Projection/index |
|---|---|---|
| Task admitted | `AdmissionCommitted` | task, runs, dependency, parent, admission key |
| Attempt began | target attempt-start record | active attempt, lease fence |
| Attempt yielded | `AttemptDispositionCommitted` | attempt history, wait, checkpoint, state |
| Signal entered queue | `SignalAccepted` | signal ID/digest/sequence and match index |
| Wait resolved | `WaitSatisfied` | resolved wait, pending resume context, run Ready |
| Wait deadline fired | `WaitTimedOut` or compound resolution | timeout history and target state |
| Wait canceled | `WaitCanceled` or task-cancel compound effect | canceled wait |
| Resume consumed | attempt-start record references resume context | no longer pending; retained in history |
| Control action | relevant event carries `ControlMutationContext` | control history/index |
| Routing declaration | actor registration with `ExecutorTraits` | actor routing registry |

---

# Appendix D — Current source disposition

| Current surface | Proposed disposition |
|---|---|
| `actionqueue-core/src/run/state.rs` | Add `Awaiting`. |
| `actionqueue-core/src/run/transitions.rs` | Add explicit continuation transitions. |
| `actionqueue-core/src/mutation/mod.rs` | Add compound admission/disposition/signal/wait commands; preserve single authority. |
| `actionqueue-core/src/subscription.rs` | Replace old filters with target reactive-subscription, wait, and signal domain types. |
| `actionqueue-core/src/event.rs` | Clarify internal-event semantics; optionally bridge durable queue events to wake evidence. |
| `actionqueue-core/src/task/metadata.rs` | Keep organizational metadata; do not overload it with causal context. |
| `actionqueue-core/src/task/constraints.rs` | Rename routing field; add wait concurrency-key policy. |
| `actionqueue-core/src/actor.rs` | Rename `ActorCapabilities` to `ExecutorTraits`. |
| `actionqueue-executor-local/src/handler.rs` | Replace handler input/output with target resume-aware input and `AttemptDisposition`. |
| `actionqueue-workflow/src/submission.rs` | Delete fire-and-forget submission; use compound admission and application outboxes. |
| `actionqueue-runtime/src/dispatch.rs` | Integrate admission planner, compound commits, signal ingress, matching, and recovery reconciliation. |
| `actionqueue-budget` subscription registry | Move to generic continuation subsystem. |
| `actionqueue-storage/src/wal/*` | Add versioned compound records and signal/wait events. |
| `actionqueue-storage/src/recovery/*` | Rebuild admission, wait, signal, resume, and causal indexes. |
| `actionqueue-storage/src/snapshot/*` | Implement the fresh target schema and future-version upgrade hooks. |
| `actionqueue-daemon` | Add protected v2 mutation/query endpoints. |
| `actionqueue-cli` | Add ensure, signal, wait, trace, inspect, backup, and restore commands. |
| Existing acceptance/chaos scenarios | Re-express as target fixtures and extend with continuation/admission matrices. |

---

# Appendix E — Contract with companion systems

## Exoskeleton

Exoskeleton may supply:

```text
opaque task payloads
admission keys
purpose/principal/authorization refs
delegation or commitment correlation IDs
completion signals
developmental campaign, arm, evaluation-plan, task-distribution, and component-fingerprint attribution through existing bounded causal references
```

ActionQueue returns durable scheduling, continuation, and lineage. It does not interpret the references, infer a binding constraint, score an intervention arm, or determine whether a delegated result or candidate should be accepted.

## WorldInterface

WorldInterface may supply:

```text
effect-execution task payloads
intent-derived admission keys
callback/reconciliation signals
agency and authorization refs
```

ActionQueue returns durable execution and waiting. It does not create receipts, classify idempotency, resolve canonical resources, or represent effect uncertainty.

## NetCorp or another platform

A platform may supply authenticated tenant, principal, and control context. ActionQueue enforces tenant separation and queue-local RBAC where enabled. It does not become the organization's identity or policy authority.

---

# Appendix F — Reviewed baseline anchors

The proposal was derived from the following pinned source surfaces at commit `97c9dc26c19c697dbfb204ed503e82c5f053394f`:

| Surface | Baseline observation |
|---|---|
| [`README.md`](https://github.com/zed-colonel/actionqueue/blob/97c9dc26c19c697dbfb204ed503e82c5f053394f/README.md) | Declares the opaque-action charter, canonical states, WAL v5, snapshots v8, workflow, budget, actor, platform, and acceptance-test surfaces. |
| [`docs/actionqueue-charter.md`](https://github.com/zed-colonel/actionqueue/blob/97c9dc26c19c697dbfb204ed503e82c5f053394f/docs/actionqueue-charter.md) | Establishes durable intent, mutation invariants, external-effect reality clause, embedded/daemon parity, and the “does not care what an action means” boundary. |
| [`docs/invariant-boundaries-v1.0.md`](https://github.com/zed-colonel/actionqueue/blob/97c9dc26c19c697dbfb204ed503e82c5f053394f/docs/invariant-boundaries-v1.0.md) | Makes WAL authoritative and limits mutation to validated construction, transition, append, and replay lanes. |
| [`crates/actionqueue-core/src/run/state.rs`](https://github.com/zed-colonel/actionqueue/blob/97c9dc26c19c697dbfb204ed503e82c5f053394f/crates/actionqueue-core/src/run/state.rs) | Defines the current lifecycle through `Suspended` but no durable continuation state. |
| [`crates/actionqueue-core/src/run/transitions.rs`](https://github.com/zed-colonel/actionqueue/blob/97c9dc26c19c697dbfb204ed503e82c5f053394f/crates/actionqueue-core/src/run/transitions.rs) | Encodes the canonical transition table and terminal finality. |
| [`crates/actionqueue-core/src/mutation/mod.rs`](https://github.com/zed-colonel/actionqueue/blob/97c9dc26c19c697dbfb204ed503e82c5f053394f/crates/actionqueue-core/src/mutation/mod.rs) | Defines one-command mutation authority, attempt outcomes, suspension, subscriptions, actors, and platform commands. |
| [`crates/actionqueue-executor-local/src/handler.rs`](https://github.com/zed-colonel/actionqueue/blob/97c9dc26c19c697dbfb204ed503e82c5f053394f/crates/actionqueue-executor-local/src/handler.rs) | Defines current handler input/output, optional suspended output, and the fire-and-forget submission port contract. |
| [`crates/actionqueue-workflow/src/submission.rs`](https://github.com/zed-colonel/actionqueue/blob/97c9dc26c19c697dbfb204ed503e82c5f053394f/crates/actionqueue-workflow/src/submission.rs) | Explicitly documents closed-channel loss, no handler error path, and next-tick processing for dynamic submissions. |
| [`crates/actionqueue-runtime/src/dispatch.rs`](https://github.com/zed-colonel/actionqueue/blob/97c9dc26c19c697dbfb204ed503e82c5f053394f/crates/actionqueue-runtime/src/dispatch.rs) | Shows exclusive mutation authority, sequential dynamic task/run/dependency admission, attempt-result processing, and subscription reconstruction. |
| [`crates/actionqueue-core/src/subscription.rs`](https://github.com/zed-colonel/actionqueue/blob/97c9dc26c19c697dbfb204ed503e82c5f053394f/crates/actionqueue-core/src/subscription.rs) | Defines one-shot filters over task completion, run state, budget threshold, and custom string keys. |
| [`crates/actionqueue-core/src/event.rs`](https://github.com/zed-colonel/actionqueue/blob/97c9dc26c19c697dbfb204ed503e82c5f053394f/crates/actionqueue-core/src/event.rs) | Defines dispatch-tick events and structural filter matching. |
| [`crates/actionqueue-core/src/actor.rs`](https://github.com/zed-colonel/actionqueue/blob/97c9dc26c19c697dbfb204ed503e82c5f053394f/crates/actionqueue-core/src/actor.rs) | Defines free-form actor “capabilities” used for routing eligibility. |
| [`docs/wal-recovery-guide.md`](https://github.com/zed-colonel/actionqueue/blob/97c9dc26c19c697dbfb204ed503e82c5f053394f/docs/wal-recovery-guide.md) | Documents WAL v5 framing, typed event families, CRC behavior, and tail repair. |
| [`docs/examples/idempotency-runid.md`](https://github.com/zed-colonel/actionqueue/blob/97c9dc26c19c697dbfb204ed503e82c5f053394f/docs/examples/idempotency-runid.md) | Correctly assigns external-effect idempotency to `RunId` and lineage to `AttemptId`. |
| [`Cargo.toml`](https://github.com/zed-colonel/actionqueue/blob/97c9dc26c19c697dbfb204ed503e82c5f053394f/Cargo.toml) | Defines the eleven-crate workspace, feature combinations, and named acceptance/chaos tests. |

---

## Closing statement

ActionQueue's original design remains the correct foundation:

> It turns intent into durable, inspectable, scheduled action without caring what the action means.

The next hardening step should make that statement true for work that spans asynchronous boundaries:

> **A durable run may yield without disappearing, wait without holding execution, resume without losing causality, and delegate without guessing whether the delegated work exists.**

That is the generic substrate Exoskeleton, WorldInterface, NetCorp, and ordinary workflow applications can safely build upon.
