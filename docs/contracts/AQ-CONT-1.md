# AQ-CONT-1 — ActionQueue Development Contract

**Status:** Frozen development contract (established by PR `AQ-01`)
**Contract:** `AQ-CONT-1`
**Planning profile:** `AQ-PLAN-2`
**Coordinated stack revision:** `STACK-2026-07-20-CLEAN-1`
**Developmental diagnostics profile:** `STACK-DEVELOPMENTAL-DIAGNOSTICS-1`
**Pinned source baseline:** `97c9dc26c19c697dbfb204ed503e82c5f053394f` (`main`, June 9, 2026)
**Baseline tag:** `actionqueue/pre-aq-cont-1`
**Integration branch:** `aq-cont-1`
**Architecture date:** August 10, 2026

This document is the repository-level index for the `AQ-CONT-1` contract. It states what
is frozen, what will be removed, and which conformance revision future pull requests
implement. The normative text lives in the documents listed below; this file only links,
pins, and summarizes them.

## 1. Normative documents

All other files in this directory are verbatim copies of the planning package. Their hashes are
recorded in [`SHA256SUMS`](SHA256SUMS) and verified by the repository conformance tests.
Editing a frozen document requires a contract amendment, a new package revision, and an
updated hash.

| Document | Role | SHA-256 |
|---|---|---|
| [`actionqueue-hardening-implementation-ready.md`](actionqueue-hardening-implementation-ready.md) | Normative architecture: invariants `AQ-H1`–`AQ-H20`, target vocabulary, lifecycle, persistence, verification plan | `1b80afa27e57c0ebe95eb456dcb490357187df23aa9b144970a69df1ba8f4cb0` |
| [`aq-cont-1-developmental-campaign-execution-profile.md`](aq-cont-1-developmental-campaign-execution-profile.md) | Additive profile: neutral campaign attribution and the execution/evaluation boundary | `2a7a4ecc6dd4b30551e7fcb8d72a863333564c7fdba191d899db4a5bf30d3499` |
| [`constitutional-stack-implementation-contracts.yaml`](constitutional-stack-implementation-contracts.yaml) | Cross-stack ownership, handoff rule, removed paths, conformance package ownership | `842d5ae8bfbd4b8c3767f7b589267ab26de1b40a2bf722b0ab1d182768d48922` |
| [`developmental-diagnostics-cross-stack-profile.yaml`](developmental-diagnostics-cross-stack-profile.yaml) | Shared Exoskeleton/WorldInterface/ActionQueue developmental terminology and ownership | `5214651327d1aa0c688eb20a404de5a5f692387b193ee747ac79d3e4e1380f2d` |

Supporting planning documents (implementation plan, work breakdown, package README, and
validation results) are committed under [`docs/planning/aq-cont-1/`](../planning/aq-cont-1/).
The architectural decision queue is under [`docs/adrs/`](../adrs/README.md). The conformance
package is under [`conformance/aq-cont-1/`](../../conformance/aq-cont-1/manifest.yaml).

## 2. What ActionQueue owns and must not own

From `constitutional-stack-implementation-contracts.yaml`:

**Owns:** task/run/attempt lifecycle, compound admission, `Awaiting`, durable signals,
checkpoints, opaque causal references, WAL replay and recovery.

**Must not own:** Vessel semantics, external identity resolution, effect or receipt meaning,
authorization over external resources, narrative or relationship meaning.

The developmental profile adds, without changing the protocol: ActionQueue owns durable
opaque campaign work, trace/correlation/causation preservation, waits, deadlines, retries,
fan-out, backpressure, checkpoints, recovery, and execution facts without interpretation of
experiment meaning. It does not own task-distribution or metric semantics, capability or
representation profiles, component fingerprints, binding-constraint assessment,
evaluation-plan freezes, performance-vector interpretation, or campaign admission, stop,
acceptance, or continuity policy.

## 3. Hardening invariants

| ID | Invariant |
|---|---|
| `AQ-H1` | Opaque meaning |
| `AQ-H2` | WAL authority |
| `AQ-H3` | No lost wakeups |
| `AQ-H4` | Wait establishment is atomic |
| `AQ-H5` | Wake-up is single and replay-stable |
| `AQ-H6` | Admission is knowable |
| `AQ-H7` | Idempotent admission detects semantic conflict |
| `AQ-H8` | Parent wait implies durable child |
| `AQ-H9` | Attempt disposition is complete |
| `AQ-H10` | Causal context is immutable attribution |
| `AQ-H11` | Attribution is not authorization |
| `AQ-H12` | Routing traits are not capabilities |
| `AQ-H13` | Awaiting is not failure |
| `AQ-H14` | External uncertainty remains external |
| `AQ-H15` | Control operations are attributable |
| `AQ-H16` | Version evolution is explicit |
| `AQ-H17` | External identity and proof references remain opaque |
| `AQ-H18` | Agenda review is scheduled work; quiescence is not a queue lifecycle state |
| `AQ-H19` | Developmental correlation is neutral attribution |
| `AQ-H20` | Execution outcome is not performance judgment |

Full text: architecture document, Section 5.

## 4. Clean-break posture

- Existing source, fixtures, and persisted stores are **evidence, not a compatibility
  obligation**. They are preserved by the baseline tag and the archive under
  [`archive/pre-aq-cont-1/`](../../archive/pre-aq-cont-1/README.md).
- The target runtime contains **one authority path**. No feature flag selects old versus new
  semantics; no pre-`AQ-CONT-1` store reader, legacy handler adapter, or live dual authority
  exists in the released target.
- WAL v5 and snapshot schema v8 are **not migrated**. Target stores carry an explicit manifest
  (`AQ-ADR-001`) and pre-contract stores are rejected without modification.
- Offline differential testing against archived fixtures is permitted; target crates must not
  depend on, read, or upgrade archived files.

## 5. Frozen, removed, and rejected inventory

### 5.1 Retain and re-prove

Run-policy accounting; terminal-state finality; retry caps; lease fencing and expiry;
concurrency-key enforcement; DAG cycle rejection and dependency gating; intentional
parent-child cancellation and completion rules; WAL-first mutation authority; deterministic
replay; snapshot-as-acceleration; budget consumption and preemptive suspension; actor
heartbeat and tenant isolation where semantics remain valid.

### 5.2 Forbidden legacy symbols (staged removal)

These symbols exist in the baseline. Each is reported by the boundary check today and becomes
a hard failure once its removal PR lands (see
[`conformance/aq-cont-1/contract-boundaries.json`](../../conformance/aq-cont-1/contract-boundaries.json)).
The scan covers git-tracked files outside `archive/`, `docs/contracts/`, `docs/planning/`, and
`docs/adrs/`, and its per-symbol file-count ratchet has no headroom: a new tracked file that
names a legacy symbol is a conscious policy bump in the same pull request.

| Symbol | Replaced by | Removal PR |
|---|---|---|
| `ActorCapabilities` | `ExecutorTraits` | `AQ-02` |
| `required_capabilities` | `required_executor_traits` | `AQ-02` |
| `with_capabilities` | executor-trait constructors | `AQ-02` |
| `HandlerOutput` | `AttemptDisposition` | `AQ-08` |
| `TaskSubmissionPort` | compound child admission | `AQ-09` |
| `SubmissionChannel` | compound child admission | `AQ-09` |
| `EventFilter::Custom` | durable `SignalEnvelope` / `WaitSpec` | `AQ-10` |
| `ActionQueueEvent::CustomEvent` | durable `SignalEnvelope` / `WaitSpec` | `AQ-10` |

Concept-level removals that have no single symbol (WAL v5 reader, snapshot schema v8 reader,
legacy handler adapter, compatibility admission path) are verified by the `AQ-03`, `AQ-08`,
and `AQ-14` gates.

### 5.3 Rejected as target behavior

Silent child-submission loss; external signal loss across restart; waiting while holding an
execution lease; caller metadata treated as authorization; blind retry in response to
application-level uncertain external effects; automatic opening or mutation of old stores;
live dual authority or fallback to a pre-contract path.

## 6. Domain-neutrality and developmental-neutrality rules

### 6.1 Forbidden domain ownership in target code

The following names must not appear as identifiers in target crate code
(`crates/**`). They may appear in explanatory documentation and in archived evidence.

```text
Vessel  Faculty  Commitment  DelegatedAgent  WorkOrder  EffectIntent  Receipt
Constitution  ApprovalMeaning  Relationship  Conversation  AttentionDemand
ResponseObligation  Belief  Narrative  EntityID  AuthorizationEnvelope
```

### 6.2 Forbidden queue-owned developmental ontology (`AQ-H19`, `AQ-H20`)

No target crate may define or expose campaign, intervention-arm, baseline/candidate,
benchmark, benchmark-score, evaluation-plan, task-distribution, component-fingerprint,
binding-constraint, saturation, replication/sample, winning-arm, or activation-recommendation
types, fields, metric labels, or API outputs. Campaign and arm lineage travels only through
the existing `trace_id`, `correlation_id`, `CausationLink`, and one bounded `origin_ref`.

### 6.3 No free-form metadata backchannel

`CausalContext` stays fixed and bounded. No public type in a target crate may carry a
string-keyed free-form map (`HashMap<String, String>`, `BTreeMap<String, String>`, JSON
`Value`, or equivalent) as an experiment, campaign, scheduler-hint, or extension channel.
Rich campaign state lives in an application-owned immutable record behind one `origin_ref`.

### 6.4 No campaign scheduler or authority backchannel

Scheduling (`actionqueue-engine`), budget (`actionqueue-budget`), and platform authority
(`actionqueue-platform`) code must not read, parse, or branch on `origin_ref`,
`correlation_id`, or campaign/arm identifiers. Any intentional scheduling difference between
arms is expressed through the ordinary explicit queue field that already owns that behavior.

### 6.5 Execution is not evaluation

Queue terminal states, timings, retries, and recovery facts are execution evidence. No API,
projection, metric, or inspector field declares task correctness, arm preference, gain,
binding constraint, saturation, candidate acceptance, or activation eligibility.

## 7. Downstream freeze

WorldInterface and Exoskeleton remain pinned to the pre-`AQ-CONT-1` ActionQueue baseline
until a target release publishes the `AQ-CONT-1` conformance manifest. Development adapters
may depend on exact Git revisions of the `aq-cont-1` integration branch only after the
relevant interface gate (`AQ-04` for admission prototyping, `AQ-06` for continuation
prototyping) is stable, and may not claim `WI-FABRIC-2` or `EXO-V3` conformance until `AQ-13`
and `AQ-14` complete. Exoskeleton may rely on the developmental profile only after the
`AQ-DD-*` matrix is published with a pinned conformance revision.

## 8. Repository checks

| Check | Location | Gate |
|---|---|---|
| Contract, planning, archive, and matrix hash verification | `tests/conformance/frozen_evidence.rs` | every `cargo test --workspace` |
| Domain-leakage, developmental-neutrality, metadata, backchannel, archive-isolation, and staged legacy-symbol checks | `tests/conformance/contract_boundaries.rs` driven by `conformance/aq-cont-1/contract-boundaries.json` | every `cargo test --workspace`; CI reports revisions via `cargo aq-conformance` |
| Pre-contract store rejection scaffold | `tests/conformance/pre_contract_store.rs` | activated by `AQ-03` |

## 9. Revision policy

Additive conformance coverage increments the package revision recorded in
`conformance/aq-cont-1/manifest.yaml`. A normative semantic change requires an explicit
contract amendment, a new architecture document hash, and an ADR. The developmental profile
is additive: it changes no protocol primitive, persistence family, scheduler branch, or
authority path.
