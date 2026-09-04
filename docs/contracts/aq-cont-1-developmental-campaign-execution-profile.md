# AQ-CONT-1 Developmental Campaign Execution Profile

**Status:** Additive planning and conformance clarification  
**Base contract:** `AQ-CONT-1` / `STACK-2026-07-20-CLEAN-1`  
**Cross-stack profile:** `STACK-DEVELOPMENTAL-DIAGNOSTICS-1`  
**Date:** August 10, 2026  
**Normative lower-layer contract change:** No

## 1. Purpose

The OHIO-loop material contributes a useful developmental lesson to the Exoskeleton stack: improvement work should be directed by an evidence-backed diagnosis of the currently binding constraint rather than by repeatedly changing whichever component is easiest to modify.

That lesson does **not** make ActionQueue an auto-research engine, an experiment manager, a statistical evaluator, or a domain ontology. ActionQueue remains a durable executor of opaque work. This profile only specifies how a higher layer may execute developmental campaigns through the existing `AQ-CONT-1` contract without creating hidden scheduler semantics or semantic ownership in the queue.

The profile is intended to support both:

- specialized domain systems, such as a legacy accounting or ERP development harness; and
- the more general Exoskeleton thesis of persistent, embodied, constitutionally governed AI.

The specialized system owns its business ontology, workloads, evaluators, and intervention policy. Exoskeleton owns general developmental diagnosis and acceptance. ActionQueue owns neither.

## 2. Architectural boundary

ActionQueue may durably execute work that a caller describes as:

```text
campaign
intervention arm
baseline
candidate
probe
trial
evaluation run
replication
reconciliation
```

Those words are application meaning. They do not enter ActionQueue's public ontology.

ActionQueue continues to own only:

```text
admission
opaque task and run identity
attempts
waiting and wake-up
signals
checkpoints and resume context
causal attribution
explicit scheduling constraints
retries, deadlines, cancellation, and recovery
```

A campaign label must not affect admission, authorization, priority, budget, executor eligibility, deadline handling, retention, retry behavior, or terminal-state semantics unless the caller uses the ordinary explicit field that already owns that behavior.

## 3. Existing seams only

No developmental primitive is added to `actionqueue-core`, the WAL, snapshots, the daemon API, or the scheduler.

The profile uses existing `AQ-CONT-1` seams:

| Existing seam | Developmental use | Queue interpretation |
|---|---|---|
| `trace_id` | Preserve one structural execution lineage. | Structural trace identity only. |
| `correlation_id` | Group a caller-defined arm, paired block, or campaign execution set. | Equality and lookup only. |
| `CausationLink` | Preserve parent task/run/attempt or external causal origin. | Structural lineage only. |
| `origin_ref` | Point to one immutable application-owned campaign execution context. | Bounded opaque reference only. |
| `AdmissionKey` | Make one caller-defined execution unit idempotent. | Admission identity and digest conflict only. |
| ordinary priority, budgets, constraints, and executor traits | Express actual execution policy. | Their existing explicit semantics. |
| waits, signals, checkpoints, and resume context | Sustain long-running arms across asynchronous boundaries and restart. | Existing continuation semantics. |

The application-owned record referenced by `origin_ref` may conceptually contain:

```text
campaign reference
intervention-arm reference
evaluation-plan reference
task-distribution reference
component-fingerprint reference
replication or paired-block reference
```

That record is not an ActionQueue type. ActionQueue does not dereference it, validate it, index its internal fields, or copy it into free-form metadata.

## 4. Neutral attribution rules

### 4.1 Correlation is not scheduling

Campaign and arm correlation is attribution only.

The following are forbidden:

- assigning elevated priority because `origin_ref` names a research or self-improvement campaign;
- selecting an executor by parsing an arm name;
- widening a budget because a correlation ID matches a protected benchmark;
- bypassing tenant, admission, or control checks for experimental work;
- treating a campaign reference as authorization;
- treating a baseline or candidate label as a queue lifecycle state.

Any intentional scheduling difference between arms must be encoded through the ordinary explicit scheduling contract and preserved by the higher layer as an intended intervention or a confound.

### 4.2 No semantic metadata backchannel

`CausalContext` remains fixed and bounded. A free-form experiment metadata map is not introduced.

If the higher layer needs rich campaign state, it stores that state in its own authoritative domain record and supplies one bounded opaque reference. This preserves replay stability, privacy, and ActionQueue's domain neutrality.

### 4.3 Retries are not samples

An `AttemptId` is a physical execution attempt. A retry after lease loss, crash, or a retryable handler failure is not automatically a new experimental sample, replication, or evaluation episode.

The higher layer decides whether recovered or repeated execution is:

- the same logical sample;
- an excluded infrastructure failure;
- a valid replication;
- or a new evaluation unit requiring a new `AdmissionKey`.

ActionQueue reports the exact lineage needed for that decision but does not make it.

## 5. Durable campaign execution

### 5.1 Idempotent arm admission

A higher layer should derive stable admission keys from its own immutable execution-unit identity. A lost response followed by `AlreadyExists` remains one queue task, not a duplicate arm.

A changed payload, task constraint, dependency, causal context, or other digest-bearing field under the same key produces the ordinary admission conflict. A campaign implementation must not use a stable key to conceal a materially changed intervention.

### 5.2 Fan-out and paired work

ActionQueue may execute fan-out for multiple arms, workload shards, or replications through ordinary child admission and DAG semantics. It does not infer pairing, randomization, stratification, blinding, or statistical dependence.

The higher layer freezes and records those meanings before admission. ActionQueue preserves structural parentage and execution order as evidence.

### 5.3 Long-running and externally mediated arms

An arm may enter `Awaiting` while an external build, tool call, provider callback, human review, or reconciliation proceeds. Durable signals, checkpoints, and resume context preserve the work across restart.

A queue wait is not an Exoskeleton uncertainty judgment and is not a WorldInterface `Uncertain` effect state. The owning layer preserves those distinctions.

### 5.4 Cancellation and bounded campaigns

A developmental campaign may stop because its budget, deadline, no-progress window, or higher-priority obligations require it. ActionQueue executes explicit cancellation, deadlines, and budget gates. It does not infer saturation or decide that a campaign should switch intervention class.

## 6. Completion boundary

ActionQueue terminal states describe queue execution only.

```text
Completed  = the handler's accepted disposition completed the queue run
Failed     = queue execution reached the declared failure condition
Canceled   = an authorized queue control path canceled the work
```

They do not mean:

```text
the task answer is correct
the external effect succeeded semantically
the candidate is safe or better
the intervention arm won
a binding constraint was identified
the current frontier is saturated
the candidate may be activated
```

WorldInterface supplies accountable boundary evidence. Exoskeleton or the specialized application verifies outputs, interprets the performance vector, and makes acceptance decisions.

## 7. Recovery and evidentiary integrity

For developmental workloads, existing `AQ-CONT-1` recovery guarantees have additional evidentiary value:

- idempotent admission prevents response-loss duplication;
- immutable causal context prevents an arm from silently changing attribution;
- WAL replay preserves task/run/attempt/wait/signal lineage;
- lease fencing rejects stale executor commits;
- checkpoint and resume context distinguish resumed work from fresh execution;
- atomic child admission prevents partial fan-out;
- deterministic signal/wait resolution prevents duplicate continuation;
- control mutation attribution records who canceled, resumed, or reprioritized work.

These facts make higher-layer comparisons more trustworthy. They still do not make ActionQueue an evaluator.

## 8. Inspection and observability

Inspection should permit an authorized operator to answer:

```text
Which queue work shares this correlation ID?
Which immutable origin reference was attached at admission?
Which parent attempt caused this child?
Was this a duplicate admission or a new task?
How many physical attempts occurred, and why?
Which wait, signal, checkpoint, deadline, or cancellation affected execution?
Did recovery change the structural execution path?
Were explicit priority, budget, constraints, or executor traits different?
```

Inspection must not display a queue-computed arm winner, benchmark score, binding constraint, saturation status, or activation recommendation.

Campaign and arm identifiers are high-cardinality and may be sensitive. They belong in authorized inspection and traces, not metric labels. Aggregate queue metrics remain domain-neutral.

## 9. Security and abuse considerations

| Risk | Required control |
|---|---|
| Campaign metadata becomes an authority token. | `origin_ref` and `correlation_id` remain opaque; host authorization and ordinary control checks remain mandatory. |
| Hidden scheduler directives are placed in a reference. | ActionQueue never dereferences the reference; scheduling changes require explicit queue fields. |
| Candidate code floods the queue with arms. | Ordinary admission limits, quotas, budgets, backpressure, tenant policy, and cancellation apply. |
| Retried attempts are counted as independent wins. | Preserve physical attempt lineage; higher-layer evaluation policy classifies samples. |
| Sensitive benchmark or ERP data leaks into telemetry. | Use bounded references and hashes; redact opaque refs by default; prohibit IDs as metric labels. |
| Queue completion is used as candidate acceptance. | Enforce `AQ-H20` in documentation, APIs, examples, and conformance tests. |

## 10. Allocation across the existing fourteen-PR plan

No additional pull request or crate is required.

| PR | Additive developmental-profile work |
|---|---|
| `AQ-01` | Commit this profile and its acceptance matrix; add boundary checks that forbid experiment-domain types and free-form metadata in queue core. |
| `AQ-02` | Document and test neutral `CausalContext` semantics; no new campaign type or scheduler interpretation. |
| `AQ-04` | Add conformance fixtures proving stable arm admission is idempotent and changed execution meaning conflicts. |
| `AQ-06`–`AQ-09` | Reuse ordinary waits, checkpoints, resume context, and atomic fan-out; prove recovery preserves arm lineage without interpreting it. |
| `AQ-11` | Prove campaign references do not change tenant, routing, or control authority. |
| `AQ-12` | Add authorized structural filtering by trace/correlation/origin reference and explicit scheduling-difference inspection. |
| `AQ-13` | Run `aq-cont-1-developmental-campaign-acceptance-matrix.yaml` with replay and crash variants. |
| `AQ-14` | Publish the profile as a downstream conformance clarification and state that no protocol primitive or authority path changed. |

The critical path remains fourteen PRs.

## 11. Acceptance statement

This profile is satisfied when:

- the existing `AQ-CONT-1` causal and continuation seams can durably carry campaign work;
- campaign and arm references are provably neutral to scheduling and authority;
- no new developmental ontology or free-form metadata enters ActionQueue;
- queue retries, recovery, fan-out, waits, and cancellation remain reconstructible;
- queue terminal states are never presented as task-quality or intervention judgments;
- the developmental campaign acceptance matrix passes alongside the ordinary `AQ-CONT-1` conformance package.

The result is not an OHIO-specific queue. It is the same general durable execution substrate, with a clearer contract for evidence-bearing developmental work.
