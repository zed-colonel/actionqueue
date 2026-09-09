# Acceptance Test Classification for AQ-CONT-1

Companion to [`acceptance-test-taxonomy.md`](../../acceptance-test-taxonomy.md). Every baseline
acceptance and chaos test is classified per Section 1.3 of the
[implementation plan](aq-cont-1-implementation-plan.md):

- **Retain** — the behaviour survives `AQ-CONT-1`; the test is re-proved against target
  types (it may be rewritten, but its invariant must still hold).
- **Replace** — the behaviour is superseded; the test is rewritten to prove the target
  seam and the old expectation is deleted in the named PR.
- **Reject** — the expectation contradicts the target contract; the test is deleted in the
  named PR and its scenario becomes a negative test.

This file lives under a root the legacy-symbol scan exempts (`legacy_scan_exempt_roots` in the
boundary policy) so it may keep naming legacy symbols after their removal PRs land. Baseline sources are frozen at tag `actionqueue/pre-aq-cont-1`; the frozen pass/fail record is
in [`archive/pre-aq-cont-1/characterization-results/`](../../../archive/pre-aq-cont-1/characterization-results/summary.json).

## Core contract tests (no features)

| Test | Classification | Reason | Re-proved / removed in |
|---|---|---|---|
| `once_accounting` | Retain | Run-policy accounting is a retained invariant | `AQ-04` |
| `repeat_accounting` | Retain | Run-policy accounting | `AQ-04` |
| `retry_cap` | Retain (accounting changes) | Retry cap remains; physical and failure attempts split per `AQ-ADR-012` | `AQ-08` |
| `crash_recovery` | Retain | WAL-first recovery; crash points re-expressed against `AttemptDisposition` | `AQ-03`, `AQ-08` |
| `concurrency_key` | Retain | Concurrency-key enforcement; adds wait-time policy per `AQ-ADR-009` | `AQ-06` |
| `observability` | Replace | Metrics/inspection surfaces are rebuilt; no developmental identifiers in labels | `AQ-12` |
| `cancellation` | Retain (attribution added) | Terminal finality retained; control mutations gain `ControlMutationContext` | `AQ-11` |
| `negative_transitions` | Retain (table grows) | Exhaustive transition table adds `Awaiting` rules | `AQ-02` |
| `lease_expiry` | Retain | Lease fencing and expiry | `AQ-03`, `AQ-08` |
| `wal_corruption_recovery` | Retain | Trailing-corruption repair policy carries into the target WAL format | `AQ-03` |
| `misfire` | Retain | Misfire policy for scheduled runs | `AQ-04` |
| `dispatch_invariants` | Retain (expanded) | Property-based dispatch invariants gain `Awaiting` | `AQ-06` |
| `concurrent_dispatch_stress` | Retain | Stress under concurrent dispatch | `AQ-13` |
| `snapshot_corruption_recovery` | Retain | Snapshot-as-acceleration fallback | `AQ-03` |
| `concurrent_mutation_boundary` | Retain | Sequence monotonicity in the authority lane | `AQ-03` |
| `mixed_attempt_outcomes` | Replace | Outcome kinds are superseded by `AttemptDisposition` | `AQ-08` |
| `crash_during_promotion` | Retain | Promotion durability | `AQ-03` |
| `multi_mutation_session` | Retain (commands change) | Single-session authority pipeline; command set is the target set | `AQ-03` |
| `kill_recovery` (chaos) | Retain | Abrupt-termination durability | `AQ-03`, `AQ-13` |

## Workflow feature tests

| Test | Classification | Reason | Re-proved / removed in |
|---|---|---|---|
| `handler_output_roundtrip` | Reject | `HandlerOutput` is removed outright; output becomes part of the compound disposition | `AQ-08` |
| `dag_ordering` | Retain | DAG dependencies remain first-class gates | `AQ-09` |
| `dag_failure_propagation` | Retain | Failed prerequisite cascades | `AQ-09` |
| `dag_cycle_rejection` | Retain | Cycle rejection at declaration | `AQ-09` |
| `hierarchy_lifecycle` | Retain (policy explicit) | Completion gating and cascade retained per `AQ-ADR-013` | `AQ-09` |
| `dynamic_submission` | Reject | Fire-and-forget `SubmissionChannel` is deleted; children are admitted with the parent disposition | `AQ-09` |
| `coordinator_multi_attempt` | Replace | `ChildrenSnapshot` delivery moves to resume context / handler input | `AQ-08`, `AQ-09` |
| `cron_scheduling` | Retain | Cron derivation | `AQ-04` |
| `workflow_crash_recovery` | Retain | Workflow state survives recovery | `AQ-09` |
| `dag_snapshot_recovery` | Retain | Dependency declarations survive snapshot recovery | `AQ-03`, `AQ-09` |
| `attempt_lineage` | Retain (accounting split) | Stable `RunId`, unique `AttemptId`; physical vs failure count added | `AQ-08` |

## Budget feature tests

| Test | Classification | Reason | Re-proved / removed in |
|---|---|---|---|
| `budget_enforcement` | Retain | Budget consumption | `AQ-10` |
| `budget_replenishment` | Retain | Replenishment | `AQ-10` |
| `suspend_resume` | Retain (meaning narrowed) | `Suspended` remains preemption only; waiting moves to `Awaiting` | `AQ-06`, `AQ-10` |
| `budget_recovery` | Retain | Budget state survives recovery | `AQ-10` |
| `suspended_concurrency_key` | Retain | Key behaviour under suspension; awaiting policy added separately | `AQ-06` |
| `budget_threshold_suspension` | Retain | Threshold preemption | `AQ-10` |
| `subscription_triggered_promotion` | Replace | Internal reactive subscriptions stay; external semantics move to signals | `AQ-10` |
| `budget_threshold_subscription` | Retain | Internal structural subscription | `AQ-10` |
| `cascading_budget` | Retain | Hierarchical cascade of budget effects | `AQ-10` |
| `custom_event_subscription` | Reject | `EventFilter::Custom` and `CustomEvent` are removed; durable signals replace them | `AQ-10` |

## Actor feature tests

| Test | Classification | Reason | Re-proved / removed in |
|---|---|---|---|
| `actor_registration` | Retain (renamed) | Registration with `ExecutorTraits` | `AQ-02`, `AQ-11` |
| `capability_matching` | Replace | Renamed to executor-trait matching; routing-only semantics asserted | `AQ-02`, `AQ-11` |
| `remote_actor_crash` | Retain (fenced) | Crash detection plus result-envelope fencing per `AQ-ADR-017` | `AQ-11` |
| `department_routing` | Replace | Routing by trait; must prove traits grant no authority | `AQ-11` |

## Platform feature tests

| Test | Classification | Reason | Re-proved / removed in |
|---|---|---|---|
| `multi_tenant_isolation` | Retain | Tenant isolation; campaign references must not cross tenants (`AQ-DD-003`) | `AQ-11` |
| `rbac_enforcement` | Retain (permissions added) | Typed permissions for admission, signals, waits, cancellation, reprioritization | `AQ-11` |
| `approval_workflow` | Retain | Approval flow as ordinary scheduled work | `AQ-11` |
| `ledger_recovery` | Retain | Ledger survives recovery | `AQ-11` |
| `triad_mvp` | Replace | End-to-end scenario rebuilt on target APIs | `AQ-12` |

## Summary

| Classification | Count |
|---|---:|
| Retain | 39 |
| Replace | 7 |
| Reject | 3 |
| **Total** | **49** |

Counts cover the 48 acceptance tests plus the chaos test registered in `Cargo.toml` at the
baseline. Tests whose classification is "Retain" with a qualifier are counted as Retain.
