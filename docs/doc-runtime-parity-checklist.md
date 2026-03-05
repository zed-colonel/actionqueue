# Documentation Runtime Parity Checklist

This checklist is the fail-fast review artifact for the documentation truth gate.

A public capability claim is accepted only when it maps to concrete runtime symbols and, where applicable, executable tests.

## 1) Shipped capability claim inventory (current repository state)

| Public surface claim | Status | Runtime evidence | Test evidence |
|---|---|---|---|
| Daemon HTTP server and API endpoints | **Shipped** | HTTP routes in `crates/actionqueue-daemon/src/http/` | Acceptance tests in `tests/acceptance/` |
| Daemon metrics exporter | **Shipped** | `/metrics` endpoint | `acceptance_observability` test |
| CLI subcommands (`daemon`, `submit`, `stats`) | **Shipped** | `crates/actionqueue-cli/src/cmd/` | CLI integration tests |
| Local executor timeout and cancellation | **Shipped** | `crates/actionqueue-executor-local/src/timeout.rs` | Timeout and cancellation unit tests |
| WAL v5 (postcard + CRC-32) durability/replay | **Shipped** | `crates/actionqueue-storage/src/wal/codec.rs` | Codec roundtrip, CRC-32 integrity, WAL replay tests |
| WAL partial write repair | **Shipped** | `crates/actionqueue-storage/src/wal/repair.rs` | `wal_corruption_recovery` acceptance test |
| Snapshot parity mapping (schema v8) | **Shipped** | `crates/actionqueue-storage/src/snapshot/mapping.rs` | Snapshot model parity tests |
| Runtime dispatch loop | **Shipped** | `crates/actionqueue-runtime/src/dispatch.rs` | All lifecycle acceptance tests |
| Backoff strategies (fixed + exponential) | **Shipped** | `crates/actionqueue-executor-local/src/backoff.rs` | Backoff unit tests |
| DAG dependency scheduling | **Shipped** | `crates/actionqueue-workflow/src/dag.rs` | `dag_ordering`, `dag_failure_propagation`, `dag_cycle_rejection` |
| Task hierarchy (parent-child) | **Shipped** | `crates/actionqueue-workflow/src/hierarchy.rs` | `hierarchy_lifecycle` acceptance test |
| Cron scheduling | **Shipped** | `crates/actionqueue-engine/src/derive/cron.rs` | `cron_scheduling` acceptance test |
| Dynamic task submission | **Shipped** | `crates/actionqueue-workflow/src/submission.rs` | `dynamic_submission` acceptance test |
| Budget enforcement | **Shipped** | `crates/actionqueue-budget/src/gate.rs` | `budget_enforcement`, `budget_replenishment` tests |
| Suspend/resume | **Shipped** | RunState::Suspended + dispatch handling | `suspend_resume` acceptance test |
| Event subscriptions | **Shipped** | `crates/actionqueue-budget/src/subscription/` | `subscription_triggered_promotion`, `custom_event_subscription` tests |
| Actor registration + heartbeat | **Shipped** | `crates/actionqueue-actor/src/registry.rs`, `heartbeat.rs` | `actor_registration`, `remote_actor_crash` tests |
| Capability routing | **Shipped** | `crates/actionqueue-actor/src/routing.rs` | `capability_matching` acceptance test |
| Multi-tenant isolation | **Shipped** | `crates/actionqueue-platform/src/tenant.rs` | `multi_tenant_isolation` acceptance test |
| RBAC enforcement | **Shipped** | `crates/actionqueue-platform/src/rbac.rs` | `rbac_enforcement` acceptance test |
| Append-only ledgers | **Shipped** | `crates/actionqueue-platform/src/ledger.rs` | `ledger_recovery` acceptance test |
| Approval workflows | **Shipped** | DAG + roles integration | `approval_workflow`, `triad_mvp` acceptance tests |

## 2) Positive checks (must all pass)

1. **P1**: each shipped capability statement in `README.md` maps to code symbols and test evidence.
2. **P2**: no roadmap-only capabilities are claimed as shipped.

## 3) Negative checks (any single fail blocks closure)

1. **N1**: documented endpoint/command lacks implementation evidence in repository code.
2. **N2**: implemented contract-relevant behavior is omitted or contradicted by public docs.
3. **N3**: docs contain contradictory wording between shipped and planned sections.

## 4) Reviewer execution steps

1. Read capability sections in `README.md`.
2. For each claim, resolve runtime evidence links and verify symbols are implemented.
3. Confirm shipped claims have matching tests where behavior is contract-relevant.
4. Mark gate result as pass/fail and record any blocking claim drift.

## 5) Gate decision rubric

- **Pass**: all positive checks pass and no negative check fails.
- **Fail**: any negative check fails or any shipped claim lacks concrete runtime evidence.
