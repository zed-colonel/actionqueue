# AQ-ADR-013 — Parent completion with children

- **Status:** Accepted.
- **Decide before:** `AQ-09`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H8
- **Architecture references:** §19.4, §19.5, §28.9 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

Baseline hierarchy gates parent completion on children. Compound child admission
makes the gate atomic but the policy for detached children must be explicit.

## Recommended decision

Preserve explicit completion gating: a parent cannot complete while required children
are nonterminal. A task policy may declare children detached, in which case they neither gate
completion nor are canceled with the parent. The default is required (gated).

## Alternatives considered

Always detached (rejected: silent orphaning); always gated (rejected: legitimate
fire-and-forget children exist when explicitly declared).

## Consequences

Coordinator dispositions declare child requirement at admission time; `AQ-DD-005`
atomic fan-out relies on this.

## Verification required

Gating, detached, cascade-cancel, and crash-between-child-admission tests.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | `AQ-09` |
| Accepted on | 2026-09-11 |
| Superseded by | — |

Completion is checked at storage preparation and replay, including legacy attempt
closure and run completion paths. Child failure/cancellation satisfies the gate;
DAG success still requires at least one successful run after task termination.
Detached edges stop cancellation traversal. Direct child waits carry immutable
terminal evidence and reconcile from durable task state before dispatch/deadlines.

Verified by `acceptance_transactional_child_admission` and
`acceptance_parent_wait_child_atomicity`, including WAL-only and snapshot recovery.
