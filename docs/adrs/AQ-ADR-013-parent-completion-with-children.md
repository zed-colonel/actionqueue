# AQ-ADR-013 — Parent completion with children

- **Status:** Proposed. The recommended default below is the working implementation choice
  until code review produces a concrete counterexample (implementation plan, Section 3).
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
| Accepted in PR | _pending_ |
| Accepted on | _pending_ |
| Superseded by | — |
