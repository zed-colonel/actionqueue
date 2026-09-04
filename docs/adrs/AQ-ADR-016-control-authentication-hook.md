# AQ-ADR-016 — Control authentication hook

- **Status:** Proposed. The recommended default below is the working implementation choice
  until code review produces a concrete counterexample (implementation plan, Section 3).
- **Decide before:** `AQ-11`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H11, AQ-H15
- **Architecture references:** §15.5, §15.6, §25.1, §25.12 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

Cancellation, resume, reprioritization, and signal admission must be attributable
without making ActionQueue an authentication system.

## Recommended decision

ActionQueue records a host-attested `ControlMutationContext` on every control
mutation. Authentication and authorization are performed by the daemon or platform host before
the mutation is proposed; ActionQueue validates only structural completeness of the context.
No reference inside the context (including campaign or identity references) grants authority.

## Alternatives considered

Embedding token validation in core (rejected: non-goal, §4.2).

## Consequences

`AQ-DD-003` and `AQ-DD-008` are provable without campaign-aware code.

## Verification required

Control mutations without context are rejected; context is replayed verbatim;
platform tests prove references do not widen permissions.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | _pending_ |
| Accepted on | _pending_ |
| Superseded by | — |
