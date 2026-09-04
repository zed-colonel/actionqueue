# AQ-ADR-018 — Crate extraction

- **Status:** Proposed. The recommended default below is the working implementation choice
  until code review produces a concrete counterexample (implementation plan, Section 3).
- **Decide before:** `post-release`
- **Contract:** `AQ-CONT-1`
- **Invariants:** —
- **Architecture references:** §22.1, plan §2.4, plan Appendix D of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

Continuation logic could live in a new `actionqueue-continuation` crate. Extracting
before the interfaces stabilize would freeze module boundaries prematurely.

## Recommended decision

Keep continuation modules inside the existing eleven crates for `AQ-CONT-1`. Revisit
extraction only after dependency pressure and performance evidence justify it.

## Alternatives considered

Extract now (rejected: no evidence yet; the DAG remains serviceable).

## Consequences

The workspace member list is unchanged through the first target release; the
boundary checks assume the eleven-crate layout.

## Verification required

Not applicable until reconsidered; the decision is recorded so the omission is
deliberate.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | _pending_ |
| Accepted on | _pending_ |
| Superseded by | — |
