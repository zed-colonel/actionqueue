# AQ-ADR-016 — Control authentication hook

- **Status:** Accepted.
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
| Accepted in PR | `AQ-02` |
| Accepted on | 2026-09-09 |
| Superseded by | — |
| Deferred verification | Verified in `AQ-02`: `ControlMutationContext` round trip, and the acceptance proof that executor traits grant no RBAC permission (`tests/acceptance/executor_trait_matching.rs`). Deferred to `AQ-11`: rejection of control mutations without context, and verbatim replay of context. |

## AQ-11 implementation evidence

The shared host service uses `HostControlContext` (not deserializable) and typed
queue-action grants. HTTP actor/platform/control routes fail closed without a
configured host hook. Explicit scope and current permission checks precede
idempotency responses. `acceptance_control_mutation_attribution` covers body
attribution replacement, permission revocation, reference neutrality, attributed
WAL/snapshot replay, and administrative suspension/resumption through the service.

WAL kind 352/schema 1 contains the underlying mutation and control attribution in
one frame. Snapshot/projection version 8 retains those contexts. Existing version
7 stores are rejected by manifest validation before mutation.

The remaining ADR verification is rejection of context-free submissions through
all older mutation/runtime entry points; those entry points have not yet all been
migrated to the host service.
