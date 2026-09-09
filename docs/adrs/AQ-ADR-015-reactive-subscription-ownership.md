# AQ-ADR-015 — Reactive subscription ownership

- **Status:** Proposed. The recommended default below is the working implementation choice
  until code review produces a concrete counterexample (implementation plan, Section 3).
- **Decide before:** `AQ-10`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H3
- **Architecture references:** §17.1–§17.7, §2.3 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

Baseline subscriptions evaluate tick events, including `EventFilter::Custom`, and are
used both for internal reactivity and for external wake-up. External wake-up over ephemeral
events is lost across restart.

## Recommended decision

Keep internal queue-event subscriptions as a separate, non-durable-wakeup mechanism
owned by the budget/reactivity layer. Delete `EventFilter::Custom` and
`ActionQueueEvent::CustomEvent` as external semantics; durable `SignalEnvelope` and `WaitSpec`
are the only external continuation path.

## Alternatives considered

Make custom events durable (rejected: duplicates signals with weaker
matching); delete subscriptions entirely (rejected: internal reactivity remains useful).

## Consequences

`AQ-10` removes the custom-event acceptance tests and adds deletion tests
(§17.7).

## Verification required

Absence of the legacy symbols (staged boundary check); internal subscriptions still
promote on structural events; external wake-up survives restart only through signals.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | _pending_ |
| Accepted on | _pending_ |
| Superseded by | — |
