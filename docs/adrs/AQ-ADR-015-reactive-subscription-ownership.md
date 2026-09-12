# AQ-ADR-015 — Reactive subscription ownership

- **Status:** Accepted.
- **Decide before:** `AQ-10`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H3
- **Architecture references:** §17.1–§17.7, §2.3 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

Baseline subscriptions evaluate tick events, including `EventFilter::Custom`, and are
used both for internal reactivity and for external wake-up. External wake-up over ephemeral
events is lost across restart.

## Recommended decision (accepted)

Keep internal queue-event subscriptions as a separate, non-durable-wakeup mechanism
owned by `actionqueue-engine::reactivity::InternalSubscriptionRegistry`. Runtime
subscription APIs and storage profiles retain the existing budget feature gate. Delete `EventFilter::Custom` and
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
| Accepted in PR | `AQ-10` |
| Accepted on | 2026-09-11 |
| Superseded by | — |

Budget caches restore complete projection records after accepted mutations. Budget
exhaustion gates dispatch, not wait satisfaction; pending input survives until an
accepted start. Replenishment does not resolve waits or resume suspension.

Internal triggers are persisted for inspection, but notification is post-commit
and rearming changes only memory. No replay-complete recurring notification
promise is made. The storage-owned structural filter format preserves tags 0–2;
removed tag 3 fails closed in WAL and snapshots without migration.
