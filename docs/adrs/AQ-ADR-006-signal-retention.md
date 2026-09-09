# AQ-ADR-006 — Signal retention

- **Status:** Proposed. The recommended default below is the working implementation choice
  until code review produces a concrete counterexample (implementation plan, Section 3).
- **Decide before:** `AQ-05`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H3, AQ-H5
- **Architecture references:** §9.9, §21.5, §26.6, §29.5 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

Retained signals are required for late waits and for replay; unbounded retention
threatens storage simplicity, while aggressive deletion reintroduces lost wakeups.

## Recommended decision

Conservative default: retain signals until an explicit, configurable horizon elapses
and no active or historical wait references them. Support explicit pins. Never automatically
delete a signal referenced by an active wait. Compaction is a separate, explicit operator or
policy action with metrics.

## Alternatives considered

Delete on first match (rejected: fan-out semantics); infinite retention
(rejected: capacity).

## Consequences

Retention pressure is measurable before compaction is designed. Snapshots record
the signal sequence covered.

## Verification required

Retention-horizon tests; pinned-signal survives compaction; replay after compaction
reproduces active-wait resolution.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | _pending_ |
| Accepted on | _pending_ |
| Superseded by | — |
