# AQ-CONT-1 Architectural Decision Records

This directory holds the decision queue from Section 3 of the
[implementation plan](../planning/aq-cont-1/aq-cont-1-implementation-plan.md) and Section 29.13
of the [architecture document](../contracts/actionqueue-hardening-implementation-ready.md).

Each ADR records a recommended default. A recommended default is the working implementation
choice unless code review produces a concrete counterexample. An ADR must be marked Accepted
in the same PR that first depends on it. A PR that changes a durable record, transition,
matching rule, or digest algorithm must reference the governing ADR and include replay tests.

| ADR | Title | Must be fixed before | Status |
|---|---|---|---|
| [`AQ-ADR-001`](AQ-ADR-001-store-identity-and-format-lineage.md) | Store identity and format lineage | `AQ-03` | Proposed |
| [`AQ-ADR-002`](AQ-ADR-002-admission-canonicalization.md) | Admission canonicalization | `AQ-04` | Proposed |
| [`AQ-ADR-003`](AQ-ADR-003-admission-hash-algorithm.md) | Admission hash algorithm | `AQ-04` | Proposed |
| [`AQ-ADR-004`](AQ-ADR-004-signal-matching-grammar.md) | Signal matching grammar | `AQ-05` | Proposed |
| [`AQ-ADR-005`](AQ-ADR-005-signal-ordering-and-cursor.md) | Signal ordering and cursor | `AQ-05` | Proposed |
| [`AQ-ADR-006`](AQ-ADR-006-signal-retention.md) | Signal retention | `AQ-05` | Proposed |
| [`AQ-ADR-007`](AQ-ADR-007-wait-cardinality.md) | Wait cardinality | `AQ-06` | Proposed |
| [`AQ-ADR-008`](AQ-ADR-008-wait-timeout-race.md) | Wait timeout race | `AQ-06` | Proposed |
| [`AQ-ADR-009`](AQ-ADR-009-concurrency-key-while-awaiting.md) | Concurrency key while awaiting | `AQ-06` | Proposed |
| [`AQ-ADR-010`](AQ-ADR-010-checkpoint-representation.md) | Checkpoint representation | `AQ-07` | Proposed |
| [`AQ-ADR-011`](AQ-ADR-011-disposition-invalidity.md) | Disposition invalidity | `AQ-08` | Proposed |
| [`AQ-ADR-012`](AQ-ADR-012-attempt-accounting.md) | Attempt accounting | `AQ-08` | Proposed |
| [`AQ-ADR-013`](AQ-ADR-013-parent-completion-with-children.md) | Parent completion with children | `AQ-09` | Proposed |
| [`AQ-ADR-014`](AQ-ADR-014-causal-inheritance.md) | Causal inheritance | `AQ-09` | Proposed |
| [`AQ-ADR-015`](AQ-ADR-015-reactive-subscription-ownership.md) | Reactive subscription ownership | `AQ-10` | Proposed |
| [`AQ-ADR-016`](AQ-ADR-016-control-authentication-hook.md) | Control authentication hook | `AQ-11` | Proposed |
| [`AQ-ADR-017`](AQ-ADR-017-remote-actor-result-envelope.md) | Remote actor result envelope | `AQ-11` | Proposed |
| [`AQ-ADR-018`](AQ-ADR-018-crate-extraction.md) | Crate extraction | `post-release` | Proposed |

## Lifecycle

```text
Proposed  → Accepted (in the PR that first depends on it)
Accepted  → Superseded (by a later ADR with an explicit contract amendment)
```

A failed planning assumption (plan Appendix D) creates a new ADR and a contract review; it does
not justify an undocumented local workaround.
