# AQ-ADR-004 — Signal matching grammar

- **Status:** Accepted.
- **Decide before:** `AQ-05`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H3, AQ-H5
- **Architecture references:** §9.4, §9.5, §10.3, §25.4, §28.5 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

Durable waits must match durable signals deterministically at registration time, at
signal arrival, and during replay. Arbitrary predicates make replay and security analysis
intractable.

## Recommended decision

Match on exact tenant plus exact namespace and kind, with optional exact
`correlation_id` and exact `source` filters. No wildcards, regex, ranges, or payload
predicates in `AQ-CONT-1`. Absence of a filter is represented structurally, never by an empty
string. Broad filters (no correlation) are permitted but explicit and inspectable.

## Alternatives considered

Predicate language or JSONPath over payload (rejected: replay non-determinism,
broker drift, §29.1); regex on kind (rejected: unbounded matching cost).

## Consequences

Correlation-first design; applications carry meaning in payload references, not
in matchers. Matching is index-friendly (§26.1).

## Verification required

Exhaustive matcher table; constructor rejection of empty/unbounded filters; cross-tenant
non-match; replay-equivalence of match decisions.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | `AQ-02` |
| Accepted on | 2026-09-09 |
| Superseded by | — |
| Deferred verification | Verified in `AQ-02`: exhaustive filter table with exact tenant and optional-attribute equality, constructor rejection of empty or unbounded filters, and cross-tenant non-match (`crates/actionqueue-core/tests/continuation_vocabulary.rs`). Verified in `AQ-05`: indexed candidates against the pure matcher across optional correlation/source combinations, exclusive cursor pagination and replay (`tests/acceptance/signal_admission.rs`); retirement and snapshot/backup parity (`tests/acceptance/signal_retention.rs`). |
