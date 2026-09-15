# AQ-ADR-014 — Causal inheritance

- **Status:** Accepted.
- **Decide before:** `AQ-09`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H10, AQ-H11, AQ-H19
- **Architecture references:** §15.3, §15.10, §19.6 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

Children admitted by a parent attempt need causal context. Copying everything invites
ontology creep; copying nothing loses lineage.

## Recommended decision

Children inherit `trace_id` and `purpose_ref` by default. A new `CausationLink` points
at the parent task, run, and attempt. `correlation_id` is inherited unless the parent disposition
supplies an explicit one. Only requester and origin references may be explicitly overridden, and
only through bounded `OpaqueRef` values. No metadata map is inherited because none exists.

## Alternatives considered

Free-form inheritance rules per field (rejected: §29.7).

## Consequences

Developmental arms fan out with preserved campaign lineage using only existing
fields (`AQ-DD-005`, `AQ-DD-012`).

## Verification required

Inheritance table tests; override rejection for non-overridable fields; replay
preserves inherited context.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | `AQ-02` |
| Accepted on | 2026-09-09 |
| Superseded by | — |
| Deferred verification | Verified in `AQ-02`: `CausalContext` construction, override, and JSON/postcard round trip; causation-link ancestry consistency (`continuation_vocabulary.rs`, `target_serde.rs`). Verified in `AQ-09`: exact inherited context with bounded correlation/requester/origin overrides, unknown-field rejection, original producing-attempt preservation on duplicate admission, and WAL/snapshot replay. |

Child keys use SHA-256 over a versioned domain-separated encoding of tenant,
parent TaskId, parent RunId, and the local key. AttemptId and observational
attribution are excluded. Canonical admission v2 includes lifecycle policy;
changed policy, dependencies, identity, or attribution conflicts atomically.
