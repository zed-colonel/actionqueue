# AQ-ADR-002 — Admission canonicalization

- **Status:** Proposed. The recommended default below is the working implementation choice
  until code review produces a concrete counterexample (implementation plan, Section 3).
- **Decide before:** `AQ-04`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H6, AQ-H7
- **Architecture references:** §13.1–§13.4, §26.7, §28.8 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

Idempotent admission must detect semantic conflict under a stable `AdmissionKey`.
Hashing arbitrary `serde` output or in-memory layout is fragile across dependency and field
ordering changes.

## Recommended decision

Build a dedicated normalized `CanonicalAdmission` value with a versioned canonical byte
encoding. Hash those bytes. Field order, optional-field absence, and integer widths are fixed by
the canonical form, not by the serializer.

## Alternatives considered

Hash `serde_json` output (rejected: map ordering and float formatting drift);
hash postcard output (rejected: layout coupled to struct evolution).

## Consequences

Adding a digest-bearing field is a canonical-form version bump and an ADR
amendment. Conflict detection is deterministic across releases that share a canonical version.

## Verification required

Property test: equal admissions hash equal, any digest-bearing change hashes different;
fixture with pinned canonical bytes and digest; `AQ-DD-009` and `AQ-DD-010` neutrality cases.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | _pending_ |
| Accepted on | _pending_ |
| Superseded by | — |
