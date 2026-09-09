# AQ-ADR-003 — Admission hash algorithm

- **Status:** Proposed. The recommended default below is the working implementation choice
  until code review produces a concrete counterexample (implementation plan, Section 3).
- **Decide before:** `AQ-04`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H7
- **Architecture references:** §13.3, §26.7 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

The admission digest is stored durably and compared across restarts and releases; it
needs a broadly implemented, collision-resistant algorithm and room for future agility.

## Recommended decision

Use SHA-256 over the canonical admission bytes. Store an algorithm identifier alongside
the digest (`AdmissionDigest { algorithm, bytes }`) so a future algorithm can coexist without
reinterpreting old records. Introduce the hash dependency only in this PR, after `AQ-ADR-002` is
accepted.

## Alternatives considered

BLAKE3 (acceptable, less universally available downstream); non-cryptographic
hashes (rejected: admission-key poisoning surface, §25.6).

## Consequences

One new cryptographic dependency in `actionqueue-core` or a thin adjacent module.
Downstream systems can recompute digests independently.

## Verification required

Known-answer test vectors; digest stability fixture; conflict surfaced without echoing
payload bytes.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | _pending_ |
| Accepted on | _pending_ |
| Superseded by | — |
