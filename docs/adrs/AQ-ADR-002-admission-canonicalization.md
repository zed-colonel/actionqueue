# AQ-ADR-002 — Admission canonicalization

- **Status:** Accepted for AQ-04 implementation.
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
| Accepted in PR | AQ-04 |
| Accepted on | 2026-09-10 |
| Superseded by | — |

## CanonicalAdmissionV1 encoding (AQ-04)

Start with exact bytes `AQ-CONT-1\0admission\0` and a little-endian u32 canonical
version (`1`). Encode the following fields in order. Integers use their stated fixed
width, never varints. UUIDs use the 16 RFC UUID bytes, in display/network order.
Every option has a u8 tag (`0` absent, `1` present) followed by its value when present.
Every byte string or UTF-8 string has a little-endian u64 byte length followed by its
exact bytes. Collections have a u64 element count followed by their elements.

1. Task UUID; payload byte string; optional content-type string.
2. Policy u8: Once `0`; Repeat `1` followed by u32 count and u64 interval seconds;
   Cron `2` followed by expression string and optional u32 maximum occurrences.
3. Constraints: u32 maximum attempts; optional u64 timeout; optional concurrency key;
   u8 retry hold policy (`0` hold, `1` release); u8 wait hold policy (`0` release,
   `1` hold); u8 safety (`0` pure, `1` idempotent, `2` transactional); optional
   collection of executor trait strings.
4. Metadata: i32 priority, optional description string, tag string collection.
5. Optional parent UUID; dependency UUID collection; optional tenant UUID.
6. Trace string; correlation string; optional causation link. A link contains optional
   task, run, and attempt UUIDs, then an optional external-reference string.
7. Eight optional opaque reference strings: submitting principal, requesting actor,
   purpose, authorization, identity context, signed statement, proof context, origin.

Tags and executor traits are sorted by UTF-8 bytes and deduplicated. Dependencies are
sorted by UUID bytes and deduplicated. Other values preserve exact bytes, including
empty present strings and cron whitespace. The task UUID is digest-bearing: callers
must retain it with their key. Tenant absence is explicit. Lookup key, control context,
generated run identities, commit timestamp, and WAL sequence are excluded. The first
successful control context remains immutable on retries.

The current core stores only the default wait concurrency policy; V1 encodes that
`0` explicitly. Adding configurable durable wait policy remains its owning continuation
work item's responsibility. No second field is introduced by admission.

Hard input bounds apply before normalization allocation or hashing. Configurable
creation limits apply only after duplicate resolution. A dedicated Python encoder
pins a vector containing every optional causal field, set permutations, a negative
priority, and fixed UUIDs in `conformance/aq-cont-1/admission-v1-vector.json`.
