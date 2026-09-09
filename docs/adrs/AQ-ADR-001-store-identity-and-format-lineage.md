# AQ-ADR-001 — Store identity and format lineage

- **Status:** Proposed. The recommended default below is the working implementation choice
  until code review produces a concrete counterexample (implementation plan, Section 3).
- **Decide before:** `AQ-03`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H2, AQ-H16
- **Architecture references:** §21.1, §21.6, §27.5, §28.12 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

The baseline persists WAL v5 records and snapshot schema v8 with no directory-level
manifest. A target store must be unambiguously identifiable so the runtime never opens,
upgrades, or mixes a pre-contract store, and so future supported upgrades have an explicit
starting point.

## Recommended decision

Write a human-readable `manifest.json` in the data directory root containing at least
`contract = "AQ-CONT-1"`, `wal_format = 1`, `snapshot_schema = 1`, creation metadata, and
feature-compatibility data. Refuse to open a nonempty directory that lacks a recognized target
manifest with the precise `UnsupportedStoreFormat` / `MissingTargetManifest` errors. Never
modify the rejected directory.

## Alternatives considered

In-place `v5 → v6` migration (rejected: pre-production data is evidence, not an
obligation); sniffing WAL framing bytes instead of a manifest (rejected: ambiguous and silent).

## Consequences

Pre-contract data directories are inert. Backup/restore and `storage inspect`
have a stable identity record. Format bumps after the first target release require fixtures and
source-preserving migration per §21.6.

## Verification required

Reject-old fixture test (archived v5/v8 store is refused, untouched, and produces the
documented error); manifest round-trip; replay of WAL-only versus snapshot-plus-tail yields an
identical projection digest.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | _pending_ |
| Accepted on | _pending_ |
| Superseded by | — |
