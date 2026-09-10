# AQ-ADR-001 — Store identity and format lineage

- **Status:** Accepted for AQ-03 implementation.
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
| Accepted in PR | AQ-03 |
| Accepted on | 2026-09-10 |
| Superseded by | — |

## AQ-03 implementation decisions

Manifest schema, WAL framing, snapshot schema, and projection are version 1. Sessions
validate compatibility before opening the persistent lock file, revalidate under an OS
lock, and own that lock through the runtime/authority lifetime. Fresh initialization and
restore publish synced sibling staging directories. No old-reader or format-upgrade
branch remains. Complete WAL history is mandatory.

Stable kind identifiers, a store UUID and checksummed bounded frame headers distinguish
target WAL bytes independently of the manifest. Reserved continuation families reject
until their owning work items implement them. Store feature profiles are immutable and
are enforced independently of a reader's compiled capabilities.

Exact storage-owned hydration and canonical SHA-256 replace synthetic snapshot events.
Full-WAL verification currently accompanies snapshot recovery. Prepared mutations clone
the retained projection because its reducer has no affected-map patch interface; this
is a performance tradeoff, not a durable-format choice. See the storage README for
canonical byte rules, reserved IDs, and offline backup descriptor semantics.

The archived fixture capture example is no longer registered as a build target; its
hash-pinned historical source remains unchanged. Low-level framing tests explicitly use
the test-support constructors, while target conformance initializes real manifest stores.
