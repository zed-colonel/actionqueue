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

## AQ-04 addendum: compound admission lineage

Activate WAL kind 256, record schema 1, as storage-owned AdmissionCommittedV1. It
contains immutable admission facts and the complete bounded initial run set. WAL frame
version and snapshot frame version remain 1. Snapshot schema, projection image, and
projection digest version advance to 2. The manifest requires those versions, refusing
AQ-03 development stores before opening writable state; no migration is provided.
The v1 evidence vector remains unchanged, and a separately hashed v2 vector is added.

Projection v2 includes immutable admission records and rebuilds both tenant/key and
task/admission indexes. Records preserve original task specifications and dependencies
so future controls cannot silently rewrite admission meaning. Snapshot validation checks
identity, tenant and dependency references, uniqueness, sequence, and canonical digest.
Admission is an immediate-sync operation; Deferred commits are rejected. Duplicate
resolution under the exclusive mutation owner precedes stale sequence and current
parent status checks. Uncertain append, sync, or publication failures fence the whole
authority, including cached duplicate reads, until it is reconstructed by recovery.

## AQ-05 addendum: durable signal lineage

Activate kinds 288–291/schema 1 for admitted signals, pin, unpin and bounded
retirement. Their storage-owned DTOs freeze all producer content and retention
attribution. Snapshot schema, projection image and projection digest advance to 3;
WAL and snapshot framing stay version 1. Version-2 development manifests are
rejected before writable access, without migration or rewriting. Existing v1/v2
vectors and frozen contract/archive evidence remain unchanged.

Projection v3 includes immutable signal records, pins, retirement state and an
independent signal high-water mark. Hydration checks digests, unique identities,
consecutive signal sequences, WAL bounds, ordered pins and retention transitions,
then rebuilds all indexes and resident counters. Recovery verifies against complete
WAL history and does not sample a clock or rerun current retention policy.
Signal operations use the existing prepare/append/immediate-sync/publish lane and
fence both writer and authority after uncertainty.
