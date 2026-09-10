# actionqueue-storage

AQ-CONT-1 persistence begins at version 1. Pre-contract WAL v5 and snapshot schema 8
are evidence only: this crate has no compatibility reader or migration path.

## Ownership and opening

Use `store::open_store(root, OpenOptions)` with `Initialize { features }`, `ReadWrite`,
or `ReadOnly`. Initialization accepts only absent/empty directories and publishes a
fully synced sibling staging directory by atomic directory rename. An interrupted
staging directory is never adopted by scanning. Initialization races validate the winner.

`manifest.json` is bounded to 16 KiB, rejects unknown/duplicate fields, and identifies
contract, manifest/WAL/snapshot/projection versions, UUID, creation time/creator,
sorted immutable store features, and SHA-256 algorithms. Manifest compatibility is
validated before opening the existing `store.lock`, then revalidated under its lock.
Nonempty unidentified stores and unsupported manifests receive no writes.

A writable session holds an exclusive OS file lock; offline sessions hold shared
locks. Lock contention returns `StoreInUse`. The WAL writer owns its session for its
entire lifetime; the daemon also retains it when controls are disabled. Snapshot
writers require an exclusive session. Production builds have no path-only filesystem
writer constructors. The `testing` feature exposes raw framing fixtures only.

The feature profile enables operations **for the store**, not every feature compiled
into a reader. Additional binary capabilities do not upgrade an existing profile.
Budget/subscription, actor, platform, and cron writes require their store feature.
Hierarchy and dependency declarations remain base operations, as in the retained
runtime. Profile upgrades are not implemented.

## Durable representation

WAL v1 uses a 52-byte little-endian header followed by at most 16 MiB of postcard data:

| Offset | Field |
|---|---|
| 0 | 8-byte `AQCONT1W` magic |
| 8 | u32 format = 1 |
| 12 | u16 record kind |
| 14 | u16 payload schema = 1 |
| 16 | 16-byte store UUID |
| 32 | u64 sequence |
| 40 | u32 payload length |
| 44 | u32 payload CRC-32 |
| 48 | u32 CRC-32 of header bytes 0..48 |

Sequence 1 is `StoreInitialized`, containing the SHA-256 binding of the immutable
manifest's compact, fixed-struct-order JSON representation. Every subsequent sequence
is exactly previous + 1, checked for overflow. `wire_v1.rs` owns explicit kind IDs and
named v1 payload structures; Rust enum discriminants are not record IDs. Scheduling
policy uses an explicit always-present wire layout, including cron with an absent
occurrence limit. Existing bounded domain values retain their validation.

Kinds 16–46 carry retained task/run, attempt, lease, engine, dependency, suspension,
budget, subscription, actor, and platform records. Reserved unsupported kinds are
256 (admission), 272/273 (compound attempt start/disposition), 288 (signal), 304–307
(wait lifecycle), and 320/321 (attributed controls). A reserved/unknown kind or schema
fails; none is treated as a no-op. Each owning work item must add encoding, semantic
validation, reduction, snapshot representation, and replay tests together.

Mutation authority prepares a cloned projection before append, syncs when required,
then publishes the prepared state. This currently clones the whole projection because
the retained reducer mutates several private maps per event. This favors correctness
over large-store mutation throughput; it can later be replaced by affected-map patches
without changing the durable format. Filesystem writers also validate against their
recovered state, and sync failure fences further writes. Recovery uses the same target
validation, including references and dependency cycles. Invalid attempt outcomes and
actor traits are rejected instead of reconstructed into different values.

One bounded streaming parser serves WAL iteration and repair. `Strict` is the default.
`TruncatePartial` can truncate only an incomplete final target frame after a validated
semantic prefix. A complete header must pass format, identity, kind/schema, bounds,
sequence, and integrity checks before short payloads qualify. CRC failures, invalid
payloads, gaps, unknown records, and interior damage never qualify; recovery never
resynchronizes past damage. Prefix recovery and snapshot verification precede writable
opening and truncation. The complete WAL is retained; missing/empty WALs and
snapshot-only stores are rejected.

## Exact snapshots and projection digest

Snapshots use `AQCONT1S`, u32 frame version, u32 payload length, u32 payload CRC, and a
strict JSON envelope, with a symmetric 256 MiB writer/reader payload limit. The envelope
binds store UUID, snapshot/projection versions, covered sequence, and projection digest.
It reserves empty admission, signal, wait, checkpoint, resume-assignment and causal/control
sections; nonempty sections are unsupported. These reservations do not provide continuation
recovery before AQ-04–AQ-08.

`ProjectionImageV1` restores complete state directly without synthesizing events:
run priority and transition times, full attempt/output history, active leases, control
state, original dependency timestamps, and feature records. Derived indexes are rebuilt;
hydration must reproduce the original image digest. Snapshot writers verify their image
against the covered WAL prefix and sync that WAL before snapshot publication. They retain
temp-file write, sync, rename, and parent-directory sync. Only physical snapshot damage
permits fallback. Identity, compatibility, and semantic failures halt recovery.

Projection SHA-256 uses domain bytes `AQ-CONT-1\0projection\0v1\0`, followed by the
following canonical **typed tree**, not JSON serializer output:

- Null: tag 0. Boolean: tag 1 followed by byte 0/1.
- Nonnegative integer: tag 2 + u64 LE; negative integer: tag 3 + i64 LE. No floats.
- String: tag 4 + u64 LE UTF-8 byte length + UTF-8 bytes.
- Array: tag 5 + u64 LE item count + items in order.
- Object: tag 6 + u64 LE field count + string-key/value pairs, sorted by UTF-8 key.

The fixed v1 image fields are defined by `snapshot/model.rs`. Every field is included;
snapshot creation time is normalized to zero. Task/run/dependency/subscription/actor/
tenant maps are ordered by UUID; dependency sets are ordered by UUID. Budgets, roles,
and capability grants are ordered by canonical record bytes. Chronological state,
attempt and ledger vectors retain order. Sequence and durable state are included;
paths, metrics and recovery duration are excluded. The independent Python-generated
vector is `conformance/aq-cont-1/projection-v1-vector.json`.

AQ-03 verifies snapshot-plus-tail against full WAL replay at opening. It intentionally
pays full-history replay cost while the complete WAL is required. Compaction and a
trusted history boundary require a separate specification.

## Offline operations

The existing `actionqueue-cli` binary exposes:

```text
actionqueue-cli storage inspect --data-dir DIR [--json]
actionqueue-cli storage backup --data-dir DIR --output BACKUP [--json]
actionqueue-cli storage restore --input BACKUP --data-dir DEST [--json]
```

Inspection reports versions, store feature profile, binary capabilities, tail health,
sequence, counts and digest without opaque payloads. It never initializes or repairs.
Backup requires a quiescent source, copies the complete WAL and only a validated
snapshot, records file lengths/SHA-256s and projection identity in `backup.json`, then
verifies the staged backup before publishing it. Restore checks the bounded descriptor
and manifest before payload decode, rejects symlinks, traversal, unexpected entries,
source/destination overlap and populated destinations, and verifies checksums plus both
recovery paths before atomic publication. Both preserve identity and never reconcile
leases, invoke handlers or retrieve external artifacts. External artifact bytes are
outside the backup; their durable references remain opaque and preserved.

## Verification

`cargo aq-conformance` includes target lineage, corruption, projection, failure-injection,
real process-kill lock recovery, and offline transfer tests. Run the standard workspace
and expanded feature matrices as well. Actual independent binary compatibility is tested
with `conformance/aq-cont-1/cross-feature-persistence.sh` from the repository root.
