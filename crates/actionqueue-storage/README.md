# actionqueue-storage

Durable persistence and mutation authority for ActionQueue / AQ-CONT-1.
Store manifest schema 1, WAL framing 1, snapshot schema 10, projection version 9.
Released schema-9 stores have an explicit source-preserving offline upgrade;
pre-contract and older development stores remain unsupported.

Use `store::open_store` with an explicit `OpenOptions` mode. A writable session
owns the exclusive OS lock for its lifetime. Initialization only accepts an
absent or empty store and publishes a fully synced staging directory. The
immutable manifest binds store UUID, feature profile, versions and hash algorithms.
A broader binary cannot upgrade the store profile.

The mutation authority prepares a projection, appends and syncs the authoritative
WAL frame, then publishes state. Uncertain writes fence subsequent mutation until
recovery. Preparation shares persistent projection roots and copies only changed
paths and records; see [ADR-019](../../docs/adrs/AQ-ADR-019-structurally-shared-projections.md).
Production filesystem writers require a store session; raw fault-injection hooks
require `testing`.

WAL frames have a 52-byte header with explicit kind/schema, store identity,
sequence, length, payload CRC and header CRC. Snapshots include durable waits,
checkpoints, resume assignments, child admissions and causal/control history.
Recovery checks checkpoint integrity and WAL boundary frames, hydrates projection
and operation-history indexes, and reads only the tail. The skipped prefix is
trusted until explicit full verification; see [ADR-020](../../docs/adrs/AQ-ADR-020-bounded-snapshot-bootstrap.md).
Capture from a proven live authority flushes its WAL and shares immutable roots;
publication performs no prefix replay. Compaction is not implemented.

`Strict` is the default repair policy. `TruncatePartial` only repairs an incomplete
final frame after a validated semantic prefix. CRC, identity, compatibility,
sequence and semantic errors halt recovery. No resynchronization skips corruption.
Snapshot-only stores are rejected. Backup/restore preserve identity and verify
checksums and projection digests before atomic publication.

```sh
actionqueue store inspect --data-dir DIR
actionqueue store verify --data-dir DIR --json
actionqueue store upgrade --data-dir SCHEMA9_SOURCE --output SCHEMA10_DEST
actionqueue backup --data-dir DIR --output BACKUP
actionqueue restore --input BACKUP --data-dir DEST
```

These are offline operations; stop the store owner first. External artifact bytes
remain application-owned. Signal retirement removes matching eligibility, while
immutable identity/content/history remain resident and count against quotas.
Wait/resume/history references protect retained signals from retirement.

See [store format](../../docs/data-dir-format-v1.0.md),
[recovery guide](../../docs/wal-recovery-guide.md), and
[API disclosure and bounds](../../docs/aq-12-apis.md). Run `cargo aq-conformance`
and the cross-feature persistence script from the repository root.
