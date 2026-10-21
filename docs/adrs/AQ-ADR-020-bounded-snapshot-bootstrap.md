# AQ-ADR-020 — Bounded snapshot bootstrap

Status: accepted for AQ-P3 / AQ-PERF-1. Supersedes the full-history startup
statements in ADR-001 and ADR-006, and refines ADR-019's hydration implementation.
The frozen AQ-CONT-1 contracts and archived evidence remain unchanged.

## Decision and trust boundary

Ordinary writable startup validates the manifest, checkpoint integrity, identity,
feature profile, projection invariants, and inspection associations. It checks
sequence 1's manifest binding and the last covered frame's sequence, exact-frame
SHA-256, start/end offsets and file bounds. It then seeks directly to the tail.
Only tail events enter the reducer. Writer initialization consumes the same
recovered session proof and cursor, without scanning again. The complete WAL
remains mandatory; there is no retention or WAL truncation in this change.

**Ordinary startup does not recompute the skipped-prefix commitment and cannot
detect changes wholly within that prefix when the checked boundary remains
identical.** The rolling commitment is integrity evidence, not authentication.
`store verify`, backup/restore, upgrades, and validation before destructive tail
repair recompute it against every exact encoded frame. Physically damaged or
missing snapshots permit WAL-only startup. Identity, compatibility, semantic and
boundary failures halt startup. Explicit verification rejects a present damaged
snapshot; backup alone may omit a physically damaged source snapshot, after fully
validating its WAL. Every included and staged snapshot is fully verified.

## Schema 10 and compatibility

New manifests use snapshot schema 10; WAL framing remains 1 and logical projection
and canonical digest version remain 9. Snapshot frame 2 has the existing 20-byte
magic/version/length/CRC header. Its payload consists of length-delimited binary
records and a domain-separated SHA-256 over all binding and projection bytes.
Both the payload and complete frame are bounded by 268,435,456 bytes. Top-level
collections and control/state histories are bounded to 1,000,000 rows; record
lengths are checked against remaining input
before allocation, numeric tags and references are checked, and trailing bytes
are rejected. Existing domain payload limits also apply. A publication exceeding
these limits fails; it does not discard history or raise a ceiling.

The binding contains fixed-byte store UUID, manifest SHA-256, commitment version,
covered sequence/end offset, last frame start offset/hash and prefix commitment.
For each exact frame, commitment v1 is SHA-256 of
`AQ-CONT-1\0wal-prefix\0v1\0 || previous_commitment || sequence_le64 ||
start_le64 || frame_length_le64 || sha256(frame)`; the initial commitment is 32
zero bytes. Historical encodings are hashed as read, never re-encoded.

The codec uses explicit ordered DTOs and checked run-to-task ordinals. Task and
admission records reuse frozen WAL DTOs. UUIDs in these frozen DTOs retain their
binary UUID encoding; new references use fixed 16-byte identifiers. Opaque task
payloads remain bytes. Tagged continuation/extension records retain an explicitly
versioned logical-v9 JSON subcodec inside length-delimited sections because their
validated tagged deserializers require it. This avoids inventing another
continuation format while removing the whole-image JSON tree. Field order and
numeric tags are frozen by `checkpoint-v9.hex` and `checkpoint-v10.hex` fixtures.
The encoder reports the actual byte count of each section, including indexes and
framing. Canonical v9 hashing sorts references without changing operational run
order and streams one logical record at a time to SHA-256.

Schema-9 stores require an explicit offline upgrade:

```sh
actionqueue store upgrade --data-dir SOURCE --output DEST
```

The compatibility reader fully verifies the schema-9 source, including every
present snapshot, while holding its shared lock. It creates a separate staging
directory, preserves UUID, creation fields, feature profile and all domain history,
and changes only the destination manifest's snapshot schema. It regenerates
sequence 1's manifest binding; subsequent frames remain byte-for-byte identical.
Fresh offsets and commitments are computed from the destination WAL. Projection
digest and logical inspection associations must agree before publication, and the
entire destination is verified. The source is untouched. Populated or overlapping
destinations fail. Writable opening never silently upgrades schema 9. Earlier
unreleased schemas and pre-contract stores remain unsupported. There is no
downgrade or automatic restore-time migration: upgrade the source/backup first,
then create a schema-10 backup with a fresh descriptor.

## Proven capture and publication

`capture_checkpoint(&mut authority, timestamp)` flushes the covered WAL and
captures structurally shared immutable roots and the exact matching cursor under
exclusive authority ownership. Recovery supplies private session/revision
provenance. Independently paired projections/writers, even at equal sequences,
cannot use this fast API. The arbitrary `SnapshotWriter` API retains full prefix
validation. Candidates keep the session lock alive until consumed or dropped.

`publish_checkpoint(candidate)` builds and encodes outside the authority lane.
Publication is serialized per session, rejects older candidates, syncs a unique
temporary file, atomically renames it and syncs the directory. The acknowledged
published sequence advances only after success. Snapshot-only errors leave durable
progress pending; uncertain WAL flushes fence the authority. Publication replays
zero prefix events. Existing crash boundaries remain in use.

The runtime and daemon threshold is `latest_sequence - last_successfully_published_sequence`,
seeded by recovery. Admissions between ticks and recovered tails count. The daemon
default is 10,000 durable events; configuration can change or disable it. It keeps
one publication in flight, releases the mutation lock before encoding, and awaits
the maintenance/publication task at shutdown.

## Hydration and operation history

Snapshots retain control-target associations and attempt ownership, including
historical targets without a live-table ordinal. Hydration checks unique ordered
associations, ownership, control-record references and sequence ranges, and
rebuilds exact trace/correlation/origin lookup from admissions. Full verification
compares logical associations as well as projection digests at the cut and end.

Measurements rejected a sequential persistent-map insertion implementation: its
hydration time grew faster than snapshot bytes. Hydration now bulk-builds immutable
native hash tables with persistent deltas for later mutations. Clones share both
roots; a write copies only its addressed record/path, and tombstones shadow removed
base entries. Memory is bounded by retained identities plus their deltas, not WAL
length. Independent roots use scoped workers; large record sections decode with
at most four workers and a 32,768-record grain. Every worker joins before the
projection is exposed. Revision buckets and relationship tables are built once.
No deferred full-table copy is charged to the first mutation after hydration.

## Verification and performance evidence

```sh
actionqueue store verify --data-dir DIR --json
```

Verification is read-only: it never initializes, repairs, reconciles execution or
runs handlers. It validates the entire WAL, recomputes the covered commitment,
compares the exact checkpoint projection, per-task run order and operation associations, then verifies
final snapshot-plus-tail equivalence. Backup and restore use this same verifier.

[Measured evidence](../performance/aq-p3-bootstrap.md) records paired complete
writable startup, hydration phases, section sizes, boundary/tail I/O, zero-replay
publication, and tails of 0, 20 and 200 events at 10,000 and 100,000 retained runs.
The gates are exactly 200 reducer applications and 202 frame reads for a 200-event
tail, median paired startup ratio at most 0.5 at each size, hydration growth no
greater than serialized byte growth, and the complete large checkpoint below
256 MiB. No timing tolerance or size-limit increase is implied.
