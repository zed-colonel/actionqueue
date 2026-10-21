# WAL recovery operator guide

Stop the process holding the store lock before offline work. Preserve the original
store and diagnostics when investigating corruption. Use target-aware inspection:

```sh
actionqueue store inspect --data-dir /srv/actionqueue
actionqueue store verify --data-dir /srv/actionqueue --json
```

Ordinary startup checks the manifest, snapshot integrity and invariants, sequence 1,
and the last covered frame, then hydrates state/operation-history indexes and seeks
to the WAL tail. It does not recompute the skipped-prefix commitment: changes wholly
inside that prefix can remain undetected when the boundary is unchanged. Full
`store verify` reads every frame and compares the checkpoint, prefix commitment,
inspection associations and final projection. It never initializes, repairs or
reconciles execution. The commitment is not authentication.

`Strict` is the default repair policy. `TruncatePartial` permits only an incomplete
final target frame after complete candidate-prefix validation, including checkpoint
equivalence when present. A complete header must pass
format, identity, kind/schema, bounds, sequence and integrity checks before a short
payload qualifies. CRC failures, unknown records, gaps and interior damage halt
recovery. Recovery never scans forward to resynchronize past damage. Do not repair
these errors with byte editing or treat them as ordinary interrupted appends.

The [storage API](../crates/actionqueue-storage/README.md) owns opening, repair,
backup, restore, upgrade and full verification. `runtime::store` exposes these
offline operations. The operational CLI does not expose an arbitrary WAL repair
command. The complete WAL must remain present, even with a snapshot.

After uncertain writes the mutation authority is fenced. Drop the engine/store
owner and reopen through recovery before further mutation or cached duplicate
acknowledgement. Recoverable interrupted attempts are closed by runtime recovery;
offline inspection and backup do not invoke handlers or reconcile execution.

```sh
actionqueue backup --data-dir /srv/actionqueue --output /srv/backups/queue-001
actionqueue restore --input /srv/backups/queue-001 --data-dir /srv/restored-queue
```

Backup verifies the complete WAL, a usable snapshot, checksums and projection
identity before publication. Restore rejects populated/unsafe destinations,
symlinks, traversal, overlap, unexpected entries and mismatched digests. Restore
preserves store identity and profile. Application-owned external artifact bytes
require separate retention and verification.

Released schema-9 stores require an explicit offline, source-preserving upgrade:

```sh
actionqueue store upgrade --data-dir /srv/schema9 --output /srv/schema10
```

The destination must be absent or empty and separate from the source. The upgrade
fully verifies both sides and preserves store identity, feature profile, creation
fields, domain history, projection digest and operation history. Sequence 1 receives
the destination manifest binding; later WAL frames are unchanged. Writable opening
and restore do not silently migrate. Upgrade a schema-9 backup to a separate store,
then produce a new schema-10 backup descriptor. There is no downgrade. Pre-contract
and earlier development schemas remain unsupported. See
[ADR-020](adrs/AQ-ADR-020-bounded-snapshot-bootstrap.md).

Runtime and daemon publication count all durable progress since the last successful
checkpoint, including admissions between ticks and recovered tails. The daemon
uses 10,000 events by default; set `snapshot_event_threshold`, or use
`--snapshot-event-threshold N` / `--no-snapshots` on the CLI. One publication runs at
a time and shutdown awaits it. Snapshot-only failure leaves the threshold pending;
WAL uncertainty fences the authority.
