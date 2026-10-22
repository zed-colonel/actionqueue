# WAL recovery operator guide

Stop the process holding the store lock before offline work. Preserve the original
store and diagnostics when investigating corruption. Inspect, verify, backup and
upgrade take a shared source lock and return `StoreInUse` against a live daemon or
other writer. Use target-aware inspection:

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

A snapshot whose declared payload length differs from its actual file size is
physically damaged, even if the declared length exceeds the size ceiling; ordinary
startup falls back to the complete WAL. A complete frame above the size ceiling,
unsupported magic or an unsupported frame version halts startup as a compatibility
or bounds error. These cases do not permit automatic fallback.

A checkpoint that fails to decode or hydrate also halts startup. The error names
the file and the remedy: the WAL is complete, so stop the process, move the file
out of the store (keep it for diagnosis) and restart to recover from the WAL alone.
Runtime and daemon publication run the same load before installing a checkpoint,
so such a state fails its publication instead and keeps the previous checkpoint.

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

An incomplete final source frame, left by a crash during an append, is never
acknowledged. Upgrade leaves it out of the destination, as `TruncatePartial` repair
would, keeps it in the source and reports its length as `dropped_tail_bytes`. A CRC
failure or other damage inside a complete frame still fails the upgrade.

When the store's state exceeds the checkpoint bounds (a 256 MiB frame or 1,000,000
rows per collection), upgrade completes without a checkpoint: `checkpoint_omitted`
gives the reason and `snapshot_sequence` is 0. The destination is complete and
verified; its startup replays the complete WAL until a publication fits.

If upgrade reports a physically damaged schema-9 snapshot, preserve a copy of the
source store first. With the source offline, remove `snapshots/snapshot.bin` from
the upgrade source and retry `actionqueue store upgrade` to reconstruct from the
complete WAL. Upgrade still fully verifies that WAL and the staged destination;
it does not change the source on failure. Use this procedure only for physical
damage (for example, truncation or CRC failure), not semantic or compatibility errors.

Runtime and daemon publication count all durable progress since the last successful
checkpoint, including admissions between ticks and recovered tails. The daemon
uses 10,000 events by default; set `snapshot_event_threshold`, or use
`--snapshot-event-threshold N` / `--no-snapshots` on the CLI. One publication runs at
a time and shutdown awaits it. Snapshot-only failure leaves the threshold pending.
Runtime and daemon retries wait 1 second after the first failure, double up to 60
seconds, and reset after successful publication. Only failed attempts log errors;
ticks and maintenance continue during the cooldown. WAL uncertainty fences the
authority.
