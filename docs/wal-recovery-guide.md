# WAL recovery operator guide

Stop the process holding the store lock before offline work. Preserve the original
store and diagnostics when investigating corruption. Use target-aware inspection:

```sh
actionqueue store inspect --data-dir /srv/actionqueue
```

`Strict` is the default repair policy. `TruncatePartial` permits only an incomplete
final target frame after a validated semantic prefix. A complete header must pass
format, identity, kind/schema, bounds, sequence and integrity checks before a short
payload qualifies. CRC failures, unknown records, gaps and interior damage halt
recovery. Recovery never scans forward to resynchronize past damage. Do not repair
these errors with byte editing or treat them as ordinary interrupted appends.

The [storage API](../crates/actionqueue-storage/README.md) owns opening, repair,
backup and restore. `runtime::store::{inspect_store,backup,restore}` expose verified
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

No release migration reader accepts 0.1.x or earlier development schemas. Consult
[release limits](releases/0.2.0.md) before cutover.
