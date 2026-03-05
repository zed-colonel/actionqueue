# Data Directory Format

## Overview

The ActionQueue data directory stores all persistent state including the write-ahead log (WAL) and snapshots.

## Directory Structure

```
<data_dir>/
├── wal/
│   └── actionqueue.wal       # Write-ahead log (append-only)
└── snapshots/
    └── snapshot.bin      # Snapshot (atomic via .tmp rename)
```

## WAL v5 Record Format

Each WAL record uses the following binary framing:

| Field    | Size    | Encoding       | Description                              |
|----------|---------|----------------|------------------------------------------|
| version  | 4 bytes | LE u32         | WAL format version (currently 5)         |
| length   | 4 bytes | LE u32         | Byte length of the postcard payload      |
| crc32    | 4 bytes | LE u32         | CRC-32 checksum of the payload bytes     |
| payload  | N bytes | postcard binary | Serialized `WalEvent` (sequence + event) |

- **Path**: `<data_dir>/wal/actionqueue.wal`
- **Write mode**: Append-only, fsync after each durable write
- **Maximum payload size**: `u32::MAX` bytes (enforced by `PayloadTooLarge` guard)
- **Repair policy**: Trailing corruption (incomplete final record) is truncated on recovery
- **Event types**: 32 event variants covering task/run lifecycle, leases, dependencies, budgets, subscriptions, actor registration, and platform operations

## Snapshot Format (schema v8)

Each snapshot file uses the same framing envelope as the WAL:

| Field    | Size    | Encoding       | Description                              |
|----------|---------|----------------|------------------------------------------|
| version  | 4 bytes | LE u32         | Snapshot format version (currently 5)    |
| length   | 4 bytes | LE u32         | Byte length of the JSON payload          |
| crc32    | 4 bytes | LE u32         | CRC-32 checksum of the payload bytes     |
| payload  | N bytes | JSON (UTF-8)   | Serialized `Snapshot` struct             |

The snapshot schema version (currently 8) is recorded inside the JSON payload in the `metadata.schema_version` field, separate from the outer framing version.

- **Path**: `<data_dir>/snapshots/snapshot.bin`
- **Write mode**: Atomic via temp file + rename (write to `snapshot.bin.tmp`, rename on close)
- **Parent directory fsync**: Performed after rename to make directory entry durable
- **Drop safety**: Temp file removed on drop if `close()` was not called
- **Content**: Tasks, runs (with state history and attempt lineage), engine control, dependency declarations, budgets, subscriptions, actors, tenants, role assignments, capability grants, ledger entries

## Recovery Procedure

1. **Load snapshot** (if present): Read and validate `snapshots/snapshot.bin` CRC-32.
   If corrupt, discard snapshot and fall through to WAL-only replay.
2. **Replay WAL tail**: Read `wal/actionqueue.wal` from the sequence after the snapshot
   (or from the beginning if no snapshot). Validate each record's CRC-32.
3. **Truncate trailing corruption**: If the final WAL record has a partial header
   or payload (CRC mismatch), truncate at the last valid record boundary.
4. **Reconstruct projection**: Apply all valid WAL events to the in-memory reducer.
5. **Rebuild in-memory structures**: Reconstruct DependencyGate, HierarchyTracker,
   KeyGate, BudgetTracker, SubscriptionRegistry, ActorRegistry, HeartbeatMonitor,
   DepartmentRegistry, TenantRegistry, RbacEnforcer, and AppendLedger from the
   reducer's projection state.
6. **Re-open WAL writer**: Position the writer at the end of the validated WAL
   for subsequent appends.
