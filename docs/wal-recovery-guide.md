# WAL Recovery Operator Guide

## Overview

The ActionQueue WAL (Write-Ahead Log) uses a binary format (postcard + CRC-32) to durably record every state change. On crash or unclean shutdown, partial writes may leave incomplete records at the tail of the WAL file.

## What Are Partial Writes?

A partial write occurs when the process crashes mid-way through appending a WAL record. This can leave:

- **Incomplete header**: Less than 12 bytes of the record header were written
- **Incomplete payload**: The header was written but the payload is truncated
- **CRC mismatch**: All bytes were written but data was corrupted (e.g., hardware fault)

Partial writes only occur at the **tail** of the WAL -- records before the crash point are guaranteed complete and checksummed.

## Detection

On startup, the WAL writer validates all existing records. Corruption is reported as a `WalCorruption` with:
- `offset`: Byte position of the corrupt record
- `reason`: One of `IncompleteHeader`, `IncompletePayload`, `CrcMismatch`, `UnsupportedVersion`, `DecodeFailure`

## RepairPolicy

The `RepairPolicy` enum controls how the writer handles trailing corruption:

### `RepairPolicy::Strict` (default)

Hard-fails on any corruption. The writer will not open if any record is incomplete or invalid. This is the safest option and requires manual intervention to resolve corruption.

### `RepairPolicy::TruncatePartial`

Automatically truncates the incomplete trailing record and opens normally. Only the last incomplete record is removed -- all prior complete records are preserved.

**What data is lost**: Only the single incomplete trailing record. This is the record that was being written when the crash occurred, so it was never confirmed durable.

## When to Use Each Policy

| Scenario | Recommended Policy |
|----------|-------------------|
| Production with strict durability requirements | `Strict` |
| Development and testing | `TruncatePartial` |
| Recovery after known crash | `TruncatePartial` |
| Corruption in the middle of the WAL | Neither -- requires manual investigation |

## Programmatic Recovery Example

```rust
use actionqueue_storage::wal::fs_writer::WalFsWriter;
use actionqueue_storage::wal::repair::RepairPolicy;

// Attempt strict open first
let path = std::path::PathBuf::from("data/wal/actionqueue.wal");
match WalFsWriter::new(path.clone()) {
    Ok(writer) => { /* clean WAL, proceed normally */ }
    Err(e) => {
        eprintln!("Strict validation failed: {e}");
        // Fall back to truncation repair
        let writer = WalFsWriter::new_with_repair(path, RepairPolicy::TruncatePartial)
            .expect("repair should succeed for trailing corruption");
        // Proceed with repaired WAL
    }
}
```

## WAL Format (v5)

Each record has a 12-byte header:

```
+-------------+-------------+------------+------------------------+
| Version (4B)| Length (4B) | CRC-32 (4B)| Serialized Event (N B) |
+-------------+-------------+------------+------------------------+
```

- **Version**: `5` (little-endian u32)
- **Length**: Payload byte count (little-endian u32)
- **CRC-32**: Checksum of the payload (little-endian u32)
- **Payload**: postcard-serialized `WalEvent`

### WAL Event Types (v5)

The WAL records 32 event types across five categories:

**Core lifecycle**: TaskCreated, RunCreated, RunStateChanged, AttemptStarted, AttemptFinished, TaskCanceled, RunCanceled, LeaseAcquired, LeaseHeartbeat, LeaseExpired, LeaseReleased, EnginePaused, EngineResumed

**Workflow**: DependencyDeclared

**Budget/Subscription**: RunSuspended, RunResumed, BudgetAllocated, BudgetConsumed, BudgetExhausted (reserved), BudgetReplenished, SubscriptionCreated, SubscriptionTriggered, SubscriptionCanceled

**Actor**: ActorRegistered, ActorDeregistered, ActorHeartbeat

**Platform**: TenantCreated, RoleAssigned, CapabilityGranted, CapabilityRevoked, LedgerEntryAppended
