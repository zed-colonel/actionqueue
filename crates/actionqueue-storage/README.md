# actionqueue-storage

WAL, snapshot, and recovery storage layer for the ActionQueue task queue engine.

## Overview

This crate provides the persistence layer for ActionQueue:

- **wal** -- Write-Ahead Log (v5, postcard + CRC-32) for durable event persistence
- **snapshot** -- State snapshots (schema v8, JSON) for recovery acceleration
- **recovery** -- Deterministic WAL replay and state reconstruction
- **mutation** -- Storage-owned WAL-first mutation authority

The WAL is the source of truth. Snapshots are derived acceleration artifacts. Recovery is deterministic from snapshot + WAL tail, or WAL alone.

## Part of the ActionQueue workspace

See the [workspace root](https://github.com/zed-colonel/actionqueue) for full documentation.

## License

Apache-2.0
