# actionqueue-runtime

Async dispatch loop, embedded API, and runtime configuration for the ActionQueue task queue engine.

## Overview

This crate composes storage, engine, and executor primitives into a cohesive runtime:

- **DispatchLoop** -- Full task lifecycle: promote, select, gate, lease, execute, finish, release
- **ActionQueueEngine / BootstrappedEngine** -- Primary entry point for embedding ActionQueue as a library
- **RuntimeConfig** -- Backoff, concurrency, lease, and snapshot configuration

The dispatch loop is async (tokio). Handlers run via `spawn_blocking`. The dispatch loop owns all WAL mutation authority exclusively.

## Part of the ActionQueue workspace

See the [workspace root](https://github.com/zed-colonel/actionqueue) for full documentation.

## License

Apache-2.0
