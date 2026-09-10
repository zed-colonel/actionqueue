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

## Idempotent admission

`BootstrappedEngine::ensure_task` and `DispatchLoop::ensure_task` accept an
`EnsureTaskRequest` and return Created or AlreadyExists with the original task UUID,
canonical digest, admission key, and WAL sequence. `submit_task(spec)` derives stable
`task/<uuid>` key, trace, and correlation values and delegates to this operation. Retain
the UUID on retries. Outbox callers should persist their own stable key, task UUID,
and causal context. Changed digest-bearing meaning returns a typed rejection conflict.

All ordinary submissions, including CLI and consumed workflow channel messages, use
the handler-independent admission service. The workflow channel itself still gives no
durable enqueue acknowledgement; atomic disposition/child admission belongs to later
work items. Attribution is opaque and confers no scheduling priority or authority.

RuntimeConfig.admission_limits can lower creation bounds. A retry uses the original
admission even after limits are lowered or tasks/parents finish. Storage failures may
require dropping and reopening the engine before retry; a fenced authority cannot
return cached duplicate success. See ADR-002 for exact digest fields and encoding.
