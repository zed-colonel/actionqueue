# actionqueue-core

Pure domain types and state machine for the ActionQueue durable task queue engine.

## Overview

This crate defines the fundamental types used throughout the ActionQueue system with no internal dependencies and no I/O:

- **ids** -- Strongly-typed identifiers (TaskId, RunId, AttemptId, ActorId, TenantId, etc.)
- **run** -- Run state machine with validated monotonic transitions
- **task** -- Task specifications, run policies, constraints, and metadata
- **mutation** -- Mutation authority boundary contracts
- **budget** -- Budget dimension and consumption types
- **subscription** -- Event subscription and filter types
- **actor** -- Remote actor registration and heartbeat policy types
- **platform** -- Multi-tenant roles, capabilities, and ledger entry types
- **event** -- System events for subscription matching
- **time** -- Clock trait for deterministic testing

## Run States

```
Scheduled -> Ready -> Leased -> Running -> Completed
                                       -> RetryWait -> Ready
                                       -> Suspended
                                       -> Failed
                                       -> Canceled
```

## Part of the ActionQueue workspace

See the [workspace root](https://github.com/zed-colonel/actionqueue) for full documentation.

## License

MIT
