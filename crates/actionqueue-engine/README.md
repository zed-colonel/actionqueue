# actionqueue-engine

Scheduling, derivation, and concurrency primitives for the ActionQueue task queue engine.

## Overview

This crate provides the scheduling engine:

- **derive** -- Run derivation from task specifications (Once, Repeat, Cron)
- **index** -- Run indexing by state (Scheduled, Ready, Running, Terminal)
- **selection** -- Priority + FIFO run selection for executor leasing
- **scheduler** -- State promotion (Scheduled to Ready, RetryWait to Ready)
- **lease** -- Lease ownership and expiry models
- **concurrency** -- Concurrency key gates for single-flight execution control
- **time** -- Clock trait abstraction for deterministic testing

## Part of the ActionQueue workspace

See the [workspace root](https://github.com/zed-colonel/actionqueue) for full documentation.

## License

Apache-2.0
