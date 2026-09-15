# actionqueue-engine

Release 0.2.0 implements AQ-CONT-1. See the [release and compatibility notes](../../docs/releases/0.2.0.md).

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

## Internal reactivity

`reactivity::InternalSubscriptionRegistry` matches core structural events (task
completion, run state changes, and budget thresholds). Runtime triggers promote
Scheduled runs only; they never resolve Awaiting or resume Suspended runs.
Subscription records and `triggered_at` persist as inspection evidence separately
from signal history. Notification follows the originating mutation, and clearing
triggers rearms only memory; this is not a durable recurring wake protocol.
External wake-ups use identified, idempotently admitted signals and durable waits.
