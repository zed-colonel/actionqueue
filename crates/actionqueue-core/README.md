# actionqueue-core

Release 0.2.0 implements AQ-CONT-1. See the [release and compatibility notes](../../docs/releases/0.2.0.md).

Pure domain types and state machine for the ActionQueue durable task queue engine.

## Overview

This crate defines the fundamental types used throughout the ActionQueue system with no internal dependencies and no I/O:

- **ids** -- UUID identities and bounded caller-supplied admission, signal, trace and correlation identifiers
- **run** -- Run state machine with validated transitions and typed rejection reasons
- **task** -- Task specifications, run policies, constraints, and metadata
- **mutation** -- Mutation authority boundary contracts and compound commands
- **bounded / limits** -- Validated opaque values and hard byte/count ceilings
- **executor** -- Canonical executor routing traits, separate from RBAC
- **causal** -- Immutable bounded attribution; references grant no authority
- **data_ref / continuation** -- Data, signals, exact filters, waits, checkpoints and resume context
- **admission / disposition** -- Validated admission plans and attempt effect combinations
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
                                       -> Suspended -> Ready
                                       -> Awaiting -> Ready / Failed / Canceled
                                       -> Failed
                                       -> Canceled
```

## AQ-CONT-1 integration

`Awaiting` is nonterminal and is established by a compound attempt disposition
that finishes the physical attempt and durably records its wait/checkpoint.
Generic state transitions cannot bypass this boundary. A matching signal, deadline
or explicit host control resolves the wait and records resume input. The default
concurrency-key wait policy releases the key; hold-until-terminal is explicit.

Caller references are attribution only. Core validates bounded data and computes
canonical admission/content hashes; it never dereferences application locators.
Storage owns commit ordering, durable effects and recovery. Queue outcomes express
execution results and do not establish application correctness.

## Part of the ActionQueue workspace

See the [workspace root](https://github.com/zed-colonel/actionqueue) for full documentation.

## License

Apache-2.0
