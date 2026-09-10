# actionqueue-core

Pure domain types and state machine for the ActionQueue durable task queue engine.

## Overview

This crate defines the fundamental types used throughout the ActionQueue system with no internal dependencies and no I/O:

- **ids** -- UUID identities and bounded caller-supplied admission, signal, trace and correlation identifiers
- **run** -- Run state machine with validated transitions and typed rejection reasons
- **task** -- Task specifications, run policies, constraints, and metadata
- **mutation** -- Mutation authority boundary contracts and provisional compound command shapes
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

## Part of the ActionQueue workspace

See the [workspace root](https://github.com/zed-colonel/actionqueue) for full documentation.

## License

Apache-2.0

`Awaiting` is non-terminal and may originate only from `Running`, after the active
attempt is finished. Generic mutation commands reject Awaiting transitions until
AQ-06 supplies the compound continuation record. Existing WAL-embedded layouts
remain unchanged in AQ-02; the new state and attempt result are appended variants.
`ConcurrencyKeyWaitPolicy` defaults to release; its constraints field arrives in AQ-03.

Caller references are attribution only. Core neither dereferences them nor computes
content hashes. Canonical admission hashing is an AQ-04 obligation; inline hash
verification is an AQ-07 obligation. New command types are not yet members of
`MutationCommand`.
