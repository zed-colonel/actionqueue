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

### Durable signal ingress

`BootstrappedEngine::admit_signal(request, ingress)` and the handler-independent
`signals::admit_signal(authority, request, ingress, clock)` return `Admitted` or
`AlreadyExists` with the original global signal sequence. A changed producer field
under the same tenant/id returns `SignalRejection::Conflict`. Exact retries preserve
receipt time and attribution even after retirement or lowered creation limits.

Construct producer content with `AdmitSignalRequest::new`; provide authenticated
host scope and attribution separately through `SignalIngressContext`. The host
performs authentication/authorization; references themselves confer no permission.
Payload hashes are verified/normalized without fetching external data. Signal
ingress works without the budget subscription registry.

`get_signal`, `list_signals`, `pin_signal`, `unpin_signal`, `retire_signals` and
`signal_statistics` expose inspection and explicit retention. `RuntimeConfig`
contains `signal_limits` and `signal_retention`. Defaults: 100,000 identities,
16 MiB of framed immutable records, seven-day minimum receipt age and a 10,000
sequence window. Retirement is never automatic and does not reclaim immutable
record storage or WAL bytes. Capacity errors require operator/application action.

Storage uncertainty is distinct from definitive rejection. Reopen/recover a fenced
authority before retrying. Wait establishment, wake promotion and daemon/CLI signal
endpoints are deferred to their designated work items.

AQ-06 adds the handler-independent `waits` service and pre-dispatch recovery
reconciliation. See [continuation semantics](../../docs/aq-06-continuations.md), including
the pending-input guard until accepted-start delivery lands.
