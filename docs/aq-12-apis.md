# AQ-12 operational API

The runtime owns authorization, admission and inspection semantics. HTTP v2 and
`actionqueue` use those services. API v1 and the `submit`, `stats`, and `storage`
CLI commands have been removed. The remote executor result protocol version is
independent of the HTTP URL version.

## Modes and parity

| Operation | Embedded | Authenticated HTTP | CLI |
|---|---|---|---|
| Ensure task, admit signal | `control`, typed convenience methods | `/api/v2/admissions:ensure`, `/api/v2/signals` | `ensure-task`, `signal admit` |
| Admission/task/run/attempt/wait/signal/checkpoint inspection | `Inspector`, engine getters | GET resource routes under `/api/v2` | `admission inspect`, `task inspect`, `run inspect`, `attempt inspect --run RUN`, `wait inspect`, `signal inspect`, `checkpoint inspect` |
| Run history and attempts | `Inspector::{run_history,list_attempts}` | GET `/api/v2/runs/{id}/history`, `/api/v2/runs/{id}/attempts` | `run history`, `run attempts` |
| Structural trace | `Inspector::trace` | `/api/v2/traces/{id}`, `/api/v2/inspect` | `trace ID`, `trace --correlation ID`, `inspect --origin-ref REF` |
| Task/run/wait cancellation and wait resolution | `control`, engine convenience methods | POST resource `{id}:cancel` or `{id}:resolve` | `task cancel`, `run cancel`, `wait cancel`, `wait resolve` |
| Store inspection | `runtime::store::inspect_store` | Offline only | `store inspect --data-dir PATH` |
| Backup and restore | `runtime::store::{backup,restore}` | Offline only | `backup --data-dir PATH --output PATH`, `restore --input PATH --data-dir PATH` |

CLI operational commands require either `--offline --data-dir PATH` or
`--daemon http://127.0.0.1:PORT`. The local HTTP client obtains a bearer credential
from `--token-file PATH` or `ACTIONQUEUE_TOKEN`; credentials are never rendered.
Offline mode holds the storage lock, supplies an explicit `SingleTenant` host
boundary, and cannot manufacture platform scope from JSON. It does not run a
scheduler or close interrupted attempts. A running daemon owns its store for its
entire lifetime, including during graceful SIGINT/SIGTERM shutdown. The daemon serves its configured metrics listener alongside the API listener. Client operations have a 30-second timeout; a timed-out mutation may have committed, so retain admission identities for retry.

Backup and restore remain storage-owned offline operations. They verify target
formats and digests, enforce exclusive ownership, and reject populated or
unsafe destinations. No endpoint accepts a server filesystem path.

## Authentication and mutation results

Object inspection requires a configured host authenticator for every store
profile. Health, readiness, aggregate statistics and aggregate metrics are
separate. Mutations also require control enablement. Library bootstrap rejects
control enablement without a host authenticator. `HostControlContext` cannot be
deserialized from request JSON. Current durable permissions are checked before
an idempotent acknowledgement, and the host replaces request control attribution.

Task and signal creates return 201; exact duplicates return 200; conflicts return
409. Invalid JSON is 400, structural rejection is 422, missing authentication is
401, permission denial is 403, absent or out-of-scope lookup is 404, oversized
bodies are 413, repeated admission conflicts are 429, and storage uncertainty/backpressure is 503. Service errors use
fixed `error_code` values, never display-string classification. A signal that
committed before matching failed returns 503 with `committed` and
`recovery_required: true`. Retrying its identity after recovery is safe.

Wait control bodies contain `{"run_id":"UUID"}`. Admission keys use the query
parameter `GET /api/v2/admissions?key=...`; opaque values must be URL encoded.
Mutation request bodies are limited to 2 MiB. A bounded admission lane rejects
excess concurrent work instead of queuing unbounded blocking work.

## Inspection schema and disclosure

`TaskView` includes immutable admission facts, ordinary constraints, priority,
run policy, budgets and structural parent/dependency links. `RunView` includes
physical/failure attempt counts, schedule, lease, active/last wait, pending resume
assignment, dispatch blockers and paged attempts/state history. Executor matching
is explicitly unevaluated when no executor traits are supplied. Dispatch and
inspection share the same gate calculation.

Attempt views preserve accepted-start sequences, finish origin, original resume
identity, previous physical attempt (also before any wait), checkpoint production and causal child and
signal links. Waits expose structural filters/child targets, eligibility cursor,
deadline policy and recorded resolution. Checkpoints and signals expose data
summaries. Pending and consumed resume views include typed wake reasons and recursively redacted data summaries. Signal-linked waits and checkpoint consumers have separate pages at `/signals/{id}/waits` and `/checkpoints/{id}/consumers`. Trace nodes and typed edges connect these facts. Control attribution
is joined by recorded WAL sequences; task cancellation targets come from the
retained WAL records, never matching timestamps. Task/run control histories are paginated at `/tasks/{id}/controls` and `/runs/{id}/controls`. Snapshot hydration restores operation-target and attempt-owner indexes from the retained complete WAL. Standalone snapshot history explicitly reports `available: false`. Missing control history
is represented by null, not fabricated events or durable retry counters.

A data summary contains representation, content hash, content type and size.
Inline bytes and external locators are never inspection output, including through
resume contexts. Free-text handler errors remain redacted even when reference
disclosure is allowed. Metadata descriptions and tags are not inspection output.
References distinguish `absent`, `redacted` and `disclosed`. Reference disclosure
requires both the request flag and a trusted `DisclosurePolicy`; the default
policy denies disclosure. Payload retrieval is outside this API.

Trace JSON and CLI rendering include:

> Opaque references are attribution only. Queue outcomes describe execution; application-level judgments belong to the caller.

Exact trace/correlation/origin filters do no parsing, normalization or
fetching. Origin selection uses immutable admission context and follows child
links. Correlation inspection includes unmatched signals. Tenant-local indexes
are derived during both replay and snapshot hydration. Each linked resource is
checked under the same projection revision. Full traces require task, wait and
signal inspection grants.

Unfiltered wait listing requires `InspectWait` and is scoped to the caller's tenant.
Filtering waits through task admission references additionally requires `InspectTask`,
as does inspecting child-task targets. Individual signal-wait lookup requires the
same `InspectWait` permission as unfiltered listing.

## Bounds and cursors

Pages default to 100 entries and accept limits from 1 through 1,000. Each page is
limited to 1 MiB of serialized entries. Trace expansion is limited to 10,000 nodes;
queries exceeding that budget return a bounded-capacity error. Narrow the exact
filter for larger stores. Task/run histories use separate pages rather than
unbounded inline arrays. Run history and attempt pages can be continued via
`/api/v2/runs/{id}/history` and `/api/v2/runs/{id}/attempts`.

Run history and attempt pages are available through `actionqueue run history RUN_ID`
and `actionqueue run attempts RUN_ID`, with `--limit` and `--cursor` in daemon and
offline modes. `run inspect` includes first pages and rejects pagination flags.
Only trace commands accept `--edge-cursor`.

Cursors bind the lane, filter, scope, disclosure settings and projection revision.
Mutation invalidates an old cursor with `stale_cursor` (409). Trace nodes use
`cursor`; trace edges use `edge_cursor`. No total count reveals objects outside
the authorized namespace. Single-object responses also have a 2 MiB cap. `different_fields` reports ordinary unequal scheduling
fields (priority, constraints, run policy, budgets, run schedules and wait deadlines) across tasks on the returned node page; it never chooses a preferred task.

## Readiness and telemetry

Continuation maintenance runs without the actor feature. Actor lease maintenance
remains feature-gated. Reconciliation failures, poisoned authority/projection
locks and uncertain writes make readiness return 503. Durable progress is
published even when later matching fails. A fenced authority must be reopened
through recovery before further mutation. Embedded daemon hosts drain their HTTP requests and await `BootstrapState::shutdown()` before reopening the store; the CLI does this automatically.

Counters are process-lifetime observations, not durable historical totals. They
start at zero on reopening; replay does not count old admissions as new ones.
The authority records compound-child admissions, signal admissions, duplicate
lookups, conflicts, wait resolutions, disposition rejections and recovery
closures. Current-state gauges are derived from one coherent projection snapshot.
Scrapes never create latency or record-size observations. Wait latency only
observes establishments and resolutions seen by that telemetry instance.

Task and run cancellation count each wait resolved by the committed compound record.
Latency is observed only for waits established during the current process lifetime;
resolution drops that wait's pending timing entry. Retried cancellations and repeated
metrics scrapes do not add observations.

Signal namespace/kind labels default to `overflow/overflow`.
`DaemonConfig::signal_metric_allowlist` and `QueueTelemetry::set_signal_allowlist` accept at most 64 validated pairs before
observation begins; all other pairs remain in the single overflow bucket. No
identity, opaque causal/control reference, payload, external locator or arbitrary
error becomes a metric label. Histogram observations are accumulated once at
commit, with cumulative count/sum and the positive-infinity bucket.

The storage crate requires its `serde` feature for the target persistence format. A minimal storage build uses `--no-default-features --features serde`; runtime and daemon can be checked with `--no-default-features`.

### Admission conflict throttling

The daemon permits eight known changed-digest admission attempts per 30-second
window for each authenticated `(tenant, actor)` scope. A single-tenant host without
actor identities shares one scope. The window uses monotonic process time and
starts at the first conflict. Further conflicts return HTTP 429 with
`{"error_code":"admission_conflict_throttled"}` and `Retry-After: 30`, without
entering the mutation lane or changing the WAL/projection. New admission keys and
exact idempotent retries remain available and undergo normal authorization.
Changing request IDs, caller attribution, admission keys or rejected content does
not reset a scope's budget. Scope comes from the host, never the payload.

Limiter state is process-local and resets on daemon restart. At most 1,024 live
scope budgets are retained; when full, known conflicts from additional scopes are
throttled until a budget expires. Existing scopes remain isolated. This protects
the host boundary; embedded execution retains the ordinary admission contract.

### Broad matching telemetry

A broad signal wait omits both exact correlation and exact source. It still needs
an explicit lower-bound cursor. `actionqueue_waits_broad_active` reports the current
number, including waits restored by replay. `actionqueue_waits_broad_established_total`
counts live committed establishments in this authority's lifetime.
`actionqueue_signal_match_candidates_total{direction="waits"|"signals"}` counts
actual indexed candidate visits while preparing live mutations: waiter visits for
incoming signals and retained-signal visits for wait matching. Repeated preparation
work counts as work, even when a later append fails. These are work measurements,
not distinct signals, successful wakeups, or durable history totals. Empty index
lookups contribute zero. Replay, snapshot hydration, inspection and metrics scrapes
do not increment these counters. All labels are fixed and contain no identifiers.
