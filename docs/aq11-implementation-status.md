# AQ-11 implementation and remediation

AQ-11 uses one storage control boundary for embedded, daemon, CLI, and direct
Rust callers. Host identity, explicit scope, current queue permissions, target
namespace, and embedded attribution consistency are checked before duplicate
acknowledgement or append. Routing traits and opaque causal references grant no
permission. Controls without an authenticated host binding fail closed.

## Host integration

Use `runtime::control::execute_control` for admission, signal admission, task/run
cancellation, and wait resolution/cancellation. `execute_mutation` handles the
administrative command vocabulary, including signal pin/unpin/retirement.
`inspect_wait` and `inspect_signal` authorize current grants. Embedded convenience
methods require `with_host` or `set_control_context`; the authority also supports
scoped `with_control_context` calls. Temporary bindings are restored after errors
and unwinding. The host constructs `HostControlContext`; request JSON cannot.

Platform stores require a named `Tenant` scope, an active actor, a role, and the
specific current `QueueAction` grant. `Store` scope covers store administration.
A separate host-authorized `ProvisionTenant(id)` scope permits actor registration
in exactly one existing tenant. It grants no inspection, dispatch, or result
permission. This permits fresh tenant provisioning entirely through the control
boundary, before the first actor has a role or grant.

New local stores default to compiled capabilities excluding `platform`. Hosts
opt into platform tenancy through `RuntimeConfig::store_features` or storage's
`load_projection_with_features`/`OpenOptions::Initialize`. Existing store
manifests remain authoritative regardless of binary features.

Daemon hosts pass an `Authenticator` to `bootstrap_with_authenticator`. The CLI
requires `daemon --enable-control --auth-file PATH`. The file contains a JSON
array of trusted identities, each with `token`, `actor_id`, `scope`, and
`attribution` fields. Tokens must be unique, at least 32 printable ASCII bytes;
requests send `Authorization: Bearer <token>`. Scope and attribution are operator
configuration, never accepted from request bodies. Missing or invalid
configuration is rejected. CLI task submission binds a trusted local CLI caller
to the explicit single namespace; it cannot infer a tenant from submitted data.

Task/run HTTP inspection uses authentication even when controls are disabled,
authorizes against the same current projection used to construct responses, and
filters tenant scope before lookup and pagination. A platform store without a
host hook rejects inspection. Legacy anonymous inspection is restricted to the
single namespace of a non-platform store. Mutating routes remain gated by
`enable_control` and authentication.

## Execution and recovery

Local and remote eligibility share FIFO selection, executor trait matching,
dependency, budget, pause, namespace, and concurrency-key gates. Remote claims
and results use the same accepted-start and atomic disposition paths as embedded
execution. Embedded runtime exposes claimable, claim, renewal, and result methods.
The daemon serializes remote ingress and its 100 ms maintenance timer under one
mutation owner. `DaemonConfig::remote_policy` controls capacity, lease duration,
and retry delay. Maintenance handles actor liveness, partial/expired executions,
retry promotion, rolling cron windows, cancellations, durable waits, and secondary subscription
reconciliation after remote results or crashes. Internal subscription triggers
can make Scheduled runs eligible early; they cannot resolve Awaiting/Suspended
continuations.

Execution commands retain their typed attempt/lease/scheduler preconditions.
`RecoveryControl` permits only controls justified by durable antecedents:
heartbeat timeout, cancellation propagation, and completion of a legacy suspended
attempt after its durable finish and lease release. It cannot supply an anonymous
administrative wildcard. Administrative suspension becomes a fenced suspension
disposition at storage, closing the attempt and lease together.

## Persistence and limits

Every host control has attribution in the same WAL frame as its mutation.
Kind 352/schema 2 encodes the bounded attribution plus the original binary inner
frame, avoiding JSON inflation of binary payloads. The reader still supports
schema 1 and rejects nested envelopes or mismatched store/sequence identities.
Snapshot/projection version 9 retains attribution history and the first matching
WAL sequence for each reactive subscription. Version 8 images lack this ordering
proof and are deliberately rejected; no in-place migration is provided.
Subscription matches are recorded during ordered replay, then reconciled by both
embedded ticks and daemon maintenance. Equal timestamps cannot create retrospective
matches or suppress valid matches. The independent projection vector is version 9.

The authority validates complete encoded frames against configured and hard
limits before append. Oversize proposals are definitive rejections and do not
fence the store. Signal quota accounting includes the immutable attribution
envelope both live and after snapshot hydration.

## Review evidence

- F-001: central mandatory controls, current authorization before duplicates,
  consistency rejection, host-bound convenience paths, recovery antecedents,
  and fenced suspension; generic cancel/suspend/resume transitions normalize into
  those same operations. Tests reject unbound, missing-principal, cross-tenant,
  revoked and falsely recovery-labeled requests before append;
  `acceptance_control_mutation_attribution`.
- F-002: configured serialized remote scheduler and timer; HTTP capacity, retry,
  expiry, renewal, idle deadline/liveness tests and subscription crash repair.
- F-003: HTTP anonymous/missing-scope/cross-tenant inspection and revocation
  against an intentionally stale read projection; `http::tenant_tests`.
- F-004: wait and signal inspection, wait cancellation, retention, embedded
  claimable/renewal, operation attribution and all tenant permission checks.
- F-005: CLI bootstrap tests cover absent/unreadable/malformed and valid host
  configuration; bearer tests cover successful and failed authentication.
- F-006: fresh tenants and actors are provisioned only through supported
  attributed controls in the platform and HTTP tests.
- F-007: complete-frame boundary rejection, usable authority after rejection,
  and signal accounting/recovery tests.
- F-008: all partial remote claim prefixes, competing local/remote claims,
  renewal/result/cancellation orderings, platform HTTP remote revocation,
  compound-effect envelope rejection, ordinary/developmental cancellation,
  operation-wide attribution, and independent canonical vectors. The independent
  Python generator covers checkpoint, wait, child admission, emitted signal,
  all consumption dimensions, and a separate legal completion with output.

The original trait matching, actor replacement, remote FIFO, and versioned
snapshot/conformance regression tests remain in the feature matrix (F-009–F-014).

## Follow-up review remediation

- F-008: platform tests invoke actual wait inspection/resolution/cancellation,
  signal inspection/pin/unpin/retirement, task/run cancellation, budget controls,
  subscription controls, actor controls, and ledger append. Missing principals,
  valid cross-tenant principals, and revoked permissions leave state unchanged;
  mutations retain exact host attribution through WAL and snapshot replay.
  Remote claim/renew/result and recovered result retries also reject cross-tenant
  principals. HTTP inspection and effect-permission suites remain in the matrix.
- F-015: daemon and embedded cron replenishment use one implementation. Remote
  tests complete eight occurrences of bounded and unbounded policies, restart at
  the original window boundary, and verify cancellation after restart. HTTP tests
  independently execute both policies past five occurrences.
- F-016: equal-timestamp tests exercise both registration/event orderings, WAL
  replay, snapshot recovery, actual embedded ticks, and daemon maintenance.
