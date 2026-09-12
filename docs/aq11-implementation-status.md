# AQ-11 implementation status

This branch contains an incomplete implementation of the accepted AQ-11 design.
Passing the added tests alone does not close the work item's exit gate.

## Implemented

- Explicit local executor traits and a shared exact-subset routing/eligibility
  predicate; local and remote execution use the same four-record lease/start path.
- Versioned remote claim/result envelopes, stable actor-ID lease owners, fenced
  lease renewal, canonical tagged SHA-256 disposition encoding, and an independent
  Python known-answer vector.
- Strict remote result processing and independent storage checks for active actor,
  tenant, permissions, revision, digest, and accepted fence. Invalid remote results
  do not enter the local handler-proposal terminal-failure fallback.
- Exact accepted claim/result retries; result identity is reconstructed from the
  immutable stored disposition/fence rather than stored in a second audit record.
- Remote checkpoint/resume assignment and WAL/snapshot recovery tests.
- Typed queue-action permissions and a host context that cannot be deserialized.
  The shared services authorize from current durable grants before duplicate
  acknowledgement and enforce exact scopes. Custom capabilities and role names
  do not imply new queue-action grants.
- The control service supports attributed admission, signal admission,
  cancellation, explicit wait resolution, and administrative mutation commands.
  Administrative suspension through this service commits a suspension disposition
  that closes the execution fence. Administrative resumption preserves attribution.
- WAL kind 352/schema 1 wraps an underlying mutation and host attribution in one
  frame. Snapshot/projection version 8 preserves the context history. Version 7
  stores require an explicit future migration; they cannot be opened for mutation.
- A configurable host authentication hook for daemon control, actor and platform
  routes. Missing hooks fail closed; disabled controls leave these routes absent.
  Actor claim/result/renewal routes and durable read-projection synchronization.
- Tenant-immutable actor registration, replacement-index cleanup, deterministic
  timeout order, and department-index synchronization after successful mutation.

## Remaining work before integration

1. Migrate all older runtime and direct mutation entry points. They still permit
   context-free control commands and can bypass the new service's authorization.
   In particular, raw admission/signal/cancellation and old administrative,
   actor/platform, budget and subscription entry points need mandatory attribution
   and explicit internal-operation provenance. Raw `RunSuspend` still differs
   from the safe administrative service path.
2. Complete the service's inspection and signal-retention APIs, wait cancellation,
   and CLI host-context plumbing. Existing general inspection routes have not
   been converted to tenant-authenticated inspection.
3. Complete remote scheduler integration: the standalone daemon service handles
   Scheduled/Ready work, while retry promotion still belongs to the dispatch
   loop. The daemon claim route currently uses a 300-second lease and lacks
   host-configured dispatch capacity; embedded claims do enforce worker slots and
   live-worker key reservations. Remote liveness/recovery requires further daemon
   integration and configuration. Embedded renewal/claimable convenience methods
   are also missing.
4. Extend failure/race coverage to every partial remote claim boundary,
   simultaneous local/remote claims, renewal races, complete remote tenant and
   grant-revocation cases, and every administrative control operation. Add an
   independently generated compound disposition vector covering all effect types.
5. Audit attributed-frame configured size ceilings and consistency between
   per-operation control fields and the outer attribution envelope. Existing
   operation-specific creation limits are checked before the generic envelope is
   attached; this requires consolidation at storage preparation.

## Design choices

Accepted-result deduplication uses the already durable, complete disposition and
fence. This satisfies the required durable identity without adding another record
or changing the disposition record shape. The new attribution envelope is a
separate WAL record kind containing the mutation itself, not a separate audit
append. These are implementation choices within the accepted atomicity design.
The remaining items above are unfinished scope, not claimed design exceptions.

## Verification

Focused remote protocol, host attribution, routing, budget-continuation and
snapshot/replay checks pass. The final operator handoff reports the complete
requested command matrix and its scratch log location. ADR-016 explicitly retains
its outstanding context-free-entry-point verification; ADR-017 records the
implemented envelope decision.
