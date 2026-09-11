# AQ-ADR-010 — Checkpoint representation

- **Status:** Accepted.
- **Decide before:** `AQ-07`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H9, AQ-H13
- **Architecture references:** §11.2–§11.8, §25.11, §26.3 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

Continuation state must be immutable and replay-stable without bloating WAL frames
or making ActionQueue fetch external data.

## Recommended decision

`DataRef::{Inline, External}`. Inline bytes are capped by a strict configurable limit;
external references carry an opaque locator and a content hash. A `CheckpointRef` is immutable
once committed. ActionQueue never fetches, validates, or interprets external checkpoint data;
unavailability is reported to the handler as a bounded resume error.

## Alternatives considered

Inline only (rejected: §26.3 frame growth); ActionQueue-owned blob store (rejected:
non-goal).

## Consequences

Disposition size limits are enforced at admission of the compound disposition.
Checkpoint locators are redacted by default in inspection.

## Verification required

Inline limit rejection; hash mismatch detection on external refs; exactly-once resume
context delivery under crash (§28.6).

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | `AQ-02` |
| Accepted on | 2026-09-09 |
| Superseded by | — |
| Deferred verification | Verified in `AQ-02`: inline limit rejection at construction and decode (`continuation_vocabulary.rs`, `target_serde.rs`). Deferred to `AQ-07`: hash mismatch detection on external refs and exactly-once resume-context delivery under crash (§28.6). |

## AQ-07 implementation addendum

Checkpoint identities are unique across the store and indexed atomically from
`WaitEstablished`; exact retries of that operation acknowledge its original commit.
The immutable producer run, attempt, sequence, data hash and reference remain queryable.
`DataRef::validate` verifies inline integrity; `verify_bytes` lets the executor host
verify externally resolved bytes and optional declared size without storage fetching data.
Unavailable data remains an ordinary handler failure.

Creation defaults are 64 KiB output, 32 KiB checkpoint, 16 KiB signal payload and
128 KiB complete framed disposition. Configuration can lower these limits but cannot
raise hard format ceilings. Exact acknowledgments and replay ignore lowered creation
limits. Nested debug output redacts inline bytes and external locators.

Resume identity is the store-local WAL sequence of the wake. A schema-2 accepted
start durably records its lease fence and immutable assignment. Duplicate starts
return `AlreadyStarted`; only a newly accepted `AttemptStart` may launch execution.
Retry and recovery assignments retain the wake identity and link to the previous
physical attempt. Schema-2 closure origin distinguishes executor failure from
recovery without interpreting error text. This guarantees one assignment per
accepted start, not exactly-once handler effects.

Administrative resumption retains existing checkpoint lineage and creates a new
wake identity. Legacy suspended output bytes are never interpreted as checkpoints.
The handler receives optional causal context because legacy `TaskCreated` records
have no admission attribution; admitted tasks receive their exact original context.
Compound handler disposition and new suspension checkpoints remain AQ-08.
