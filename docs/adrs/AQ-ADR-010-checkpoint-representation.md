# AQ-ADR-010 — Checkpoint representation

- **Status:** Proposed. The recommended default below is the working implementation choice
  until code review produces a concrete counterexample (implementation plan, Section 3).
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
| Accepted in PR | _pending_ |
| Accepted on | _pending_ |
| Superseded by | — |
