# AQ-ADR-011 — Disposition invalidity

- **Status:** Accepted; implemented and covered by the AQ-08 acceptance suites.
- **Decide before:** `AQ-08`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H9
- **Architecture references:** §12.2, §12.7, §28.7 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

A handler may return a disposition that is internally inconsistent (for example,
`Awaiting` without a wait, or children plus a terminal failure without policy). The engine must
neither guess nor halt the whole queue for an application error.

## Recommended decision

Semantic invalidity of a handler disposition terminally fails that run with a bounded
engine error recorded in attempt lineage. An impossible projection or storage mismatch
discovered while committing a disposition halts mutation processing rather than continuing on a
guessed state.

## Alternatives considered

Retry the attempt on invalid disposition (rejected: masks handler bugs and burns
retry allowance); drop the disposition silently (rejected: `AQ-H9`).

## Consequences

Handler authors receive precise, replayable errors. Operators see engine halts as
distinct from application failures.

## Verification required

Combination-rule table tests; halt-versus-fail classification tests; replay of a
rejected disposition reproduces the same terminal failure.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | AQ-08 work item |
| Accepted on | 2026-09-12 |
| Superseded by | — |

Storage validates the fence before subordinate effects and publishes one immediately synced record. Invalid store-dependent proposals produce a minimal terminal failure with no proposed effects. Stale results append nothing. Uncertain append/sync/publication and impossible preparation mismatches fence the authority. `acceptance_attempt_disposition` exercises these boundaries.

Local active-wait capacity rejection follows the same bounded terminal-failure
policy, with code `wait_capacity`. The rejected checkpoint, children, signals and
consumption do not commit. Closure releases execution ownership so other work can
progress, including at the minimum configured disposition size. A remote executor
instead receives the rejection with its attempt and lease intact and may retry
while the fence remains valid. F-022 acceptance tests cover both paths and recovery.
