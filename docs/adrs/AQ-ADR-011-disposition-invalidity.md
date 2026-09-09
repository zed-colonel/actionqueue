# AQ-ADR-011 — Disposition invalidity

- **Status:** Proposed. The recommended default below is the working implementation choice
  until code review produces a concrete counterexample (implementation plan, Section 3).
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
| Accepted in PR | _pending_ |
| Accepted on | _pending_ |
| Superseded by | — |
