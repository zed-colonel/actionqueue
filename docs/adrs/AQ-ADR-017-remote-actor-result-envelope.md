# AQ-ADR-017 — Remote actor result envelope

- **Status:** Accepted.
- **Decide before:** `AQ-11`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H9, AQ-H16
- **Architecture references:** §20.1, §20.2, §18.6, §25.9, §32.8 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

Remote executors may report late or stale results after lease expiry or restart.
Stale results must not mutate state.

## Recommended decision

Every remote result carries run ID, attempt ID, lease fence, disposition digest, and
the target contract revision. The authority rejects results whose fence does not match the
current lease or whose contract revision is unsupported.

## Alternatives considered

Trust actor-reported attempt numbers alone (rejected: §25.9).

## Consequences

Actor protocol is versioned from the first target release.

## Verification required

Stale-fence rejection; revision mismatch rejection; replay ignores rejected
envelopes.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | `AQ-11` |
| Accepted on | 2026-09-12 |
| Superseded by | — |

## Implementation evidence

`acceptance_remote_protocol` exercises version/digest/fence rejection, exact
claim and result retransmission, WAL and snapshot replay, and checkpoint/resume
assignment after response loss. CanonicalDispositionV1 uses explicit tags and a
separate SHA-256 domain; its known-answer fixture is generated independently in
Python. Accepted disposition history contains the original producer proposal and
fence, so result identity is reconstructed from that immutable record without a
separate durability transaction. Storage independently checks remote expectations.
