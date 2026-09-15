# AQ-ADR-006 — Signal retention

- **Status:** Accepted.
- **Decide before:** `AQ-05`
- **Contract:** `AQ-CONT-1`
- **Invariants:** AQ-H3, AQ-H5
- **Architecture references:** §9.9, §21.5, §26.6, §29.5 of
  [`actionqueue-hardening-implementation-ready.md`](../contracts/actionqueue-hardening-implementation-ready.md)

## Context

Retained signals are required for late waits and for replay; unbounded retention
threatens storage simplicity, while aggressive deletion reintroduces lost wakeups.

## Recommended decision

Conservative default: retain signals until an explicit, configurable horizon elapses
and no active or historical wait references them. Support explicit pins. Never automatically
delete a signal referenced by an active wait. Compaction is a separate, explicit operator or
policy action with metrics.

## Alternatives considered

Delete on first match (rejected: fan-out semantics); infinite retention
(rejected: capacity).

## Consequences

Retention pressure is measurable before compaction is designed. Snapshots record
the signal sequence covered.

## Verification required

Retention-horizon tests; pinned-signal survives compaction; replay after compaction
reproduces active-wait resolution.

## Acceptance record

| Field | Value |
|---|---|
| Accepted in PR | AQ-05 |
| Accepted on | 2026-09-11 |
| Superseded by | — |

## AQ-05 implementation decisions

Retirement is an explicit durable removal from matching, preserving immutable
records and deduplication identity. No automatic retirement runs at admission or
recovery. Both receipt age and sequence distance must strictly exceed configured
thresholds (defaults: seven days and 10,000 sequences). Clock rollback cannot make
a record eligible. Producer occurrence time is irrelevant.

Pins have bounded stable identities. Repeating a pin or missing unpin is a no-op;
one pin cannot release another. Retirement rechecks the whole ordered batch under
exclusive mutation ownership and rejects protected or stale targets before append.
Retirement records preserve time, WAL order and control attribution. Retired
signals cannot be pinned or reactivated by retries.

Default quotas are 100,000 resident identities, 16 MiB of immutable framed signal
records, 100,000 total pins, 64 pins per signal and 1,024 retirements per batch.
Inline content has a 64 KiB hard ceiling and each framed signal operation a 128 KiB
hard ceiling including its 52-byte header. Operational limits may lower these
ceilings. Replay enforces hard format limits independently of operational quotas.
Retired records count against capacity; capacity rejection never evicts state.
Counters expose retained, retired, pinned, total pins, bytes and live capacity
rejections. Rejection telemetry resets on reopen; resident counters rebuild.

AQ-06 must extend `SignalIndex::is_protected` with protections derived from durable
active waits, pending resumes and retained historical attempts before enabling
waits. Those protections must be independent of operator-removable pins. AQ-05
claims explicit pin protection only. The pure arithmetic lives in core so storage
can repeat validation without adding a production dependency on engine; engine
exports a timestamp/sequence-only planning helper using the same arithmetic.

Complete WAL history remains mandatory. Retirement reclaims matching-index
membership, not WAL bytes or resident immutable content. Prepared mutations still
clone the full projection; indexed lookup does not make overall admission
constant-time. Physical truncation/compaction needs a separate storage-format design.

Evidence: retention boundaries, pins, stale batches, retirement replay, snapshot
plus tail, WAL-only and backup/restore parity in `acceptance_signal_retention`;
uncertain retirement and process-kill boundaries in `acceptance_signal_crash`.
Wait-resolution and physical compaction evidence remain with their owning work.
