# archive/pre-aq-cont-1 — frozen experimental baseline evidence

**Baseline commit:** `97c9dc26c19c697dbfb204ed503e82c5f053394f` (`main`, June 9, 2026)
**Baseline tag:** `actionqueue/pre-aq-cont-1`
**Frozen by:** `AQ-01` on September 4, 2026
**Contract that supersedes it:** [`AQ-CONT-1`](../../docs/contracts/AQ-CONT-1.md)

This directory is **evidence, not runtime code**. It preserves what the pre-`AQ-CONT-1`
implementation produced and proved, so that the clean-break implementation can show it is
better without keeping the old code path alive.

Rules:

- Target crates (`crates/**`) must not depend on, read, parse, or upgrade anything here. The
  boundary check in `tests/conformance/contract_boundaries.rs` fails on any reference.
- A test under `tests/` may load an archived fixture for offline, read-only diagnostic
  comparison (implementation plan, Section 2.1 and architecture Section 27.4). It must not feed
  archived bytes into a target store.
- Files here are immutable. `SHA256SUMS` pins every artifact and is verified by
  `tests/conformance/frozen_evidence.rs`. Re-running the capture tool after `AQ-03` will not
  reproduce these bytes; that is expected and is why they are frozen now.

## Layout

| Path | Contents |
|---|---|
| `selected-wal-fixtures/lifecycle-wal-v5.wal` | WAL v5 produced by the deterministic lifecycle scenario below |
| `selected-wal-fixtures/lifecycle-wal-v5-truncated-tail.wal` | Same WAL with a partial final record (seven trailing bytes removed) |
| `selected-wal-fixtures/expected.json` | Projection facts the baseline reader reconstructs from the WAL (task/run IDs, states, attempt lineage, sequence) |
| `selected-snapshot-fixtures/lifecycle-snapshot-schema8.bin` | Snapshot (framing version 4, schema 8) built from the same projection |
| `selected-snapshot-fixtures/lifecycle-snapshot-schema8-crc-mismatch.bin` | Same snapshot with one payload byte flipped so the CRC fails |
| `characterization-results/` | Per-test pass/fail record for every CI feature set at the baseline, plus `summary.json` |
| `crash-scenarios/` | Catalogue of baseline crash and corruption tests with source hashes at the tag |
| `performance-baseline/` | Environment description and a small reproducible WAL append/replay measurement |
| `known-failure-cases/` | Baseline seams classified as reject or replace, with the PR that removes each |
| `tools/capture_fixtures.rs` | The capture tool (root-harness example `capture_pre_aq_cont_1_fixtures`) |
| `SHA256SUMS` | Hashes of every artifact above except this README and the tool source |

## Lifecycle scenario captured in the fixtures

Fixed identifiers are used so lineage is reproducible; timestamps equal WAL sequence numbers.

1. Task A (`Once`, `max_attempts = 3`, `timeout 30s`): created; run created; `Scheduled →
   Ready → Leased` with a lease acquired; `Leased → Running`; attempt A1 starts and finishes
   with a retryable failure; lease released; `Running → RetryWait → Ready`; second lease;
   attempt A2 starts and finishes successfully with output bytes; lease released; `Running →
   Completed`.
2. Task B (`Once`, concurrency key `archive-key`): created; run created; promoted to `Ready`;
   the task is then canceled.
3. Task C (`Once`): created; run created; left `Scheduled` to represent in-flight work at the
   freeze point.
4. Engine paused and resumed.

The corruption variants exist so that `AQ-03` can prove the target store rejects both healthy
and damaged pre-contract data with the documented error, without invoking the baseline reader.

## Reproducing

```bash
cargo run --release --example capture_pre_aq_cont_1_fixtures -- /tmp/pre-aq-cont-1-recapture
```

The lifecycle WAL is byte-for-byte reproducible at the baseline commit. The snapshot is **not**:
two captures from identical WALs produce different snapshot bytes because the baseline
serializes projection collections in hash-map iteration order (recorded as known failure case
11). The frozen snapshot bytes are therefore one valid capture, pinned by hash. Performance
numbers vary by machine and are recorded with their environment.

## What is deliberately absent

- Whole-repository copies: the tag preserves every source file.
- Coordinator and subscription fixtures beyond the catalogued tests: the baseline `SubmissionChannel`
  and `EventFilter::Custom` seams produce no durable artifact to archive, which is itself the
  evidence (see `known-failure-cases/`).
- Target thresholds: nothing here constrains `AQ-CONT-1` performance.
