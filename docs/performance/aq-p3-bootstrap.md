# AQ-P3 bootstrap acceptance evidence

Measured in the AQ-P3 implementation worktree after AQ-P2, on 2026-10-20.
This report concerns retained terminal Once runs; it is not a general throughput claim.
The historical fixtures, contract files and archived evidence were not changed.

The fresh baseline was measured before changing storage:

```sh
cargo bench --locked --bench retained_history -- --retained-runs 10000 --retained-runs 100000 --repetitions 3
```

| Runs | WAL-only median | Schema-9 snapshot + 200 tail median | Snapshot bytes |
|---:|---:|---:|---:|
| 10,000 | 0.685191466 s | 3.869518843 s | 26,231,173 |
| 100,000 | 8.939687847 s | 42.847811066 s | 262,703,635 |

The completed implementation was measured with:

```sh
cargo bench --locked --bench retained_history -- --retained-runs 10000 --retained-runs 100000 --repetitions 3 --fixture-cache "$TMPDIR/retained-fixtures"
```

The cache holds only durable unmeasured prefixes. Each repetition copies independent
files, recovers a production authority, captures a checkpoint, appends 100 admissions
and 100 cancellations, and publishes the saved cut. Complete writable startup
includes store opening, all snapshot preparation, both boundary probes, tail replay
and writer initialization. External digest, operation-history, and WAL-byte parity
assertions occur after the timer. Publication, fixture construction/copying and
variable-tail preparation are outside startup timers. Publication performs zero
WAL reads and zero reducer applications.

The OS cache was warm, without forced eviction. Paired paths alternate WAL/snapshot,
snapshot/WAL, WAL/snapshot, against identical WAL bytes per pair. Timing acceptance
uses medians of the three paired ratios, without tolerance. All samples are kept
in the run verification artifacts, including failed intermediate measurements.

## Final paired samples

| Runs | Pair | WAL-only seconds | Snapshot seconds | Paired ratio | H seconds | Bytes |
|---:|---:|---:|---:|---:|---:|---:|
| 10,000 | 1 | 0.872367242 | 0.108442722 | 0.124309 | 0.103346251 | 3,990,626 |
| 10,000 | 2 | 1.070660605 | 0.139652123 | 0.130435 | 0.132247584 | 3,990,626 |
| 10,000 | 3 | 0.951273259 | 0.104466446 | 0.109817 | 0.099261446 | 3,990,626 |
| 100,000 | 1 | 11.997527212 | 0.807769211 | 0.067328 | 0.800956361 | 40,434,242 |
| 100,000 | 2 | 12.393503817 | 0.774207122 | 0.062469 | 0.768539199 | 40,434,242 |
| 100,000 | 3 | 12.068469987 | 0.789113826 | 0.065386 | 0.783344880 | 40,434,242 |

At 10,000 runs the median paired ratio is **0.124309**, passing the **0.5** limit.

At 100,000 runs the median paired ratio is **0.065386**, passing the **0.5** limit.

Aggregate H includes snapshot reading, integrity, decoding, profile/semantic validation, index construction, hydration, and boundary checks through usable projection/indexes. H grows **7.579809×**, below snapshot byte growth **10.132306×**. The complete large checkpoint is **40,434,242 bytes**, below **268,435,456**; the payload limit also remains unchanged. Relative to schema 9, size falls by **84.61%** at 100,000 runs, including operation history.

The first compact sequential implementation passed startup ratios but failed H
scaling (12.862204× versus 10.132306× bytes). Independent roots alone still failed
(12.233658×); immutable bulk tables plus persistent deltas reduced it to 10.946145×,
still failing. Removing redundant validation tables, building revision buckets once,
and bounded parallel section decoding produced the passing result. No failed gate
was waived, and the size ceiling was never raised.

## Median snapshot phases (seconds)

The aggregate H also includes orchestration/probe costs; constituent timers do not
replace it. Canonical v9 digest work is zero on ordinary checkpoint startup and
remains part of explicit full verification.

| Phase | 10,000 runs | 100,000 runs |
|---|---:|---:|
| canonical_digest_seconds | 0.000000000 | 0.000000000 |
| decode_seconds | 0.052991187 | 0.177171645 |
| index_construction_seconds | 0.015638743 | 0.157845604 |
| inspection_index_seconds | 0.002490697 | 0.042347506 |
| integrity_seconds | 0.014079069 | 0.150937521 |
| io_seconds | 0.000538315 | 0.016600486 |
| mapping_validation_seconds | 0.003228382 | 0.076812469 |
| object_hydration_seconds | 0.014388187 | 0.143975820 |
| profile_validation_seconds | 0.000723239 | 0.008346826 |

## Encoded section sizes

The fixed `projection_sections` accounting label appears as `extensions` in the
measurement artifact; it was renamed to comply with the contract boundary policy.
The checkpoint bytes and measured work are identical.

| Section | 10,000 runs | 100,000 runs |
|---|---:|---:|
| header | 130 | 130 |
| tasks | 500,004 | 5,000,004 |
| admissions | 1,691,750 | 16,991,750 |
| runs | 449,876 | 4,583,492 |
| controls | 583,496 | 5,983,496 |
| cancellations | 491,750 | 4,991,750 |
| projection_sections | 68 | 68 |
| control_targets | 273,496 | 2,883,496 |
| attempt_owners | 4 | 4 |
| framing_integrity | 52 | 52 |

## WAL work follows the tail

At both snapshot sizes, every repetition asserts these actual frame-reader counts
across complete writable startup. The two probes read 383 bytes combined; they
apply no events. WAL-only startup reads every frame and applies it once. Inspection
history remains available after hydration and equals the authoritative task histories.

| Tail events | Frames read | WAL bytes read | Reducer applications |
|---:|---:|---:|---:|
| 0 | 2 | 383 | 0 |
| 20 | 22 | 9,663 | 20 |
| 200 | 202 | 76,683 | 200 |

## Artifact inventory

Run: `1964cff7-8fd0-46fe-9dbd-bfb4a7e6c25e`, under its `scratch/verification`:

- `aq-p3-baseline.log` and `.json`: fresh unchanged-AQ-P2 measurements.
- `aq-p3-checkpoint-first.json`, `aq-p3-linear-benchmark.json`,
  `aq-p3-bulk-benchmark.json`: intermediate failures and all samples.
- `aq-p3-parallel-benchmark.json`: initial passing measurement.
- `aq-p3-final-benchmark.log` and `.json`: final full protocol with variable tails.

These are verification artifacts, not committed runtime fixtures. Source fixtures
`crates/actionqueue-storage/tests/fixtures/checkpoint-v{9,10}.hex` instead freeze
wire compatibility. [ADR-020](../adrs/AQ-ADR-020-bounded-snapshot-bootstrap.md)
records the trust boundary, implementation choices and upgrade procedure.
