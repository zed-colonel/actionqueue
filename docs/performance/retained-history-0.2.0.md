# Instrumented 0.2.0 retained-history baseline

Measured on October 17, 2026, from clean committed revision
`7986dde92a43306fed384c9e6ef41c3991b36384`. The released baseline is
`29e096c4f06985d1a9bd9d5fe5a3a6a5db8a6860` (0.2.0); the work-item starting
revision is `0b8d08ec1f9dc2caeaebe904ad2e58d9e5ccc9b8`. Their intervening diff
contains planning documents and check configuration only. The measured revision
adds the benchmark, acceptance coverage and testing-only storage counters, with
the same mutation and recovery algorithms as 0.2.0.

[Machine-readable evidence](retained-history-0.2.0.json) contains raw nanosecond
timings, structural counters for every phase, file sizes, sequence lengths,
projection/WAL digests, command, revisions and environment details. Timings are
informational: one sample per size on a shared workstation, without CPU pinning
or isolation. These are storage-boundary measurements; they do not reproduce
WorldInterface's end-to-end timings.

## Workload and measurement boundary

Each size creates a fresh filesystem store under `TMPDIR` with single-tenant
control scope, no tenant identity, and no optional store features. It admits and
cancels N tasks through the storage authority. Each has a unique admission key,
a fixed three-byte payload and one `RunPolicy::Once` run. The retained runs are
canceled, with no attempts or waits. Every mutation uses immediate durability.

At that terminal prefix, the fixture builds an in-memory snapshot outside the
measurement. It then measures a batch of 100 fresh admissions through
`actionqueue_runtime::admission::ensure_task` and a batch of 100 fresh compound
task cancellations through `MutationCommand::Cancel`. Requests and target IDs
are built before the measured regions. These phases exclude runtime dependency
rebuilding, handler execution and daemon projection publication.

After closing the authority's WAL writer, the fixture measures
`load_existing_projection` without a snapshot. It publishes the saved prefix
using `SnapshotFsWriter`, closes the recovered WAL writer, and measures the same
bootstrap with that snapshot and a fixed 200-event tail. Snapshot publication
replays the prefix in 0.2.0; its cost is outside the measured phases. Both measured
recoveries read identical WAL contents, checked by SHA-256. Setup, correctness
checks, snapshot construction/publication and digest comparisons are unmeasured.
Bootstrap timings include the complete public recovery call, including store
opening, snapshot hydration, replay, validation, digest traversal and writer
creation.

The prefix is `1 + 2N` events (including `StoreInitialized`); the final WAL has
`1 + 2(N + 100)` events. Final task/run/admission counts, canceled states,
absence of attempts/waits, selected snapshot sequence, and projection-digest
parity are asserted after measurement. The small acceptance scenario exercises
N = 0 and N = 7 with the same 100-operation batches.

## Timings and sizes

| Retained runs N | Admission batch (s) | Admission mean (ms) | Cancellation batch (s) | Cancellation mean (ms) | WAL-only bootstrap (s) | Snapshot plus tail (s) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 0.282 | 2.825 | 0.299 | 2.989 | 0.931 | 1.640 |
| 10,000 | 2.620 | 26.199 | 2.543 | 25.430 | 184.150 | 214.391 |

| N | Prefix events | Tail events | Final sequence | WAL bytes | Snapshot bytes |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 2,001 | 200 | 2,201 | 837,058 | 2,619,958 |
| 10,000 | 20,001 | 200 | 20,201 | 7,689,876 | 26,231,173 |

Batch durations and mean per-operation times are both recorded in JSON.
At each size, the final task, run and admission counts equal N + 100.

## Structural work

`actionqueue-storage/testing` exposes thread-local `ProjectionWork` counters via
`recovery::work::reset_projection_work` and `projection_work`. The existing
seven-element iterator `counts()`/`reset()` interface remains separate.

- `full_projection_clones`: complete `ReplayReducer::clone` calls, centrally
  counted in its field-by-field `Clone` implementation.
- `cloned_run_entries`: sum of source run counts across those complete clones.
- `reducer_event_visits`: entries into `ReplayReducer::apply`, including nested
  and rejected applications.

Snapshot construction/copies, digest traversal and arbitrary collection scans
are outside these definitions. Counters are external to durable state and their
hooks compile away without `testing`. Measurement runs synchronously on one
thread; reset happens immediately before each phase and capture immediately
after, before checks or reporting.

For B = 100, these explicit **0.2.0 expectations** are asserted in the shared
runner. AQ-P2/AQ-P3 must deliberately replace them as the algorithms change.
No elapsed-time limit is asserted.

| Phase | Full clones | Reducer visits | Cloned run entries asserted |
| --- | ---: | ---: | ---: |
| Admissions | `2B` | `B` | `2BN + B(B - 1)` |
| Cancellations | `2B` | `B` | `2B(N + B)` |
| WAL-only bootstrap | `2(N + B)` | `1 + 2(N + B)` | recorded |
| Snapshot plus tail | `2(N + B) + 2B` | `1 + 2(N + B) + 2B` | recorded |

| N | Phase | Full clones | Cloned run entries | Reducer visits |
| ---: | --- | ---: | ---: | ---: |
| 1,000 | Admissions | 200 | 209,900 | 100 |
| 1,000 | Cancellations | 200 | 220,000 | 100 |
| 1,000 | WAL-only bootstrap | 2,200 | 1,214,950 | 2,201 |
| 1,000 | Snapshot plus tail | 2,400 | 1,429,900 | 2,401 |
| 10,000 | Admissions | 200 | 2,009,900 | 100 |
| 10,000 | Cancellations | 200 | 2,020,000 | 100 |
| 10,000 | WAL-only bootstrap | 20,200 | 102,014,950 | 20,201 |
| 10,000 | Snapshot plus tail | 20,400 | 104,029,900 | 20,401 |

Each live admission/cancellation clones once in the authority and once in the
compound reducer. Recovery has the reducer clones, and snapshot recovery also
applies the tail to the hydrated projection while replaying the complete WAL.
The snapshot therefore does not bound bootstrap work in this baseline.

## Reproduction and environment

Run from the repository root, keeping `TMPDIR` on the storage under measurement
and retaining the environment's `CARGO_TARGET_DIR`:

```sh
cargo bench --locked --bench retained_history -- \
  --retained-runs 1000 --retained-runs 10000 \
  > "$TMPDIR/retained-history-results.json"
```

This is a custom `harness = false` benchmark; it requires explicit sizes and
emits JSON to stdout and progress to stderr. The default Cargo bench profile is
optimized. No root optional features are requested; the root dev-dependency
enables storage's `testing` feature. The store manifest has an empty feature
list even when running acceptance tests in the expanded feature matrix. No new
dependency was added. Run identities and store identities are freshly generated,
so digest values vary between reproductions.

The argument parser also accepts `--retained-runs 100000` for later work. That
size was deliberately not run on 0.2.0 because setup and replay are quadratic.

The recorded host uses Rust/Cargo 1.89.0, x86-64 Linux 7.2.5-3-omarchy, an Intel
Core i9-9900K (8 cores, 16 logical CPUs), 67,329,191,936 bytes of RAM, and ext4 on
`/dev/nvme0n1p1`, a WDS100T3X0C-00SJG0 NVMe device. Both sizes run in ascending
order in one process with fresh temporary stores. Page cache is not dropped:
the WAL was just written and read for its checksum before WAL-only recovery;
the snapshot was just published with prefix verification before snapshot
recovery. Compilation and fixture setup are excluded from phase timings. Raw
start/end times and host load averages are in the JSON evidence.

Focused validation covers clone equivalence (including derived indexes),
independent collection mutation, reset/thread isolation, rejected apply-entry
counting, and exclusion of snapshot/digest work. The repository's adopted checks
are fmt, clippy, test-default and test-all-features; the structural acceptance
scenario runs in both test suites.
