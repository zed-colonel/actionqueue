# Performance baseline at the pre-AQ-CONT-1 freeze

These numbers document the environment and a few reproducible measurements at the baseline
commit. They are **not** target thresholds and make no public performance promise. `AQ-13`
defines benchmark fixtures and regression bounds against the target implementation.

## Environment

| Item | Value |
|---|---|
| Date | September 4, 2026 |
| Host | Linux 7.1.8-arch1-3 x86_64 |
| CPU | Intel Core i9-9900K @ 3.60 GHz, 8 cores / 16 threads, 16 MiB L3 |
| Memory | 31 GiB |
| Storage | NVMe, ext4 |
| Toolchain | rustc 1.89.0 (29483883e 2025-08-04), cargo 1.89.0 |
| Build settings | `.cargo/config.toml`: `jobs = 8`, `RUST_TEST_THREADS = 4` |
| Concurrent load | Other workspaces were idle; load average ≈ 1.9 at capture |

## Measurements

| Artifact | What it records |
|---|---|
| `wal-append-replay.json` | Immediate-durability appends (fsync per record), deferred-durability appends, and cold WAL-only replay time for the resulting store; produced by the capture tool in `--release` |
| `../characterization-results/summary.json` | Wall-clock time of each `cargo test` feature-set run with `--test-threads=1` (dominated by `thread::sleep` handlers and process spawns, not throughput) |

## Observation: Immediate durability cost is implausibly low

`DurabilityPolicy::Immediate` routes through `WalWriter::flush`, which calls `File::sync_all`
per record. The measured cost is nevertheless about one microsecond per append (see
`wal-append-replay.json`), far below the cost of a real device flush on this NVMe/ext4 host.
This harness therefore **cannot confirm** that the baseline persists each Immediate record
before returning. `AQ-03` must establish durability with an explicit fsync discipline and a
crash-consistency test rather than inherit the baseline assumption.

The stress test `concurrent_dispatch_stress` (100+ tasks, 4 workers) passes within the default
suite time recorded in `summary.json`; no per-test timing was extracted because the baseline
does not emit it.
