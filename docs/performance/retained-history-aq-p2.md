# AQ-P2 retained-history measurements

AQ-P2 uses structurally shared projections and derived coordination indexes;
[AQ-ADR-019](../adrs/AQ-ADR-019-structurally-shared-projections.md) records the
architecture and initial spike. The [instrumented 0.2.0 baseline](retained-history-0.2.0.md)
remains unchanged. Run-local raw results, verification logs, resource measurements
and the final acceptance report belong in the work item's `scratch/verification/`,
as required by the implementation workflow. They are attached to the handoff,
rather than committed as generated test output.

Run from the workspace root with the supplied `TMPDIR` and `CARGO_TARGET_DIR`:

```sh
cargo bench --locked --bench retained_history -- \
  --retained-runs 1000 --retained-runs 10000 --retained-runs 100000 --repetitions 5
cargo bench --locked --bench mutation_surfaces -- 1000 10000 100000
cargo test --locked --release -p actionqueue-daemon --lib \
  http::publication_tests::publication_scale_measurements -- \
  --ignored --exact --nocapture --test-threads=1
```

Run the runtime surface program five times. The publication measurement already
runs five repetitions internally. Keep builds and other verification work out of
the measured intervals. Capture process maximum RSS separately; it includes
fixture construction and full recovery/digest images, so it is not a measurement
of the resident live queue alone. These measurements use warm filesystem caches
and do not claim an isolated host or a public throughput guarantee.

The storage runner's single-sample path retains AQ-P1's setup, mutation and
recovery boundaries. With `--repetitions`, it builds and closes one fully durable
terminal prefix per size, then copies its manifest, lock file and WAL into fresh
directories. It synchronizes those copies, validates their prefix hashes and
opens independent writer sessions before timing. Each sample starts with exactly
N retained terminal runs; it admits 100 tasks and cancels their 100 runs with
immediate durability. Every sample performs both complete recovery checks and
verifies identical WAL contents and canonical projection digests. The original
seed remains unchanged. Prefix construction, copying, validation and session
opening are outside all timed regions. Prefix identities are shared across the
repetitions; measured admissions derive fresh run identities.

Report the five raw observations and median per-operation admission/cancellation
costs at each size. Both ratios of the 100,000-run median to the 1,000-run median
must be at most 2. Timing thresholds remain outside ordinary CI. The runner
asserts zero complete projection copies and zero wholesale copied run entries;
shallow root clones, copied records, subscription candidates, cancellation run
candidates and reducer visits are reported separately. The runtime runner includes
incremental dependency/hierarchy updates and synchronous cancellation cascades.
The daemon measurement times the actual publication helper, separately from
mutation preparation and WAL sync, including its equal-revision identity path.

Ordinary acceptance coverage includes retained-wait cancellation telemetry,
unrelated subscriptions, a repeating task with 1,000 retained terminal runs and
one unfinished run, 100,000-entry shared buckets and histories, revision isolation,
post-publication rollback, hydration indexes, and incremental coordination versus
reconstruction. The existing crash suites and independent canonical digest vector
retain their assertions. Mutation probes for publication rollback, hydration index
reconstruction and active-run selection must fail through assertions, with source
restored before the final checks. Independent review remains a controller step.

AQ-P3 can use the measured WAL-only and snapshot-plus-tail bootstrap observations.
AQ-P2 intentionally retains complete WAL verification and snapshot schema 9.
