# conformance/aq-cont-1

The `AQ-CONT-1` conformance package owned by ActionQueue. Downstream contracts
(`WI-FABRIC-2`, `EXO-V3`) pin an exact revision of this package before claiming
continuation semantics.

| File | Purpose |
|---|---|
| `manifest.yaml` | Contract revision, normative document hashes, fixture schema, fixture inventory, acceptance matrices |
| `aq-cont-1-developmental-campaign-acceptance-matrix.yaml` | Eighteen `AQ-DD-*` developmental-neutrality cases (attribution neutrality, durable execution, recovery and idempotency, semantic non-ownership, privacy and observability); executed with replay and crash variants by `AQ-13` |
| `contract-boundaries.json` | Policy driving the repository boundary checks in `tests/conformance/contract_boundaries.rs`; run with `cargo aq-conformance` |

The fixture inventory began empty at `AQ-01`; subsequent revisions preserve the
historical vectors while adding executable coverage.

AQ-05 adds signal canonical and projection-v3 vectors while preserving v1/v2
evidence. `generate-signal-vectors.py` independently computes the bytes/digests.
The signal admission, retention and crash acceptance binaries are included in
`cargo aq-conformance`; full feature tests cover tenant/profile isolation.
These prove retained signal admission/indexing and explicit protection, not wait
resolution or physical WAL compaction.

AQ-06 activates wait/control records, task/admission schema 2, and projection v4.
`acceptance_waits` and `acceptance_wait_crash` are continuation gates; the latter
is separate to avoid inherited store-lock descriptors during process spawning.

AQ-09 adds canonical admission v2 (lifecycle policy), parent-run scoped child keys,
typed child waits/wakes, and projection v7. Task/admission WAL schema 3 and
wait/disposition schema 2 leave prior payload layouts intact. The manifest refuses
older stores. `generate-child-vectors.py` preserves earlier vectors and independently
computes the new vectors. `acceptance_transactional_child_admission` and
`acceptance_parent_wait_child_atomicity` are included in `cargo aq-conformance`.
They cover `AQ-DD-005` with ordinary bounded child batches, checkpoints, DAG gates,
and opaque attribution. `cross-feature-persistence.sh` builds isolated base and
expanded binaries, checks identical child wire bytes, and refuses unsupported
store profiles without mutation. Scratch data honors `TMPDIR`.

## AQ-13 package revision 12

Revision 12 is executable. The full gate combines independently checked fixture
hashes, named invariant proof binaries, per-case developmental evidence, and four
public drivers. All fixture bytes and hash entries present at revision 10 remain
unchanged; additions have new paths. `coverage-v2.json` supersedes the preserved
revision-10 coverage inventory.

```sh
cargo run --example aq_conformance --features workflow,budget,actor,platform -- \
  --full --report "$TMPDIR/aq-report.json"
cargo aq-conformance
cargo aq-developmental
cargo bench --bench continuation
bash conformance/aq-cont-1/cross-feature-persistence.sh
```

The full runner builds the actual CLI and reference adapter, builds the proof test
binaries once, and executes them directly. It creates a fresh evidence directory
under `TMPDIR`; it never accepts reports from previous runs. Missing cases,
features, drivers, variants, incorrect input hashes, failed tests, and zero-test
proof runs prevent certification. CI requires `--full`.

Evidence includes:

- All twenty AQ-H invariants mapped to complete named proof binaries. `suite`
  records mean all tests in the named binary passed, including that binary's
  internal replay/crash assertions; they do not invent per-variant results.
- All eighteen AQ-DD cases with separately reported ordinary, replay, and process
  crash variants, exact projection/inspection equality, backup/restore, and
  checksum-corruption refusal. Physical recovery preserves the original resume
  assignment. Attribution probes exercise priority, budgets and executor traits;
  sensitive-content probes use synthetic external-reference canaries.
- Six storage scenarios with immutable expected observations, every declared
  crash prefix, WAL-only and snapshot/tail equality, backup and corruption checks.
- Six public workloads through normal embedded handlers, authenticated TCP daemon
  calls, actual CLI subprocesses, and the published reference adapter process.
  Every driver supplies ordinary, replay and crash results. The daemon and CLI
  drivers use the public remote-actor protocol to execute handler dispositions.
- Fourteen acknowledged storage cuts, including all eleven planned boundaries,
  torn headers/payloads, and accepted resume assignment. Reconciliation must
  produce the exact original signal/checkpoint context and permit resumed execution.
- An independent model: 192 exhaustive short races, 32 seeded longer sequences,
  and guided wake/recovery/child-coordination sequences. It compares winners,
  checkpoints, pending and assigned delivery, physical attempts, leases, accounting,
  and terminal history after every prefix. Failing seeds are minimized in scratch.
- Reproducible 8/129/257-task benchmarks. Thread-local test instrumentation rejects
  unrelated history visits and bounds indexed candidate work across the 128-item
  batch boundary. Candidate removal uses a reverse index. Measurements explicitly
  include the accepted full-projection preparation and full-WAL validation costs;
  elapsed timings remain informational. Instrumentation is absent in production.

This is the ActionQueue-owned reference implementation. An external downstream
adapter can run the same published workload protocol; this package does not claim
that an unavailable WorldInterface checkout was tested. See
[driver protocol and reporting](drivers/README.md).
