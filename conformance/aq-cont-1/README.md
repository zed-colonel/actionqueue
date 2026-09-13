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

## AQ-13 revision 10 implementation status

Revision 10 is **in progress, not release conformance**. `manifest.yaml` now uses
JSON syntax (a YAML subset) so the runner and tests share structured validation.
All previous fixture bytes and historical projection versions are preserved.
`coverage.json` inventories all twenty invariants and eighteen developmental cases;
unmapped entries are explicit release blockers, not passing evidence.

New executable evidence:

- Six immutable scenarios (early/late signal, deadline, cancellation, lost-response
  admission, and required fan-out), reviewed expected structural observations,
  WAL-only and snapshot/tail equality, and process restart at every declared cut.
- Fourteen selected storage cuts covering all eleven planned commit/publication
  boundaries, two torn-frame cuts, and accepted resume assignment. Snapshot
  publication retains complete WAL history, as required by the accepted ADRs.
- An independent one-to-three-task model with 192 short race sequences and sixteen
  seeded longer sequences. Failures save a seed and deletion-minimized command
  sequence under `TMPDIR`.
- Dedicated persisted workloads for AQ-DD-001, 013, 014, 015, 016, and 018, each
  checked live, after snapshot recovery, and after acknowledged process termination.
  These are focused structural assertions; they do not complete the full eighteen
  case exit gate or replace transport/authorization/metric matrix requirements.
- A public-runtime two-store reference workload exercising uncertain external work,
  lost admission responses, changed-meaning conflicts, early/duplicate callbacks,
  restart of both sides, and application-owned output rejection.
- Fixed-size performance inputs with ordinary admission and handler completion,
  unmatched signals/waits, matching fan-out, snapshot bytes/time, recovery, and
  compound encoding bytes/time. Timing is informational; current bounds cover
  complete match drainage and hard frame limits, not a general complexity proof.

```sh
cargo aq-conformance
cargo aq-developmental
cargo run --example aq_conformance -- --report "$TMPDIR/aq-report.json"
cargo bench --bench continuation
bash conformance/aq-cont-1/cross-feature-persistence.sh
```

`AQ_PERFORMANCE_REPORT` selects the benchmark JSON output; the default is under
`TMPDIR`. Tests never regenerate expected results or fixture hashes. Additive
fixtures require a new package revision and reviewed SHA-256 inventory updates.

Remaining AQ-13 work: executable daemon/CLI/external-adapter runner drivers;
full per-case developmental attribution, permission, scheduling, metrics, and
sensitive-content variants; parent/child and physical retry lineage in the pure
model; per-fixture backup/corruption variants; stronger algorithmic cost bounds;
and promotion to executable status only after complete coverage evidence passes.
See [driver contract and reporting](drivers/README.md).

Two existing API boundaries shape these tests. Concrete signal causation IDs must
resolve within the receiving store, so the two-store reference workload preserves
remote producer IDs in the supported opaque external causation reference. The
persistence probe's tenantless workload uses an explicitly non-platform store and
an authenticated single-tenant host; a separate empty full-feature store proves
that a base binary still rejects the platform-bearing manifest without writes.
