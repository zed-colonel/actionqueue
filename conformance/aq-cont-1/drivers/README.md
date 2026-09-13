# Executable conformance drivers

Run the full gate from the repository root:

```sh
cargo run --example aq_conformance --features workflow,budget,actor,platform -- \
  --full --report "$TMPDIR/full.json"
```

`--driver daemon`, `--driver cli`, and `--driver adapter` run the corresponding
public-driver subset. They require the expanded feature profile. The default
base-feature embedded subset runs the immutable storage scenarios. Only `--full`
requires all invariant, developmental, storage-scenario, and public-driver evidence.
`--full` rejects `--driver` and `--fixture` selections. Every storage scenario
requires ordinary, replay, backup, corruption, and each declared crash-cut record. A subset cannot
be promoted by changing its report label: missing evidence and input hashes are
validated before success. Exit 1 indicates failed assertions; exit 2 indicates
missing capabilities, coverage or evidence.

| Driver | Execution and control surface |
|---|---|
| embedded | `BootstrappedEngine`, normal handlers and `run_until_idle` |
| daemon | Real loopback TCP, production router/authentication, public remote actor claims/results |
| cli | Actual `actionqueue` executable for admission, signal, cancellation and inspection; remote actor HTTP for execution |
| adapter | External process implementing the protocol below; defaults to the built `aq_adapter` example |

The daemon host supplies a deterministic clock and synthetic authentication
configuration. The router has its normal authentication, scope checks, typed
errors, pagination and redaction. Test lifecycle control never becomes a production
endpoint. The published public fixtures specify the expected terminal state,
attempt accounting, wake kind, and active-wait count. Exact identities and lineage
are additionally compared across recovery without normalizing the projection.

## External adapter protocol, version 1

`--adapter /absolute/path/to/executable` selects a downstream adapter executable
for the adapter driver. `AQ_ADAPTER` has the same effect for the test binary.
`AQ_CLI` can select a prebuilt CLI. Neither value is evaluated by a shell.

For ordinary calls the adapter reads one JSON request from stdin, writes one JSON
response to stdout, and exits. Diagnostics go to stderr. It must also accept
`--request-file PATH` for the acknowledged process-crash controller.

```json
{"schema_version":1,"store":"/controller/scratch/store","mode":"late","phase":"prepare","hold":false}
```

Modes are `early`, `late`, `deadline`, `cancel`, `admission`, and `fanout`.
`prepare` runs the published workload to its durable preparation cut. `finish`
opens that same store, supplies the remaining callback/control/time advancement,
and checks the reviewed final result. The exact scripts are in the public driver
source; the immutable mode/expectation definitions are in `public/*-v1.json`.
The reference implementation is `adapter.rs` and `tests/conformance/harness/public.rs`.

The response is `{ "digest": <projection digest>, "inspection": <structural views> }`,
using the public inspection DTOs. The controller independently opens the target
store to verify the response, WAL replay, snapshot/tail, backup and corruption
behavior. This protocol tests an adapter to the ActionQueue store contract, not an
arbitrary queue backend with a different persistence format.

When `hold` is true, the adapter writes that response to the store path with the
extension replaced by `evidence.json`, flushes the exact line
`AQ_PUBLIC_PREPARED` to stdout, and remains alive with its store writer owned.
The controller kills and reaps it at that acknowledgment. Completion and crash
handshakes have bounded timeouts and output limits. The fixture driver then reopens
and finishes the store, comparing its exact projection and lineage.

## Evidence

Reports include package and contract revisions, fixture ID and SHA-256, compiled
feature profile, driver, variant, crash point, and assertion result. Full runs use
a new `aq-full-evidence-*` directory under `TMPDIR`. Named proof binaries must exit
successfully and report a positive passing-test count. Developmental reports are
required individually for all 18 cases; public reports are required for all
six workloads, four drivers, and three variants. Previous-run reports are never
accepted. `suite` evidence identifies a complete named test binary and preserves
its log; ordinary/replay/crash labels are reserved for executed workload variants.

## Release handoff

Release 0.2.0 pins package revision 15 and publishes this package, the unchanged
developmental profile and matrix, and exact source/evidence hashes in a separate
release manifest. See [release procedure](../../../docs/releases/0.2.0.md).
Download the complete `source.bundle` and check out the manifest SHA before running
the drivers: they import `tests/conformance/harness`, and boundary checks require
Git-tracked files. A directory-only copy cannot run the complete suite.

The reference adapter certifies ActionQueue-owned protocol behavior only; it does
not certify a WorldInterface or Exoskeleton revision. Downstream implementers run
the full driver with `--adapter PATH_TO_EXECUTABLE` for their own adapter evidence.
See the [independent outbox example](../../../docs/examples/downstream-handoff.md)
for a production consumer without repository test helpers.

## Finalization coverage (package revision 14)

Revision 14 adds admission-bypass, remote-priority, and aggregate active-wait
quota regressions (F-017–F-019) to the existing complete-binary proof suites.
It preserves the normative contract and all frozen fixtures. The full feature
matrix additionally exercises the authenticated HTTP priority path and isolated
triad fixture (F-021).

## Local wait-capacity closure (package revision 15)

Revision 15 adds F-022 real-handler regressions to `acceptance_attempt_disposition`:
zero and saturated store/tenant limits, bounded terminal failure, discarded
subordinate effects, queued-work progress, released lease/key ownership, and
WAL/snapshot/restart parity. `acceptance_remote_protocol` separately proves that
remote capacity rejection preserves the active attempt for an executor retry.
The existing full proof binaries execute this coverage. Normative semantics and
frozen fixture bytes remain unchanged.
