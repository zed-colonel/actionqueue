# AQ-CONT-1 package runner

Run from the repository root:

```sh
cargo run --example aq_conformance -- --report "$TMPDIR/aq-conformance.json"
cargo run --example aq_conformance -- --fixture AQ-CF-SCENARIO-EARLY
```

The embedded mutation-service driver exercises public admission, continuation,
control, and inspection APIs. The separate reference-adapter test uses
`BootstrappedEngine` and normal handlers in two independently owned stores.
Generated run/attempt IDs remain exact within every store and its recovered
copies. Fixture observations use task-number aliases only for cross-store run
lookup; recovery also compares the exact projection digest and inspection
history, controls, and causal edges without ID normalization.

Exit status 0 means the selected subset passed. Reports identify package revision,
fixture SHA-256, compiled feature profile, variant, and crash cut. A caught assertion
failure is exit 1. Missing capabilities, fixtures, or full-profile coverage is exit
2. A base-profile report is never full conformance. `--full` currently fails closed
because revision 9 is still in progress; it also requires
`workflow,budget,actor,platform` and forbids fixture selection.

Each crash worker owns its own store and acknowledges the exact cut. The controller
has a 30-second deadline and kills and reaps the child on success or failure. Worker
stderr is drained and retained in failure diagnostics. Stores and comparison files
are temporary and honor `TMPDIR`. Process kills do not model power loss. Torn-header
and torn-payload injection are separate deterministic storage tests.

The daemon, real CLI, and external adapter are not yet selectable runner drivers.
Existing daemon and CLI acceptance tests remain supplementary evidence. No
unavailable downstream repository is certified by this package.

## Downstream adapter protocol (reserved, version 1)

The intended adapter boundary is newline-delimited JSON over a controller-owned
child's stdin/stdout, one request and response per line:

```json
{"schema_version":1,"request_id":1,"operation":"capabilities"}
{"schema_version":1,"request_id":1,"capabilities":["admission","signals","waits","inspection"],"features":[]}
{"schema_version":1,"request_id":2,"operation":"execute","step":{"op":"signal","signal":1}}
{"schema_version":1,"request_id":2,"observation":{}}
{"schema_version":1,"request_id":3,"operation":"inspect","query":{}}
{"schema_version":1,"request_id":3,"observation":{}}
```

Identity credentials are out-of-band driver configuration, never scenario metadata.
Lifecycle, restart, clock control, and storage fault injection belong to the harness
controller, never daemon endpoints. This reserved protocol is documentation only;
revision 9 does not yet implement or certify an external protocol consumer.
