# AQ-02 review remediation

## PR description

The AQ-02 core shapes now accept absent signal correlation/source attribution,
bound every disposition collection, and permit opaque content types with parameters
and resolver schemes containing URI punctuation. Awaiting transition errors identify
the invalid outgoing edge precisely, and runtime key release reads the continuation
policy accessor. Focused tests cover optional matching, JSON/postcard validation,
collection ceilings, text domains, and both wait policy decisions.

Compatibility is limited to the legacy executor-trait vector's postcard field
layout. Deserialization applies new grammar and count/length ceilings, so formerly
accepted WAL and snapshot values can fail. Snapshot JSON routing keys changed
without aliases or a schema bump. This is a clean break, not a migration guarantee;
AQ-03 establishes the fresh store lineage. The snapshot version history now describes
v5's legacy capability requirements rather than attributing the AQ-02 rename to v5.

## Review dispositions

| Finding | Disposition | Change |
|---|---|---|
| 1 | Fixed | Envelope correlation and source are optional; constrained filters require a present, equal value. Missing and null attribution round-trip. |
| 2 | Fixed | Consumption has a 64-entry ceiling enforced by construction and JSON/postcard decode. |
| 3 | Fixed | ContentType and the new DataScheme accept non-empty bounded text without controls; spaces and `+` are accepted. |
| 4 | Fixed | The PR description above and getting-started documentation explicitly distinguish layout preservation from value-domain and JSON compatibility. |
| 5 | Fixed | Snapshot v5 history refers to legacy capability requirements and dates the terminology change to AQ-02. The removed legacy identifier remains forbidden in target files. |
| 6 | Fixed (tracking requested) | Both cancellation handlers and getting-started documentation require AQ-06 to cancel active waits durably and map continuation-record guard errors before Awaiting is reachable. |
| 7 | Fixed | Runtime release consults the task wait-policy accessor and an exhaustive policy decision. AQ-03 must add the backing durable field; the current accessor supplies the default. |
| 8 | Fixed | Outgoing Awaiting rejection takes precedence over incoming rejection; the reason names Ready, Failed, and Canceled. The exhaustive transition test checks it. |
| 9 | Fixed | TextGrammar replaces numeric modes and the catch-all with named variants and exhaustive matching. |
| 10 | Fixed | The duplicate empty-list pre-check and redundant error variant are removed; ExecutorTraits validates all entry paths. |
| 11 | Fixed | WakeReason::Signal takes its sole signal identity from the envelope. |
| 12 | Fixed | CheckpointId rustdoc describes checkpoints. |
| 13 | Fixed | Removed the redundant string conversion. |
| 14 | Fixed | Serde test name and failure message use executor-trait terminology. |
| 15 | Fixed | The new result taxonomy is DispositionOutcome, distinct from mutation::AttemptOutcome. |

The AQ-03 persistence field and AQ-06 cancellation behavior remain downstream work,
as in the implementation plan. No currently reachable continuation behavior is enabled
by this remediation.

## Follow-up review dispositions

| Finding | Disposition | Change or follow-up |
|---|---|---|
| 1 | Deferred to AQ-03 | Back `TaskConstraints::concurrency_key_wait_policy` with the persisted per-task field and add a runtime test that `try_release_concurrency_key` retains the key for `HoldWhileAwaiting`. AQ-02 establishes pure vocabulary; AQ-03 establishes the fresh durable lineage. Storage still rejects generic Awaiting edges, so the default-only accessor does not expose reachable continuation behavior. |
| 2 | Fixed | `BoundedCode` rustdoc now says "machine-readable code", covering non-error uses. |
| 3 | Fixed | `DataScheme` uses its own `MAX_DATA_SCHEME_BYTES` constant, retaining the existing 64-byte ceiling. |
| 4 | Fixed | Moved this log under `docs/planning/aq-cont-1/` alongside the work-item planning artifacts. |

## Final cross-file addendum dispositions

The supplied addendum contains findings 13 and 14. Follow-up findings 1–4 are
recorded immediately above. Findings 5–12 from that follow-up report were not
included in the work-item brief or found in this worktree; their text has been
requested from the operator. The initial review's separate 1–15 numbering above
must not be mistaken for the missing follow-up findings.

| Finding | Disposition | Change or follow-up |
|---|---|---|
| 5 | Deferred: review text unavailable | Requires the follow-up report to identify and assess this finding. |
| 6 | Deferred: review text unavailable | Requires the follow-up report to identify and assess this finding. |
| 7 | Deferred: review text unavailable | Requires the follow-up report to identify and assess this finding. |
| 8 | Deferred: review text unavailable | Requires the follow-up report to identify and assess this finding. |
| 9 | Deferred: review text unavailable | Requires the follow-up report to identify and assess this finding. |
| 10 | Deferred: review text unavailable | Requires the follow-up report to identify and assess this finding. |
| 11 | Deferred: review text unavailable | Requires the follow-up report to identify and assess this finding. |
| 12 | Deferred: review text unavailable | Requires the follow-up report to identify and assess this finding. |
| 13 | Deferred to AQ-03 as recommended; interim behavior documented | Reject pre-contract stores before decoding or repair, with a store-format/value-domain diagnostic rather than a disk-corruption diagnosis. See the qualification below concerning `TruncatePartial`. |
| 14 | Fixed | Both live dependency-failure propagation and the catch-up cancellation path now release concurrency keys after durable cancellation succeeds. Two dispatch regression tests prove that a competing run completes without restarting after a `HoldDuringRetry` run is canceled from `Suspended`. |

### Interim persistence behavior and AQ-03 handoff

Pre-AQ-02 `TaskCreated` records can have routing labels that the former validator
accepted but `ExecutorTraits` now rejects: whitespace, labels longer than 128
bytes, or collections with more than 64 entries. The postcard layout remains
readable, but deserialization rejects those values. The streaming WAL reader maps
that rejection to `WalCorruption { reason: DecodeFailure, .. }`, so the default
`RepairPolicy::Strict` refuses startup without distinguishing the pre-contract
value domain from disk corruption. AQ-03 must reject nonempty pre-contract stores
without modifying their bytes, before this decode/repair path, and report the
missing/unsupported target lineage explicitly. AQ-02 does not provide a migration.

The claim in finding 13 that `TruncatePartial` can cut only a trailing record is
**disagreed with for the current implementation**. Although its rustdoc promises
that restriction, `WalFsWriter::load_current_sequence_lenient` returns a truncation
offset at the first `StreamingReadResult::Corruption`, without inspecting later
records. `new_with_repair` passes that offset to `truncate_to_last_valid`, whose
`set_len` removes the entire suffix. Thus a rejected legacy value followed by valid
records can cause those later records to be removed when the lenient writer is
opened directly. This does not occur under the default strict policy. AQ-03 must
also enforce the documented trailing-only repair restriction for recognized target
stores; the manifest check alone is not a replacement for that restriction.

A local probe confirmed this distinction using two CRC-valid `TaskCreated`
frames (118 bytes total). In the first frame, a valid routing label was changed
to begin with whitespace and its CRC recomputed; the second frame still decoded
successfully. Strict opening rejected the first frame and preserved all 118 bytes.
Direct `TruncatePartial` opening succeeded at sequence 0 and shortened the file to
zero bytes, removing the valid second frame as well. The probe used only a disposable
WAL inside this worktree, never an operator store.

The finding 14 fix covers today's reachable suspension behavior. Persisting and
selecting `HoldWhileAwaiting`, testing that it retains the key while waiting, and
durably canceling active waits remain in AQ-03/AQ-06 as tracked in follow-up finding
1 and initial finding 6. Both dependency cancellation paths use the existing
terminal-state helper, which releases keys irrespective of retry/wait hold policy.

The dispatch regression fixture implements the currently supported handler API.
It adds a test-only `HandlerOutput` use in `dispatch.rs`; the boundary policy's
file-count ceiling is consciously raised from 43 to 44 for that one file. Its
removal stage remains `report` with AQ-08 responsible for replacement. The new
tests fail on both cancellation paths without the two release calls (the competing
run stays `Ready`) and pass with them.

### Addendum verification

Final checks on 2026-09-09:

| Check | Result |
|---|---|
| `cargo test --workspace` | 942 passed, 1 ignored |
| `cargo test --workspace --features workflow` | 976 passed, 1 ignored |
| `cargo test --workspace --features workflow,budget,actor,platform` | 1,012 passed, 1 ignored |
| `cargo test -p actionqueue-core --no-default-features` | 87 passed |
| `cargo aq-conformance` | 39 passed, 1 ignored |
| `cargo build --workspace` | Passed |
| `cargo fmt --all -- --check` | Passed (stable rustfmt reports existing nightly-option warnings) |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `git diff --check 8bc7a2b` | Passed |

The first matrix/conformance runs caught the new test fixture's legacy-symbol
footprint increase. All affected checks were rerun successfully after the explicit
policy adjustment described above. The ignored test remains the existing AQ-03
pre-contract-store rejection scaffold. No frozen contracts or evidence were edited.

## Follow-up verification

Checks rerun on 2026-09-09 for the follow-up changes:

| Check | Result |
|---|---|
| `cargo test --workspace` | 940 passed, 1 ignored |
| `cargo test --workspace --features workflow` | 974 passed, 1 ignored |
| `cargo test --workspace --features workflow,budget,actor,platform` | 1,010 passed, 1 ignored on rerun |
| `cargo test -p actionqueue-core --no-default-features` | 87 passed |
| `cargo aq-conformance` | 39 passed, 1 ignored |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `git diff --check 8bc7a2b` | Passed |

The first full-feature run failed `triad_mvp_full_workflow`: its PID-based fixture
directory already existed from an earlier run, and the recovered audit ledger
contained four entries instead of two. The existing WAL doubled in size during
that run. The complete full-feature matrix passed on rerun without code changes.
The fixture-isolation issue remains outside this vocabulary review's scope.

Existing scheme boundary tests cover the unchanged 64-byte ceiling. No additional
tests were needed for the documentation and constant extraction.

## Prior remediation verification

All checks passed on 2026-09-09:

| Check | Result |
|---|---|
| `cargo test --workspace` | 940 passed, 1 ignored |
| `cargo test --workspace --features workflow` | 974 passed, 1 ignored |
| `cargo test --workspace --features workflow,budget,actor,platform` | 1,010 passed, 1 ignored |
| `cargo test -p actionqueue-core --no-default-features` | 87 passed |
| `cargo aq-conformance` | 39 passed, 1 ignored |
| `cargo build --workspace` | Passed |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets -- -D warnings` | Passed |
| Same Clippy check with `--features workflow`, `budget`, `actor`, or `platform` (separately), and with `--all-features` | All passed |
| `git diff --check` | Passed |

The ignored test is the existing AQ-03 pre-contract-store rejection scaffold.
No frozen evidence or contract files were changed.
