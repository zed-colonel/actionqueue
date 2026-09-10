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

## Verification

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
