# AQ-02 review remediation

## PR description

AQ-02 establishes pure identifiers, bounded values, causal attribution, admission
and continuation shapes, exact `Awaiting` transitions, and executor-trait routing
terminology across dependent crates. Collection and label failures now have distinct
executor-trait errors. Dependency cancellation releases held concurrency keys only
after the durable cancellation succeeds; regression tests cover both cancellation
paths. Continuation execution remains gated on later persistence/runtime work.

Compatibility is limited to the legacy executor-trait vector's postcard field
layout. Deserialization applies new grammar and count/length ceilings, so formerly
accepted WAL and snapshot values can fail. Snapshot JSON routing keys changed
without aliases or a schema bump. AQ-03 establishes the fresh store lineage; AQ-02
provides no migration guarantee. The representation decisions and remaining gates
are recorded below.

- **Contract clauses implemented:** `AQ-CONT-1` §§2–4 and §6 neutrality boundaries;
  normative architecture §§7–8 (vocabulary/lifecycle), §§9.2 and 10.3 (pure signal
  and wait shapes), §13.1 (pure admission plan), §§15–16 (causal attribution and
  routing). These are type-level foundations, not completed durable protocols.
- **Invariant IDs covered:** AQ-H1, AQ-H10–AQ-H13, AQ-H15, AQ-H17, AQ-H19, AQ-H20;
  structural foundations for AQ-H4 and AQ-H6–AQ-H9. Atomic admission, wait
  establishment, wake-up, and disposition enforcement remain downstream work.
- **Persistence records added or changed:** no new compound WAL record family;
  append-only `Awaiting` state/result enum additions and routing terminology in
  existing task constraints. JSON field/value domains change as described above.
  ADR-001 owns the fresh lineage; ADR-007/009 record wait/key choices. Target
  records and persisted wait-policy selection arrive in AQ-03/AQ-06.
- **Crash points tested:** existing workspace recovery, replay, and chaos suites;
  no new continuation crash protocol is claimed. New dependency-cancellation
  tests verify durable cancellation precedes observable key reuse without restart;
  they do not inject a crash. AQ-03/AQ-06 must add the new protocol crash matrix.
- **Public APIs added or removed:** target IDs, bounded values, `CausalContext`,
  `ControlMutationContext`, signal/wait/checkpoint/resume types, admission and
  disposition shapes, `ConcurrencyKeyWaitPolicy`, and `RunState::Awaiting`;
  executor-trait routing replaces the legacy routing symbols. `ExecutorTraitError`
  now distinguishes collection count from indexed label validation failures.
- **Forbidden domain concepts checked:** conformance checks enforce absence of
  queue-owned campaign, intervention-arm, benchmark, evaluation, binding-constraint,
  saturation, and free-form metadata concepts; attribution does not affect
  scheduling or authority. Routing traits grant no permission.
- **Downstream contract unlocked:** AQ-03 can establish the fresh durable lineage
  using these types; AQ-04/AQ-05/AQ-06 can implement admission, signals, and waits.
  No external consumer is promised working continuation execution yet.

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

The operator supplied the missing follow-up findings 5–12 after the addendum
remediation commit. This section and the follow-up table above now account for
all 14 findings. The initial review has a separate 1–15 numbering sequence.

| Finding | Disposition | Change or follow-up |
|---|---|---|
| 5 | Fixed (tracking already present) | AQ-06 must cancel active waits durably and map the authority guard before Awaiting becomes reachable, including preflight before the task path commits `TaskCancel`. Existing handler comments and initial finding 6 track this gate; no further behavior is enabled now. |
| 6 | Fixed | The representation decisions below document `CausationLink`, `WaitDeadline`, and the single structural parent source for AQ-04/AQ-05/AQ-06. |
| 7 | Fixed | Replaced the bare error alias with `Collection { count }` and `Label { index, source }`. Constructor, TaskConstraints display, and JSON errors identify the failing domain. Input count deliberately remains bounded before deduplication; boundary tests and documentation make this explicit. |
| 8 | Fixed | Replaced the Proposed ADR-012 citation with normative invariant AQ-H13. Runtime accounting already explains the rule without that citation. ADR-012 remains Proposed for AQ-08. |
| 9 | Fixed | The `matches!` table is now the sole production eligibility source; rejection reasons classify only table misses. The independent exhaustive test oracle is retained to detect changes to the allowed edge set. |
| 10 | Fixed | Moved the wait policy above the test module and the integration paragraphs into their own README section before License. |
| 11 | Deferred: optional refactor | Keep the explicit UUID wrappers, OpaqueRef redaction/accessor implementation, and validating Wire mirrors in this remediation. Their APIs are not identical, and a shared macro must preserve serde field order, redaction, and constructor validation. A dedicated refactor can address duplication with focused compatibility tests; no behavior defect requires it here. |
| 12 | Fixed | The PR description above now uses every field from implementation-plan §2.3 and is ready to copy when the operator opens the PR. |
| 13 | Deferred to AQ-03 as recommended; interim behavior documented | Reject pre-contract stores before decoding or repair, with a store-format/value-domain diagnostic rather than a disk-corruption diagnosis. See the qualification below concerning `TruncatePartial`. |
| 14 | Fixed | Both live dependency-failure propagation and the catch-up cancellation path now release concurrency keys after durable cancellation succeeds. Two dispatch regression tests prove that a competing run completes without restarting after a `HoldDuringRetry` run is canceled from `Suspended`. |

### Type representation decisions (follow-up finding 6)

These are intentional deviations from the normative architecture's example Rust
shapes, recorded here without changing the hash-pinned contract documents:

| Normative shape | AQ-02 representation and rationale | Downstream obligation |
|---|---|---|
| §9.2 `SignalEnvelope::causation_id: Option<CausationId>` | `causation: Option<CausationLink>` embeds the same validated structural task/run/attempt ancestry or bounded external reference used by `CausalContext`. No standalone `CausationId` or causal-object lookup is introduced. Empty links and inconsistent ancestry are rejected. This changes the field name and representation, not just an alias. | AQ-05 persists the validated link as immutable, non-authorizing attribution; it must not invent a separate causation registry or silently serialize the old field name. |
| §10.3 `deadline_at: Option<u64>` plus `timeout_policy` | `deadline: Option<WaitDeadline>` holds `at` and `policy` together. No deadline means no timeout action; an active deadline always has an explicit policy. This excludes meaningless detached timeout policies. | AQ-06 persists the optional pair and applies that recorded policy on timeout. No default deadline or timeout action is inferred for `None`. |
| §13.1 `AdmissionPlan::parent_task_id` alongside `task_spec` | The structural parent is read from `plan.task_spec().parent_task_id()`. A second field could disagree with the task specification. Causal ancestry is attribution and does not replace the structural parent. | AQ-04 uses the task specification's parent for validation and canonical admission hashing; AQ-06 uses the durably admitted structure when establishing parent/child waits. |

The executor-trait count limit is likewise an **input** limit: at most 64 entries
are validated and then sorted/deduplicated. Sixty-four copies of one valid label
produce a singleton set; 65 copies fail with `Collection { count: 65 }`. Keeping
this bound avoids allowing duplicate-heavy inputs to bypass the collection ceiling
and preserves the current value domain. JSON and postcard decoding use the same
constructor. Labels still have an independent 128-byte ceiling, and indexed label
errors never echo the supplied label.

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

JSON snapshots break in two different ways, and only one of them is loud. The
routing key under task constraints and the actor executor-trait key were renamed
without aliases or a schema bump. A pre-contract snapshot that contains actors
fails to decode and falls back to full WAL replay, which is visible. A pre-contract
snapshot whose tasks carry the old routing key still decodes, because unknown keys
are ignored, so the executor-trait requirement is **silently dropped** and those
tasks become routable to any executor until a later WAL replay rebuilds them. AQ-03
must therefore reject pre-contract snapshots by lineage or manifest before decoding
them; the decode failure alone does not catch the routing-key case.

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

### Findings 5–12 verification

Final checks on 2026-09-09:

| Check | Result |
|---|---|
| `cargo test --workspace` | 943 passed, 1 ignored |
| `cargo test --workspace --features workflow` | 977 passed, 1 ignored |
| `cargo test --workspace --features workflow,budget,actor,platform` | 1,013 passed, 1 ignored |
| `cargo test -p actionqueue-core --no-default-features` | 88 passed |
| `cargo aq-conformance` | 39 passed, 1 ignored |
| `cargo build --workspace` | Passed |
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `git diff --check 8bc7a2b` | Passed |

The first default/workflow runs caught an existing serde assertion that an empty
collection's diagnostic must include "empty". The new collection error now retains
that wording while distinguishing it from an empty label; both matrices passed on
rerun. The new test exercises collection/label classification through construction,
TaskConstraints, JSON diagnostics, and postcard rejection, including the raw-input
deduplication ceiling. Existing exhaustive transitions and serialization tests
remain green. The ignored test is still the AQ-03 pre-contract-store scaffold.
No frozen contract/evidence files or ADR acceptance records were changed.

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

## Mergeable-review dispositions

The final review returned a mergeable verdict with thirteen non-blocking findings.
None of the changes below alters a durable layout or a contract surface.

| Finding | Disposition | Change or follow-up |
|---|---|---|
| 1 | Fixed | Every cancellation cascade now goes through `cancel_run_and_release_key`, which releases only when `KeyGate::key_holder` names the canceled run. Scheduled and Ready runs release nothing and emit no warning. A dispatch test captures WARN output and proves the spurious "key not held" line is gone. |
| 2 | Fixed | The helper keeps the key for a run still present in `in_flight`, so a cascade cannot let a competitor start under the same key while a worker executes. A dispatch test cancels a Running run with a blocked worker and asserts the key is still held. The helper's rustdoc records the pre-existing gap: today the worker's later result is rejected by the authority's previous-state check, so the slot is freed on restart from the projection. Reconciling in-flight workers with cascade cancellation is shared with the hierarchy cascade and stays outside AQ-02. |
| 3 | Fixed | `KeyLifecycleContext` carries a `ConcurrencyKeyWaitPolicy` (`with_wait_policy`, default release). Running to Awaiting consults it; Awaiting to a terminal state releases unconditionally, matching the dispatch rule. Four evaluator unit tests cover default release, hold, terminal release, and Awaiting to Ready. Having dispatch consume the evaluator is a larger refactor with no behaviour defect behind it; the two sites now cross-reference each other. |
| 4 | Fixed | The actor registry rebuild logs a warning naming the actor and the grammar error before substituting the placeholder trait. Typing the WAL and record fields as `ExecutorTraits` waits for AQ-03 because the WAL layout is frozen until then. |
| 5 | Fixed | `AdmissionPlan::new` rejects a repeated `RunId` with the new `AdmissionRejection::DuplicateRun`; the serde mirror routes through the same constructor, and the test covers construction and JSON decode. |
| 6 | Fixed | The dispatch Awaiting arm now states that the accessor returns the default until AQ-03 adds the persisted field, so `HoldWhileAwaiting` is not selectable there yet. |
| 7 | Fixed | The AQ-03 handoff above now names the silent-drop case for pre-contract JSON snapshots with the old routing key and requires lineage rejection before decode. |
| 8 | Fixed | `AttemptOutcome::awaiting()` exists and the replay reducer uses it instead of the validating constructor with an `expect`. |
| 9 | Fixed | Core gains `RunState::ALL` and `RunState::label()`; `Display` uses the label. The Prometheus label set is derived from `RunState::ALL` at compile time, so `suspended` is now pre-seeded. The HTTP and CLI stats breakdowns count `suspended` and `awaiting` (additive JSON fields; the CLI text output gains two lines), and their state matches are exhaustive with no discard arm. The daemon parity test and the acceptance observability test derive the expected set from the same constant. |
| 10 | No change (agreed) | Awaiting is unreachable until AQ-06, which owns cancelling active waits durably and mapping the authority guard; the handler comments and initial finding 6 track it. |
| 11 | Fixed | Each of the six Accepted ADRs has a "Deferred verification" row in its acceptance record naming what AQ-02 verified and which work item owns the rest. |
| 12 | Fixed | `crates/actionqueue-core/tests/common/mod.rs` holds `hash`, `hash_filled`, and the JSON/postcard `round` helper; the three test files use it. |
| 13 | Fixed | See finding 1. The helper makes release inseparable from cancellation on all three cascade paths. |

### Mergeable-review verification

Checks run on 2026-09-09 on the final tree:

| Check | Result |
|---|---|
| `cargo test --workspace` | 949 passed, 1 ignored |
| `cargo test --workspace --features workflow` | 983 passed, 1 ignored |
| `cargo test --workspace --all-features` | 1,019 passed, 1 ignored |
| `cargo test -p actionqueue-core --no-default-features` | 88 passed, no warnings |
| `cargo aq-conformance` | 39 passed, 1 ignored |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed |
| `cargo fmt --all -- --check` | Passed |
| `git diff --check 8bc7a2b` | Passed |

The first full run failed the daemon metrics parity test and the acceptance
observability suite: both hardcoded the nine-label set and its sample count. They
now derive the expected set from `RUN_STATE_LABEL_VALUES`, so `suspended` is
checked alongside the other states. The ignored test remains the AQ-03
pre-contract-store scaffold. No frozen contract, planning-package, or archive file
was changed; this log is not hash-pinned.

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
