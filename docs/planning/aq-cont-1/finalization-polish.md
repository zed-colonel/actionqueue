# AQ-CONT-1 plan-wide finalization, polish round 1

Plan version: `3d8a857d-25fe-4202-accf-bb133b5453cb`.
Candidate baseline: `48b84834a690d57dee30b564f2e745d8c856b422`.
Destination inspected: `main` at `97c9dc26c19c697dbfb204ed503e82c5f053394f`,
an ancestor of the candidate. Scope includes the complete integrated candidate,
not just changes made during finalization. Promotion remains operator-owned.

The supplied plan, supporting contracts/profiles, fourteen-item inventory, and
handoff were reconciled before implementation. The corrected handoff has 117
unique findings: five open, 111 resolved, and one withdrawn. Its formatting-only
retry already completed; this round implements the five open findings. Historical
IDs, including the withdrawn `AQ-02.follow-up.11` and the corrected account of
`AQ-02.follow-up.13`, retain their recorded dispositions.

## Finding dispositions

| Finding | Disposition and implementation | Focused evidence |
|---|---|---|
| F-017 | Fixed. Remote selection resolves Scheduled priority from task metadata, matching the snapshot local promotion records; Ready runs retain their persisted priority. Inspection remains read-only. | `acceptance_remote_protocol::remote_priority_matches_local_promotion_before_and_after_recovery` covers different priorities, mixed Scheduled/Ready candidates, WAL/snapshot reopen, maintenance, and claims. Daemon `http_remote_claims_higher_priority_scheduled_work_first` exercises authenticated listing and claim rejection/acceptance. |
| F-018 | Fixed. Durable mutation authorities reject standalone TaskCreate before append, even with test support compiled. Production unbound authorities reject it too. Only unbound test writers retain raw reducer-fixture construction. Additional RunCreate is restricted to admitted, uncanceled cron tasks' next occurrence, occurrence cap, bounded active window, and pristine run state. Public storage documentation uses AdmissionCommit. | `acceptance_idempotent_admission` proves small and oversized raw tasks cannot bypass a one-byte admission quota or leave task/run/admission fragments; reopen stays empty. Cron tests reject full windows, duplicates, skipped occurrences and exhausted caps, then recover valid replenishment. |
| F-019 | Fixed. ContinuationLimits defaults to 100,000 active waits per store and 10,000 per tenant namespace. Both standalone establishment and compound dispositions check capacity before append. WaitIndex maintains total and tenant counts through establishment, closure, WAL replay and snapshot hydration. Resolution/cancellation releases capacity. Lowered operational quotas preserve accepted history and duplicate acknowledgements while preventing new waits. | `acceptance_checkpoint_resume` covers store/tenant rejection, tenant isolation, signal/deadline/cancellation release, WAL/snapshot recovery and lowering quotas. `acceptance_attempt_disposition::active_wait_quota_rejects_entire_compound_disposition` proves rejection leaves checkpoint, children, signals, consumptions and wait state unchanged, and accepted compound history survives lowered limits. |
| F-020 | Fixed. Living ADR index reflects accepted decisions. Admission guidance identifies canonical v2, its lifecycle byte and persisted wait policy; historical v1 vectors remain available. AQ-11 guidance links current AQ-12 authenticated inspection rules. Accounting/disposition milestone statuses are reconciled. | Documentation, rustdoc and frozen-document checks. Hash-pinned contracts, archive and planning package documents are unchanged. |
| F-021 | Fixed. The triad fixture owns an exclusively created tempfile directory for the full engine lifetime; Drop cleans the store. | `acceptance_triad_mvp` in the combined feature matrix. |

## Plan conformance assessment

The following maps every adopted work-item exit gate to the final implementation
and required verification. Individual historical completion reports are supporting
context, not substitutes for testing the combined tree.

| Item | Obligations and combined-tree evidence |
|---|---|
| AQ-01 | Frozen baseline/archive completeness and hashes, tracked-file boundary ratchets, removed-symbol checks, neutral domain vocabulary and ADR inventory: `cargo aq-conformance`. No pinned documents changed. |
| AQ-02 | Bounded vocabulary and validating serde, exhaustive state transitions, routing/authority separation, causal-reference opacity: core default/serde/no-default-features tests and boundary suite. |
| AQ-03 | Fresh-store admission before decode, ownership, deterministic replay/snapshots, fail-closed corruption, backup/restore, feature compatibility: target persistence, process-lock, crash, store-profile and cross-feature suites. |
| AQ-04 | One atomic admission path, canonical equality/conflict, initial runs/dependencies/causal facts, quotas and crash/race recovery: admission, transactional-child and crash proofs, including F-018's negative direct-storage path. |
| AQ-05 | Stable signal identity, tenant matching, deduplication and order, retention accounting/protection: signal canonical/admission/retention/crash suites. |
| AQ-06 | Durable waits, no lost wakeup, deadlines, cancellation, key policy, bounded store/tenant capacity: wait/crash/key/cascade suites plus F-019. |
| AQ-07 | Checkpoint hashes/limits and immutable, attempt-assigned resume context across recovery: checkpoint/resume proofs, now including capacity rejection without partial checkpoint effects. |
| AQ-08 | Legal dispositions, atomic effects and children, physical-versus-failure accounting, stale fences, cancel/expiry races: disposition and continuation crash proofs; F-019 checks complete compound rejection. |
| AQ-09 | Transactional child identity/causality, required/detached lifecycle, DAG gates, parent waits and cancellation: child admission, parent-wait atomicity, hierarchy and coordinator suites. Fixtures now enter through compound admission; exhausted zero-run cron tasks retain their actual terminal semantics. |
| AQ-10 | Budget exhaustion uses Suspended, durable accounting, internal reactive events stay separate from public signals: budget/subscription/developmental feature suites. |
| AQ-11 | Authenticated controls/inspection, tenant permission/revocation, attribution, actor liveness/traits, remote fences and recovery, local/remote scheduling parity: remote/control/actor/cron suites and daemon tests, including F-017. |
| AQ-12 | Versioned operational schemas, bounded pagination/redaction, permissions, telemetry, CLI transport and daemon lifecycle: CLI/daemon workspace tests/builds, metrics/API parity, documentation checks; F-020 reconciles living guidance. |
| AQ-13 | All AQ-H invariants, eighteen AQ-DD cases, model/chaos/recovery cuts, four public drivers and bounded indexed work: conformance/developmental/full report, cross-feature persistence and continuation benchmarks. Package revision 14 records added regression coverage; historical fixture bytes remain unchanged. |
| AQ-14 | Eleven consistent packages, locked dependency/consumer resolution, generated API docs, executable publication preparation and report validation: release metadata/tests, package archives, independent consumer, rustdoc/docs and full-report validation. Actual publication/tagging and external WorldInterface/Exoskeleton certification are operator/downstream actions, not claimed here. |

Cross-item checks specifically cover admission versus cron replenishment and
hierarchy eligibility; tenant wait capacity versus checkpoints, child effects and
recovery; and remote priority versus authenticated actor claims. The full eight
feature profiles exercise architecture boundaries rather than relying only on the
combined build.

## Simplification and scope

The cron window bound now lives in core and is reused by engine derivation and
storage admission/replenishment validation. Wait accounting stores the namespace
with the active run entry rather than maintaining another historical lookup.
Legacy direct-storage fixture setup now uses the same compound admission boundary
as runtime callers, reducing hand-built partial state. No optional wrapper/serde
refactor or new domain concept was introduced. Canonical bytes, persistence schema
versions, intended scheduling policy and frozen acceptance criteria are unchanged.

## Verification record

Focused regression runs passed before the implementation commit, including the
four amended proof binaries, authenticated daemon priority test, triad fixture,
cron/coordination fixtures and crash recovery. Exploratory runs exposed outdated
zero-run Once fixture assumptions after closing F-018; these were replaced with
legitimate exhausted cron admissions and rechecked. Final quality-gate results
are recorded below after completion; scratch logs are never source artifacts.

The first matrix attempt also found missing `workflow` guards on the new cron
fixtures; deadline coverage remains feature-independent and the cron-only cases
are now explicitly gated. Public drivers use the supported `AQ_CLI`/`AQ_ADAPTER`
overrides to locate binaries in the reused scratch Cargo target, after building
those binaries from this checkout. This avoids the harness defaults under the
worktree-local `target/` directory. Failed exploratory logs remain in scratch.

The combined run subsequently reached a budget-gated malformed-filter fixture
that still created a standalone task. Its setup now admits a task and initial run
in the same frame, retaining the exact subscription-corruption and no-repair
assertions. Remaining raw TaskCreate calls are explicit negative tests or unbound
mutation/reducer test writers, not initialized production stores.

## Final verification outcome

All **51 command gates passed** on implementation commit
`f27599049369405f076dd64718a9a8d1e122a12e`: all 47 commands returned by
`scripts/release.py::gates()`, two public-driver build prerequisites, nightly
formatting, and core without default features. The subsequent documentation-only
commit records these results and does not alter the tested implementation.

Commands ran from the repository root with Rust 1.89.0, offline dependencies, the
reused Cargo cache under controller scratch, and loopback access for transport
tests. Public-driver overrides pointed at binaries built from this checkout.

- `cargo test --locked --workspace [--features PROFILE] -- --test-threads=1`
- `cargo clippy --locked --all --all-targets [--features PROFILE] -- -D warnings`
- `cargo build --locked -p actionqueue-cli --no-default-features [--features PROFILE]`
- `cargo build --locked -p actionqueue-daemon --no-default-features [--features PROFILE]`

Each of those four commands passed for all eight profiles: default, `workflow`,
`budget`, `workflow,budget`, `actor`, `platform`, `actor,platform`, and
`workflow,budget,actor,platform`. Also passed:

- `cargo build --locked --workspace`
- `cargo fmt --all -- --check` and `cargo +nightly fmt --all -- --check`
- `cargo fmt --manifest-path examples/downstream-handoff/Cargo.toml -- --check`
- `cargo test --locked -p actionqueue-core --features serde`
- `cargo test --locked -p actionqueue-storage --features serde`
- `cargo test --locked -p actionqueue-core --no-default-features`
- `cargo aq-conformance` and `cargo aq-developmental`
- `cargo run --locked --example aq_conformance --features workflow,budget,actor,platform -- --full --report SCRATCH/conformance-report.json`
- `bash conformance/aq-cont-1/cross-feature-persistence.sh`
- `cargo bench --locked --bench continuation`
- `cargo doc --locked --workspace --no-deps --features workflow,budget,actor,platform` with `RUSTDOCFLAGS=-D warnings`
- `python3 -B -m unittest discover -s tests/release`
- `python3 -B scripts/check-docs.py --api TARGET/doc`
- `python3 -B scripts/check-consumer.py --report SCRATCH/consumer-resolution.json`
- `python3 -B scripts/check-packages.py --output SCRATCH/crates`

The actual full report also passed `release.validate_report`: `passed=true`,
`missing=[]`, package revision 14, and 214 evidence rows. `release.metadata` and
`release.clean` passed on the tested commit. All eleven real crate archives were
created, their production builds passed, and their independent consumer passed.
Benchmarks met their indexed-work bounds at 8, 129 and 257 tasks. Elapsed timings
remain informational and include full-projection/full-WAL costs. Stable rustfmt
continues to emit its existing notices about nightly-only formatting options.

Controller run `2ba026d3-466f-44d9-aca8-9e787ab21197` retains per-command logs,
`results.json`, `report-validation.json`, the full report, performance measurements,
consumer resolution and crate archives in `scratch/final-evidence-v3/`.
The results inventory SHA-256 is
`413e10a9e1396776fc7b87b64de0674679205a12f45b8fa0d5f96c3c9208ad76`.

All fourteen plan obligations above have combined-tree evidence. F-017 through
F-021 have implementation fixes and passing regressions; none is deferred or
disagreed. These are implementation dispositions for the next independent review,
not a replacement for that review. No additional justified behavioral change is
left for this polish round. Publication, tagging, external downstream certification
and promotion remain outside this implementation run. No merge or push occurred.
