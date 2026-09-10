# AQ-04 review remediation

Date: 2026-09-10. Branch: `ct/aq-04-2d704923`.
Latest review source: `a0dbf8b4-028e-4a78-8578-7763bf8287c3`.
Original review source: `47842317-afae-4bff-833d-0e5918d0bfec`.

## Finding disposition

F-001: **Fixed** in `97f1cd0`, verified resolved by the latest review.
F-002: **Fixed**, as an implementation claim for the next reviewer to verify.
No findings are disputed or deferred.

`build_dependency_gate` now recomputes every declared task's eligibility after
restoring prerequisite completion from the durable projection. Both admission
publication and bootstrap use this helper, so completed prerequisites can unblock
new admissions and existing dependents after either kind of rebuild.

`DependencyGate` tracks terminal success separately from eligibility to execute.
Recomputation consults only completed prerequisites. This separation is needed
because recomputing a chain in arbitrary map order must not treat an eligible,
unfinished task as a completed prerequisite. Completion notifications, restored
terminal states, failure propagation, and garbage collection maintain the new
ephemeral completion set. Persistence formats and admission digests are unchanged.

## Regression evidence

The three runtime acceptance tests in
`tests/acceptance/dispatch_invariants.rs::admission_dependencies` run in the existing
`acceptance_dispatch_invariants` binary and in `cargo aq-conformance`.
Each exercises live operation, WAL recovery, and snapshot recovery:

- A new task admitted after its prerequisite completes dispatches successfully;
  retry retains the original admission sequence and does not duplicate the task.
- An unrelated `submit_task` preserves eligibility of an existing dependent whose
  first repeat run already completed and whose next run is waiting only on time.
- Restoring eligibility of an intermediate task does not unblock downstream
  tasks until all of the intermediate task's runs are terminal. An additional
  admission and restart with a partially completed repeat task preserve blocking.

Recovery assertions compare complete projection digests and explicitly verify
whether a snapshot was loaded. Execution assertions check exact dispatch counts,
run counts, and run states. All three tests failed against the original code and
pass with the fix. The dependency gate's unit tests also cover a newly declared
dependent of an eligible task and both orders of eligibility recomputation.

## F-002: tracked-file boundary failure

The latest review supersedes the earlier passing-check claim: the F-001 checks
ran before the new regression file was tracked. Once tracked, that file increased
the `HandlerOutput` footprint to 45 files, exceeding the unchanged allowance of 44.
The boundary test reproduced this failure before remediation.

The three regressions now live alongside the existing runtime dispatch invariants
and reuse that file's `AlwaysSuccessHandler`. Their assertions and live/WAL/snapshot
recovery scenarios are preserved. The standalone regression source and its module
declaration are removed. The conformance alias and admission test inventory now
include `acceptance_dispatch_invariants`, retaining the regressions in the standalone
gate and adding its existing dispatch invariant checks. Conformance package revision
3 records this expanded gate; contract revisions, boundary policy, and production
code are unchanged.

All changed files are staged before running the final checks so the tracked-file
scan sees the complete proposed tree.

## Verification (F-002 remediation)

All Cargo commands run from the worktree root with the worktree-local Cargo cache,
build directory, and temporary directory. Dependency resolution uses `--offline`;
debug symbols and incremental compilation are disabled for these checks.

| Command | Result |
|---|---|
| `cargo test --test conformance_contract_boundaries legacy_symbols_respect_their_removal_stage -- --nocapture` | Before: fails, 45 files; after: passes, 44 files |
| `cargo test --test acceptance_dispatch_invariants admission_dependencies::` | 3 passed |
| `cargo test --workspace` | 921 passed, 0 failed |
| `cargo test --workspace --features workflow` | 957 passed, 0 failed |
| `cargo test --workspace --features workflow,budget,actor,platform` | 997 passed, 0 failed |
| `cargo aq-conformance` | 87 passed, 0 failed; reports package revision 3 and 44 legacy-handler files |
| `cargo fmt --all -- --check` | Pass |
| `cargo build --workspace` | Pass |
| `git diff --cached --check` | Pass |

The admission conflict, race, crash, and replay exit gate passes. All three F-001
regressions execute in every workspace configuration and standalone conformance.

Focused before/after logs and workspace check logs for this remediation are retained
as `f002-*.log` in the ignored worktree-local `.aq-checks/` directory.
