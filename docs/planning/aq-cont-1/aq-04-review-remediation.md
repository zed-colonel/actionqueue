# AQ-04 review remediation

Date: 2026-09-10. Branch: `ct/aq-04-2d704923`.
Review source: `47842317-afae-4bff-833d-0e5918d0bfec`.

## Finding disposition

F-001: **Fixed**, as an implementation claim for the next reviewer to verify.
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
`tests/acceptance/admission_support/dependencies.rs` are included in the existing
`acceptance_idempotent_admission` binary and therefore in `cargo aq-conformance`.
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

## Verification

All Cargo commands run from the worktree root with the worktree-local Cargo cache,
build directory, and temporary directory. Dependency resolution uses `--offline`;
debug symbols and incremental compilation are disabled for these checks.

| Command | Result |
|---|---|
| `cargo test --test acceptance_idempotent_admission dependencies::` | Original: 3 failed; fixed: 3 passed |
| `cargo test --workspace` | Pass |
| `cargo test --workspace --features workflow` | Pass |
| `cargo test --workspace --features workflow,budget,actor,platform` | Pass |
| `cargo aq-conformance` | Pass |
| `cargo fmt --all -- --check` | Pass |
| `cargo build --workspace` | Pass |
| `git diff --check` | Pass |

The default, workflow, and full-feature workspace runs report 921, 957, and 997
passing tests respectively. The admission conflict, race, crash, and replay exit
gate passes, including the new F-001 admission/recovery cases.

Focused before/after logs and workspace check logs are retained in the ignored
worktree-local `.aq-checks/` directory.
