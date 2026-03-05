# Acceptance Test Taxonomy

Reference document for ActionQueue's acceptance test suite. Each entry lists the test name,
invariants verified, run policy/scenario exercised, and key assertions.

## Core Contract Tests

### `once_accounting`
- **Invariant:** All runs originate from durable intent; state transitions are monotonic
- **Run policy:** `Once`
- **Scenario:** Create a task with Once policy, derive runs, execute to completion
- **Key assertions:** Exactly 1 run derived, terminal state reached, no extra runs

### `repeat_accounting`
- **Invariant:** All runs originate from durable intent; derivation is deterministic
- **Run policy:** `Repeat(N, interval)`
- **Scenario:** Create a task with Repeat policy, derive runs at scheduled intervals
- **Key assertions:** Exactly N runs derived, each at correct scheduled_at offset

### `retry_cap`
- **Invariant:** max_attempts is never exceeded
- **Run policy:** `Once` with max_attempts > 1
- **Scenario:** Handler returns retryable failures; verify retry count respects cap
- **Key assertions:** Attempt count <= max_attempts, terminal state after cap reached

### `crash_recovery`
- **Invariant:** Completion is durable; WAL recovery restores correct state
- **Run policy:** Mixed
- **Scenario:** Write events to WAL, simulate crash (drop state), re-bootstrap
- **Key assertions:** All committed state restored, in-flight runs re-eligible

### `concurrency_key`
- **Invariant:** Concurrency constraints are core-enforced
- **Run policy:** `Once` with concurrency key
- **Scenario:** Multiple runs share a concurrency key; only one executes at a time
- **Key assertions:** Single-flight per key, second run blocked until first completes

### `observability`
- **Invariant:** Metrics and inspection surfaces are accurate
- **Run policy:** `Once`
- **Scenario:** Execute tasks and verify metrics/inspection endpoints
- **Key assertions:** Task counts, run counts, state counts match expectations

## Extended Contract Tests

### `cancellation`
- **Invariant:** State transitions are monotonic; terminal states are final
- **Run policy:** `Once`
- **Scenario:** Cancel tasks and runs in various states
- **Key assertions:** Canceled tasks/runs reach terminal Canceled state, already-terminal runs unaffected

### `negative_transitions`
- **Invariant:** State transitions are validated; invalid transitions rejected
- **Run policy:** Various
- **Scenario:** Attempt invalid state transitions (backward, skip, from terminal)
- **Key assertions:** All invalid transitions produce validation errors

### `lease_expiry`
- **Invariant:** Lease constraints are core-enforced
- **Run policy:** `Once`
- **Scenario:** Acquire lease, let it expire, verify run becomes re-eligible
- **Key assertions:** Expired lease releases the run, run transitions back to Ready

### `wal_corruption_recovery`
- **Invariant:** Completion is durable; recovery handles corruption gracefully
- **Run policy:** `Once`
- **Scenario:** Write valid events then append trailing corruption to WAL
- **Key assertions:** Trailing corruption truncated, valid events recovered

### `misfire`
- **Invariant:** Scheduled runs follow misfire policy for overdue scheduling
- **Run policy:** `Repeat` with interval
- **Scenario:** Clock advances past scheduled_at without promotion
- **Key assertions:** Misfire policy applied correctly to overdue runs

### `dispatch_invariants`
- **Invariant:** Dispatch loop maintains state machine invariants under all inputs
- **Run policy:** Various (property-based)
- **Scenario:** proptest-driven dispatch loop exercising random event sequences
- **Key assertions:** No invariant violations across random inputs

### `concurrent_dispatch_stress`
- **Invariant:** Dispatch loop handles concurrent execution under load without state violations
- **Run policy:** `Once` (no concurrency keys)
- **Scenario:** Submit 100+ tasks with 4 concurrent workers, run until idle
- **Key assertions:** All runs reach terminal state, no runs stuck in Running/Leased

### `snapshot_corruption_recovery`
- **Invariant:** Snapshots are derived artifacts; corrupt snapshot falls back to WAL
- **Run policy:** `Once`
- **Scenario:** Write valid snapshot, corrupt it, bootstrap
- **Key assertions:** Corrupt snapshot detected, WAL-only replay succeeds

### `concurrent_mutation_boundary`
- **Invariant:** Mutation authority enforces sequence monotonicity
- **Run policy:** `Once`
- **Scenario:** Submit commands with duplicate/regressing sequence numbers
- **Key assertions:** Sequence collisions rejected, monotonicity proof

### `mixed_attempt_outcomes`
- **Invariant:** Attempt lineage preserved across different outcome types
- **Run policy:** `Once` with max_attempts > 1
- **Scenario:** Handler returns timeout, then failure, then success across attempts
- **Key assertions:** Each attempt has correct lineage, final state reflects last outcome

### `crash_during_promotion`
- **Invariant:** Completion is durable; promoted runs survive crash and are not re-promoted
- **Run policy:** `Repeat(3, 100)` with scheduled-to-ready promotion
- **Scenario:** Submit task, promote all runs to Ready, simulate crash (drop authority),
  re-bootstrap from WAL, verify all runs retain Ready state
- **Key assertions:** Pre-crash ready count equals post-crash ready count; no runs revert
  to Scheduled; all run IDs preserved across restart

### `multi_mutation_session`
- **Invariant:** Mutation authority validates all command types within a single session; WAL
  append pipeline is correct across diverse command types (validated mutation lanes 4.1,
  WAL admission rules 6.3)
- **Run policy:** `Once`
- **Scenario:** Bootstrap once, perform 7 sequential mutations (TaskCreate x2, RunCreate,
  RunStateTransition, EnginePause, EngineResume, TaskCancel) through a single authority
  session, verify projection consistency after each step, then re-bootstrap from WAL and
  verify replay matches
- **Key assertions:** Projection state correct after each mutation, WAL replay produces
  identical state, sequence advances monotonically, task cancel does not affect unrelated tasks

## Workflow Feature Tests (require `--features workflow`)

### `handler_output_roundtrip`
- **Invariant:** Handler output bytes round-trip through WAL and projection
- **Run policy:** `Once`
- **Scenario:** Handler returns output bytes, verify they persist in WAL events and are
  accessible via projection attempt history
- **Key assertions:** Output bytes match in WAL event, attempt history entry, and after recovery

### `dag_ordering`
- **Invariant:** DAG dependencies enforce execution order
- **Run policy:** `Once` with dependency declarations
- **Scenario:** Three-task linear DAG (A → B → C); run until idle
- **Key assertions:** Tasks execute in dependency order, all reach Completed

### `dag_failure_propagation`
- **Invariant:** Failed prerequisite cascades cancellation to dependents
- **Run policy:** `Once` with dependency declarations
- **Scenario:** Three-level DAG where root task fails
- **Key assertions:** Failed root causes dependent tasks to be canceled

### `dag_cycle_rejection`
- **Invariant:** Circular dependencies are rejected at declaration time
- **Run policy:** `Once` with circular dependency declarations
- **Scenario:** Attempt to declare A → B → A cycle
- **Key assertions:** Cycle detection error returned, no partial graph applied

### `hierarchy_lifecycle`
- **Invariant:** Parent-child hierarchy enforces cascade cancellation and completion gating
- **Run policy:** `Once` with parent-child relationships
- **Scenario:** Cancel parent, verify children cascade; verify parent waits for children
- **Key assertions:** Cascade cancellation propagates, parent gated on child completion

### `dynamic_submission`
- **Invariant:** Coordinator handlers can submit child tasks via SubmissionChannel
- **Run policy:** `Once` with coordinator handler
- **Scenario:** Parent handler submits child tasks dynamically during execution
- **Key assertions:** Child tasks created, executed, and parent completes after children

### `coordinator_multi_attempt`
- **Invariant:** ChildrenSnapshot accurately reflects child state for coordinator handlers
- **Run policy:** `Once` with coordinator handler and retries
- **Scenario:** Coordinator handler receives ChildrenSnapshot on each attempt
- **Key assertions:** ChildrenSnapshot populated correctly across retry attempts

### `cron_scheduling`
- **Invariant:** Cron policy derives runs on schedule with rolling window
- **Run policy:** `Cron` with cron expression
- **Scenario:** Submit cron task, advance clock, verify run derivation
- **Key assertions:** Runs derived at correct intervals, rolling window maintained

### `workflow_crash_recovery`
- **Invariant:** Workflow features (DAG, hierarchy, cron) survive WAL recovery
- **Run policy:** Mixed workflow policies
- **Scenario:** Submit workflow tasks, crash, recover from WAL
- **Key assertions:** All workflow state restored correctly after recovery

### `dag_snapshot_recovery`
- **Invariant:** DAG dependency declarations survive snapshot-based recovery
- **Run policy:** `Once` with dependency declarations
- **Scenario:** Submit three-task linear DAG (A → B → C), declare dependencies via mutation
  authority, run step A to completion, force snapshot write (threshold=1), restart from
  snapshot + WAL tail, run remaining tasks to completion
- **Key assertions:** Dependency declarations present after snapshot recovery, all three tasks
  complete in DAG order, execution log confirms A → B → C ordering

### `attempt_lineage`
- **Invariant:** RunId is stable across retries; AttemptIds are unique per attempt; attempt
  history records failures and eventual success with output
- **Run policy:** `Once` with max_attempts=3
- **Scenario:** Handler fails first 2 attempts, succeeds with output on attempt 3; verify
  single RunId, 3 unique AttemptIds, correct error/output payloads; restart from WAL and
  verify lineage survives recovery
- **Key assertions:** Single RunId across all attempts, 3 distinct AttemptIds, failure entries
  have error messages, success entry has output bytes, lineage survives WAL recovery

## Chaos Test

### `kill_recovery` (`tests/chaos/`)
- **Invariant:** Completion is durable under abrupt termination
- **Run policy:** `Once`
- **Scenario:** Write WAL events via mutation authority, simulate kill -9 via `std::mem::forget` (skipping Drop/close), recover from storage
- **Key assertions:** WAL recovery succeeds, all committed events present, new operations succeed after recovery
