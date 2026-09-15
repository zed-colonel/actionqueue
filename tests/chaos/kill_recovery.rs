//! P6-009 Chaos tests: simulated kill -9 recovery.
//!
//! These tests exercise WAL durability under abrupt process termination by:
//!   1. Writing state via the mutation authority (task creation, run creation, transitions),
//!   2. Simulating kill -9 by dropping the authority without a durability sync,
//!   3. Re-opening from storage via `load_projection_from_storage`,
//!   4. Verifying recovered state matches expectations,
//!   5. Verifying new operations succeed after recovery.

#[path = "../acceptance/lease_support.rs"]
mod lease_support;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use actionqueue_core::ids::{AttemptId, RunId, TaskId};
use actionqueue_core::mutation::{
    AttemptFinishCommand, AttemptOutcome, AttemptStartCommand, DurabilityPolicy, MutationAuthority,
    MutationCommand, RunStateTransitionCommand,
};
use actionqueue_core::run::state::RunState;
use actionqueue_core::run::RunInstance;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_storage::mutation::authority::StorageMutationAuthority;
use actionqueue_storage::recovery::bootstrap::load_projection_from_storage;
use actionqueue_storage::recovery::reducer::ReplayReducer;
use actionqueue_storage::wal::fs_writer::WalFsWriter;
use actionqueue_storage::wal::InstrumentedWalWriter;

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Creates a unique temporary data directory for each test under the configured temporary directory.
///
/// Matches the pattern used by acceptance tests for consistent temporary-directory
/// locality and easier cleanup.
fn unique_data_dir(label: &str) -> PathBuf {
    let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir =
        std::env::temp_dir().join(format!("chaos-{}-{}-{}", label, std::process::id(), count));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("chaos data dir should be creatable");
    dir
}

/// Creates a valid TaskSpec with the given payload and RunPolicy::Once.
fn make_task_spec(payload: &[u8]) -> TaskSpec {
    TaskSpec::new(
        TaskId::new(),
        TaskPayload::with_content_type(payload.to_vec(), "application/octet-stream"),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("task spec should be valid")
}

/// Creates a valid TaskSpec with a specific TaskId.
fn make_task_spec_with_id(task_id: TaskId, payload: &[u8]) -> TaskSpec {
    TaskSpec::new(
        task_id,
        TaskPayload::with_content_type(payload.to_vec(), "application/octet-stream"),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("task spec should be valid")
}

/// Opens storage and returns a fresh mutation authority.
fn open_authority(
    data_dir: &std::path::Path,
) -> StorageMutationAuthority<InstrumentedWalWriter<WalFsWriter>, ReplayReducer> {
    let recovery =
        load_projection_from_storage(data_dir).expect("storage bootstrap should succeed");
    StorageMutationAuthority::new(recovery.wal_writer, recovery.projection).with_host(
        actionqueue_core::control::HostControlContext {
            actor_id: None,
            scope: actionqueue_core::control::ControlScope::SingleTenant,
            attribution: actionqueue_core::causal::ControlMutationContext::new(
                actionqueue_core::bounded::OpaqueRef::new("fixture-host").unwrap(),
            ),
        },
    )
}

/// Computes the next WAL sequence from the projection's latest.
fn next_seq(
    authority: &StorageMutationAuthority<InstrumentedWalWriter<WalFsWriter>, ReplayReducer>,
) -> u64 {
    authority.projection().latest_sequence().checked_add(1).expect("sequence should not overflow")
}

/// Commits a Once task and its chosen initial run in one admission frame.
fn admit_once(
    authority: &mut StorageMutationAuthority<InstrumentedWalWriter<WalFsWriter>, ReplayReducer>,
    spec: TaskSpec,
    run_id: RunId,
) {
    use actionqueue_core::admission::{AdmissionPlan, EnsureTaskRequest};
    let seq = next_seq(authority);
    let run = RunInstance::new_scheduled_with_id(run_id, spec.id(), seq, seq).unwrap();
    let request = EnsureTaskRequest::for_task(spec, vec![]).unwrap();
    let digest = request.digest().unwrap();
    let plan = AdmissionPlan::new(request, vec![run], digest).unwrap();
    let _ = authority
        .submit_command(
            MutationCommand::AdmissionCommit(
                actionqueue_core::mutation::AdmissionCommitCommand::new(seq, plan, None, seq),
            ),
            DurabilityPolicy::Immediate,
        )
        .unwrap();
}

/// Simulates loss of the process-owned projection after durable, unbuffered writes.
/// Dropping releases the OS lock without a WAL sync. A separate target conformance
/// child-process test proves real SIGKILL lock release and durable recovery.
fn simulate_kill9(
    authority: StorageMutationAuthority<InstrumentedWalWriter<WalFsWriter>, ReplayReducer>,
) {
    // WAL writes are unbuffered; dropping releases the OS lock without a sync.
    drop(authority);
}

// ---------------------------------------------------------------------------
// Scenario A: Crash after compound admission, before scheduling.
//
// Submit 3 tasks, crash immediately. Recovery should see all tasks and initial runs.
// Then promote an admitted run and verify operations proceed.
// ---------------------------------------------------------------------------

#[test]
fn crash_after_compound_admission_before_scheduling() {
    let data_dir = unique_data_dir("scenario_a");

    // Phase 1: Create tasks, then kill -9.
    let task_ids: Vec<TaskId> = {
        let mut authority = open_authority(&data_dir);

        let mut ids = Vec::new();
        for i in 0u8..3 {
            let spec = make_task_spec(&[10 + i, 20 + i]);
            let task_id = spec.id();
            admit_once(&mut authority, spec, RunId::new());
            ids.push(task_id);
        }

        // Simulate kill -9: forget authority without close.
        simulate_kill9(authority);
        ids
    };

    // Phase 2: Recovery — verify all 3 complete admissions.
    {
        let recovery =
            load_projection_from_storage(&data_dir).expect("recovery should succeed after crash");

        assert_eq!(
            recovery.projection.task_count(),
            3,
            "all 3 tasks must survive kill -9 recovery"
        );
        assert_eq!(recovery.projection.run_count(), 3, "all initial runs survive with their tasks");

        for task_id in &task_ids {
            assert!(
                recovery.projection.get_task(task_id).is_some(),
                "task {task_id} must be recoverable after crash"
            );
        }
    }

    // Phase 3: Post-recovery — promote an admitted run.
    {
        let mut authority = open_authority(&data_dir);

        // The run was committed atomically with the task before the crash.
        let run_id = authority.projection().run_ids_for_task(task_ids[0])[0];
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq,
                    run_id,
                    RunState::Scheduled,
                    RunState::Ready,
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("transition Scheduled->Ready should succeed post-recovery");

        assert_eq!(
            authority.projection().get_run_state(&run_id),
            Some(&RunState::Ready),
            "run should be in Ready state after post-recovery transition"
        );
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

// ---------------------------------------------------------------------------
// Scenario B: Crash after run creation, during state transitions.
//
// Create task, create run, drive through Scheduled -> Ready -> Leased -> Running,
// then crash. Recovery should show the run in Running state.
// Continue with attempt lifecycle post-recovery.
// ---------------------------------------------------------------------------

#[test]
fn crash_during_state_transitions_running() {
    let data_dir = unique_data_dir("scenario_b");

    let task_id = TaskId::new();
    let run_id = RunId::new();

    // Phase 1: Create task + run, drive to Running, then kill -9.
    {
        let mut authority = open_authority(&data_dir);
        let spec = make_task_spec_with_id(task_id, &[0xBA, 0xBE]);

        admit_once(&mut authority, spec, run_id);

        // Scheduled -> Ready
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq,
                    run_id,
                    RunState::Scheduled,
                    RunState::Ready,
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("Scheduled->Ready should succeed");

        // Ready -> Leased
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq,
                    run_id,
                    RunState::Ready,
                    RunState::Leased,
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("Ready->Leased should succeed");

        // Leased -> Running
        if authority.projection().get_lease(&run_id).is_none() {
            let seq = next_seq(&authority);
            let _ = authority
                .submit_command(
                    MutationCommand::LeaseAcquire(
                        actionqueue_core::mutation::LeaseAcquireCommand::new(
                            seq,
                            run_id,
                            "fixture",
                            u64::MAX,
                            0,
                        ),
                    ),
                    DurabilityPolicy::Immediate,
                )
                .unwrap();
        }
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq,
                    run_id,
                    RunState::Leased,
                    RunState::Running,
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("Leased->Running should succeed");

        // Crash while Running.
        simulate_kill9(authority);
    }

    // Phase 2: Recovery — verify run is in Running state.
    {
        let recovery =
            load_projection_from_storage(&data_dir).expect("recovery should succeed after crash");

        assert_eq!(recovery.projection.task_count(), 1);
        assert_eq!(recovery.projection.run_count(), 1);
        assert_eq!(
            recovery.projection.get_run_state(&run_id),
            Some(&RunState::Running),
            "run must recover to Running state after kill -9"
        );
    }

    // Phase 3: Post-recovery — record attempt and complete the run.
    {
        let mut authority = open_authority(&data_dir);

        let attempt_id = AttemptId::new();
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::AttemptStart(AttemptStartCommand::new(
                    seq,
                    run_id,
                    attempt_id,
                    seq,
                    lease_support::fence_for(authority.projection(), run_id),
                    authority.projection().pending_resume(run_id).map(|c| c.context_id),
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("attempt start should succeed post-recovery");

        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::AttemptFinish(AttemptFinishCommand::new(
                    seq,
                    run_id,
                    attempt_id,
                    AttemptOutcome::success(),
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("attempt finish should succeed post-recovery");

        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq,
                    run_id,
                    RunState::Running,
                    RunState::Completed,
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("Running->Completed should succeed post-recovery");

        assert_eq!(
            authority.projection().get_run_state(&run_id),
            Some(&RunState::Completed),
            "run should reach Completed after post-recovery lifecycle"
        );

        // Graceful drop this time — verify final state persists.
        drop(authority);
    }

    // Phase 4: Final verification — re-open and confirm terminal state.
    {
        let recovery =
            load_projection_from_storage(&data_dir).expect("final recovery should succeed");

        assert_eq!(
            recovery.projection.get_run_state(&run_id),
            Some(&RunState::Completed),
            "terminal Completed must be durable across recovery"
        );
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

// ---------------------------------------------------------------------------
// Scenario C: Crash with mixed terminal and active runs.
//
// Create 3 tasks, each with 1 run:
//   - Run A: driven to Completed (terminal)
//   - Run B: driven to Failed (terminal)
//   - Run C: driven to Running (active, in-flight)
// Then crash. Recovery must show A=Completed, B=Failed, C=Running.
// Post-recovery, complete Run C and verify.
// ---------------------------------------------------------------------------

#[test]
fn crash_with_mixed_terminal_and_active_runs() {
    let data_dir = unique_data_dir("scenario_c");

    let task_a = TaskId::new();
    let task_b = TaskId::new();
    let task_c = TaskId::new();
    let run_a = RunId::new();
    let run_b = RunId::new();
    let run_c = RunId::new();

    // Phase 1: Build mixed state, then kill -9.
    {
        let mut authority = open_authority(&data_dir);

        for (tid, rid, payload) in
            [(task_a, run_a, 0xAAu8), (task_b, run_b, 0xBBu8), (task_c, run_c, 0xCCu8)]
        {
            admit_once(&mut authority, make_task_spec_with_id(tid, &[payload]), rid);
        }

        // Helper: drive a run through Scheduled -> Ready -> Leased -> Running.
        let drive_to_running = |auth: &mut StorageMutationAuthority<
            InstrumentedWalWriter<WalFsWriter>,
            ReplayReducer,
        >,
                                rid: RunId| {
            for (from, to) in [
                (RunState::Scheduled, RunState::Ready),
                (RunState::Ready, RunState::Leased),
                (RunState::Leased, RunState::Running),
            ] {
                if to == RunState::Running {
                    let seq = next_seq(auth);
                    let _ = auth
                        .submit_command(
                            MutationCommand::LeaseAcquire(
                                actionqueue_core::mutation::LeaseAcquireCommand::new(
                                    seq,
                                    rid,
                                    "fixture",
                                    u64::MAX,
                                    0,
                                ),
                            ),
                            DurabilityPolicy::Immediate,
                        )
                        .unwrap();
                }
                let s = next_seq(auth);
                let _ = auth
                    .submit_command(
                        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                            s, rid, from, to, s,
                        )),
                        DurabilityPolicy::Immediate,
                    )
                    .expect("state transition should succeed");
            }
        };

        // --- Run A: drive to Completed ---
        drive_to_running(&mut authority, run_a);
        {
            let aid = AttemptId::new();
            let seq = next_seq(&authority);
            let _ = authority
                .submit_command(
                    MutationCommand::AttemptStart(AttemptStartCommand::new(
                        seq,
                        run_a,
                        aid,
                        seq,
                        lease_support::fence_for(authority.projection(), run_a),
                        authority.projection().pending_resume(run_a).map(|c| c.context_id),
                    )),
                    DurabilityPolicy::Immediate,
                )
                .expect("attempt start A");
            let seq = next_seq(&authority);
            let _ = authority
                .submit_command(
                    MutationCommand::AttemptFinish(AttemptFinishCommand::new(
                        seq,
                        run_a,
                        aid,
                        AttemptOutcome::success(),
                        seq,
                    )),
                    DurabilityPolicy::Immediate,
                )
                .expect("attempt finish A");
            let seq = next_seq(&authority);
            let _ = authority
                .submit_command(
                    MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                        seq,
                        run_a,
                        RunState::Running,
                        RunState::Completed,
                        seq,
                    )),
                    DurabilityPolicy::Immediate,
                )
                .expect("Running->Completed for A");
        }

        // --- Run B: drive to Failed ---
        drive_to_running(&mut authority, run_b);
        {
            let aid = AttemptId::new();
            let seq = next_seq(&authority);
            let _ = authority
                .submit_command(
                    MutationCommand::AttemptStart(AttemptStartCommand::new(
                        seq,
                        run_b,
                        aid,
                        seq,
                        lease_support::fence_for(authority.projection(), run_b),
                        authority.projection().pending_resume(run_b).map(|c| c.context_id),
                    )),
                    DurabilityPolicy::Immediate,
                )
                .expect("attempt start B");
            let seq = next_seq(&authority);
            let _ = authority
                .submit_command(
                    MutationCommand::AttemptFinish(AttemptFinishCommand::new(
                        seq,
                        run_b,
                        aid,
                        AttemptOutcome::failure("simulated failure"),
                        seq,
                    )),
                    DurabilityPolicy::Immediate,
                )
                .expect("attempt finish B");
            let seq = next_seq(&authority);
            let _ = authority
                .submit_command(
                    MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                        seq,
                        run_b,
                        RunState::Running,
                        RunState::Failed,
                        seq,
                    )),
                    DurabilityPolicy::Immediate,
                )
                .expect("Running->Failed for B");
        }

        // --- Run C: drive to Running only (active in-flight) ---
        drive_to_running(&mut authority, run_c);

        // Kill -9 with mixed states.
        simulate_kill9(authority);
    }

    // Phase 2: Recovery — verify all 3 runs in expected states.
    {
        let recovery =
            load_projection_from_storage(&data_dir).expect("recovery should succeed after crash");

        assert_eq!(recovery.projection.task_count(), 3);
        assert_eq!(recovery.projection.run_count(), 3);

        assert_eq!(
            recovery.projection.get_run_state(&run_a),
            Some(&RunState::Completed),
            "Run A must recover as Completed"
        );
        assert_eq!(
            recovery.projection.get_run_state(&run_b),
            Some(&RunState::Failed),
            "Run B must recover as Failed"
        );
        assert_eq!(
            recovery.projection.get_run_state(&run_c),
            Some(&RunState::Running),
            "Run C must recover as Running (active at crash)"
        );
    }

    // Phase 3: Post-recovery — complete Run C.
    {
        let mut authority = open_authority(&data_dir);

        let attempt_id = AttemptId::new();
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::AttemptStart(AttemptStartCommand::new(
                    seq,
                    run_c,
                    attempt_id,
                    seq,
                    lease_support::fence_for(authority.projection(), run_c),
                    authority.projection().pending_resume(run_c).map(|c| c.context_id),
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("attempt start C post-recovery");
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::AttemptFinish(AttemptFinishCommand::new(
                    seq,
                    run_c,
                    attempt_id,
                    AttemptOutcome::success(),
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("attempt finish C post-recovery");
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq,
                    run_c,
                    RunState::Running,
                    RunState::Completed,
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("Running->Completed for C post-recovery");

        // Terminal states are immutable.
        assert_eq!(authority.projection().get_run_state(&run_a), Some(&RunState::Completed));
        assert_eq!(authority.projection().get_run_state(&run_b), Some(&RunState::Failed));
        assert_eq!(authority.projection().get_run_state(&run_c), Some(&RunState::Completed));
    }

    // Phase 4: Final verification after graceful close.
    {
        let recovery =
            load_projection_from_storage(&data_dir).expect("final recovery should succeed");
        assert_eq!(recovery.projection.get_run_state(&run_a), Some(&RunState::Completed));
        assert_eq!(recovery.projection.get_run_state(&run_b), Some(&RunState::Failed));
        assert_eq!(recovery.projection.get_run_state(&run_c), Some(&RunState::Completed));
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

// ---------------------------------------------------------------------------
// Scenario D: Multiple sequential crashes with incremental progress.
//
// Crash 1: after task+run creation (Scheduled).
// Recover, advance to Ready, crash 2.
// Recover, advance to Running, crash 3.
// Recover, complete the run, verify.
// ---------------------------------------------------------------------------

#[test]
fn sequential_crashes_with_incremental_progress() {
    let data_dir = unique_data_dir("scenario_d");

    let task_id = TaskId::new();
    let run_id = RunId::new();

    // --- Crash 1: after task + run creation ---
    {
        let mut authority = open_authority(&data_dir);

        let spec = make_task_spec_with_id(task_id, &[0xDD]);
        admit_once(&mut authority, spec, run_id);

        simulate_kill9(authority);
    }

    // Verify recovery: Scheduled.
    {
        let recovery = load_projection_from_storage(&data_dir).expect("recovery 1");
        assert_eq!(recovery.projection.task_count(), 1);
        assert_eq!(recovery.projection.run_count(), 1);
        assert_eq!(
            recovery.projection.get_run_state(&run_id),
            Some(&RunState::Scheduled),
            "run must be Scheduled after crash 1"
        );
    }

    // --- Crash 2: after advancing to Ready ---
    {
        let mut authority = open_authority(&data_dir);

        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq,
                    run_id,
                    RunState::Scheduled,
                    RunState::Ready,
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("Scheduled->Ready");

        simulate_kill9(authority);
    }

    // Verify recovery: Ready.
    {
        let recovery = load_projection_from_storage(&data_dir).expect("recovery 2");
        assert_eq!(
            recovery.projection.get_run_state(&run_id),
            Some(&RunState::Ready),
            "run must be Ready after crash 2"
        );
    }

    // --- Crash 3: after advancing to Running ---
    {
        let mut authority = open_authority(&data_dir);

        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq,
                    run_id,
                    RunState::Ready,
                    RunState::Leased,
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("Ready->Leased");

        if authority.projection().get_lease(&run_id).is_none() {
            let seq = next_seq(&authority);
            let _ = authority
                .submit_command(
                    MutationCommand::LeaseAcquire(
                        actionqueue_core::mutation::LeaseAcquireCommand::new(
                            seq,
                            run_id,
                            "fixture",
                            u64::MAX,
                            0,
                        ),
                    ),
                    DurabilityPolicy::Immediate,
                )
                .unwrap();
        }
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq,
                    run_id,
                    RunState::Leased,
                    RunState::Running,
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("Leased->Running");

        simulate_kill9(authority);
    }

    // Verify recovery: Running.
    {
        let recovery = load_projection_from_storage(&data_dir).expect("recovery 3");
        assert_eq!(
            recovery.projection.get_run_state(&run_id),
            Some(&RunState::Running),
            "run must be Running after crash 3"
        );
    }

    // --- Final: complete the run normally ---
    {
        let mut authority = open_authority(&data_dir);

        let attempt_id = AttemptId::new();
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::AttemptStart(AttemptStartCommand::new(
                    seq,
                    run_id,
                    attempt_id,
                    seq,
                    lease_support::fence_for(authority.projection(), run_id),
                    authority.projection().pending_resume(run_id).map(|c| c.context_id),
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("attempt start");
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::AttemptFinish(AttemptFinishCommand::new(
                    seq,
                    run_id,
                    attempt_id,
                    AttemptOutcome::success(),
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("attempt finish");
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq,
                    run_id,
                    RunState::Running,
                    RunState::Completed,
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("Running->Completed");

        drop(authority);
    }

    // Final verification.
    {
        let recovery = load_projection_from_storage(&data_dir).expect("final recovery");
        assert_eq!(
            recovery.projection.get_run_state(&run_id),
            Some(&RunState::Completed),
            "run must reach Completed after 3 crashes and final completion"
        );
        assert_eq!(recovery.projection.task_count(), 1);
        assert_eq!(recovery.projection.run_count(), 1);
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

// ---------------------------------------------------------------------------
// Scenario E: WAL sequence monotonicity across crash boundaries.
//
// Verify that WAL sequence numbers increase monotonically across crashes
// and that new events after recovery get the correct next sequence.
// ---------------------------------------------------------------------------

#[test]
fn wal_sequence_monotonicity_across_crashes() {
    let data_dir = unique_data_dir("scenario_e");

    // Phase 1: Create 2 tasks (seq 1, 2), crash.
    let task_id_1;
    let task_id_2;
    {
        let mut authority = open_authority(&data_dir);

        let spec1 = make_task_spec(&[0xE1]);
        task_id_1 = spec1.id();
        let seq = next_seq(&authority);
        assert_eq!(seq, 2, "StoreInitialized precedes the first mutation");
        admit_once(&mut authority, spec1, RunId::new());

        let spec2 = make_task_spec(&[0xE2]);
        task_id_2 = spec2.id();
        let seq = next_seq(&authority);
        assert_eq!(seq, 3, "second mutation follows initialization");
        admit_once(&mut authority, spec2, RunId::new());

        simulate_kill9(authority);
    }

    // Phase 2: Recovery — latest_sequence should be 2.
    {
        let recovery = load_projection_from_storage(&data_dir).expect("recovery 1");
        assert_eq!(
            recovery.projection.latest_sequence(),
            3,
            "initialization plus two mutations survive crash"
        );
    }

    // Phase 3: Add a 3rd task (seq 3), crash again.
    {
        let mut authority = open_authority(&data_dir);

        let seq = next_seq(&authority);
        assert_eq!(seq, 4, "post-crash mutation is contiguous");
        let spec3 = make_task_spec(&[0xE3]);
        admit_once(&mut authority, spec3, RunId::new());

        simulate_kill9(authority);
    }

    // Phase 4: Recovery — latest_sequence should be 3, all 3 tasks present.
    {
        let recovery = load_projection_from_storage(&data_dir).expect("recovery 2");
        assert_eq!(
            recovery.projection.latest_sequence(),
            4,
            "initialization plus three mutations survive crashes"
        );
        assert_eq!(recovery.projection.task_count(), 3);
        assert!(recovery.projection.get_task(&task_id_1).is_some());
        assert!(recovery.projection.get_task(&task_id_2).is_some());
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

// ---------------------------------------------------------------------------
// Scenario F: Crash during RetryWait does not lose retry state.
//
// Create task+run, drive to Running, fail the attempt, transition to RetryWait,
// crash. Recovery must show RetryWait. Post-recovery, drive retry to completion.
// ---------------------------------------------------------------------------

#[test]
fn crash_during_retry_wait_preserves_state() {
    let data_dir = unique_data_dir("scenario_f");

    let task_id = TaskId::new();
    let run_id = RunId::new();
    let attempt_1 = AttemptId::new();

    // Phase 1: Drive to RetryWait, crash.
    {
        let mut authority = open_authority(&data_dir);

        let spec = make_task_spec_with_id(task_id, &[0xFF]);
        admit_once(&mut authority, spec, run_id);

        // Scheduled -> Ready -> Leased -> Running
        for (from, to) in [
            (RunState::Scheduled, RunState::Ready),
            (RunState::Ready, RunState::Leased),
            (RunState::Leased, RunState::Running),
        ] {
            if to == RunState::Running && authority.projection().get_lease(&run_id).is_none() {
                let seq = next_seq(&authority);
                let _ = authority
                    .submit_command(
                        MutationCommand::LeaseAcquire(
                            actionqueue_core::mutation::LeaseAcquireCommand::new(
                                seq,
                                run_id,
                                "fixture",
                                u64::MAX,
                                0,
                            ),
                        ),
                        DurabilityPolicy::Immediate,
                    )
                    .unwrap();
            }
            let seq = next_seq(&authority);
            let _ = authority
                .submit_command(
                    MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                        seq, run_id, from, to, seq,
                    )),
                    DurabilityPolicy::Immediate,
                )
                .expect("state transition");
        }

        // Record attempt 1 with failure.
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::AttemptStart(AttemptStartCommand::new(
                    seq,
                    run_id,
                    attempt_1,
                    seq,
                    lease_support::fence_for(authority.projection(), run_id),
                    authority.projection().pending_resume(run_id).map(|c| c.context_id),
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("attempt start");
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::AttemptFinish(AttemptFinishCommand::new(
                    seq,
                    run_id,
                    attempt_1,
                    AttemptOutcome::failure("transient error"),
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("attempt finish");

        // Running -> RetryWait
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq,
                    run_id,
                    RunState::Running,
                    RunState::RetryWait,
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("Running->RetryWait");

        simulate_kill9(authority);
    }

    // Phase 2: Recovery — must be in RetryWait.
    {
        let recovery = load_projection_from_storage(&data_dir).expect("recovery");
        assert_eq!(
            recovery.projection.get_run_state(&run_id),
            Some(&RunState::RetryWait),
            "run must recover to RetryWait after crash"
        );
    }

    // Phase 3: Drive retry to completion.
    {
        let mut authority = open_authority(&data_dir);

        // RetryWait -> Ready -> Leased -> Running
        for (from, to) in [
            (RunState::RetryWait, RunState::Ready),
            (RunState::Ready, RunState::Leased),
            (RunState::Leased, RunState::Running),
        ] {
            if to == RunState::Running && authority.projection().get_lease(&run_id).is_none() {
                let seq = next_seq(&authority);
                let _ = authority
                    .submit_command(
                        MutationCommand::LeaseAcquire(
                            actionqueue_core::mutation::LeaseAcquireCommand::new(
                                seq,
                                run_id,
                                "fixture",
                                u64::MAX,
                                0,
                            ),
                        ),
                        DurabilityPolicy::Immediate,
                    )
                    .unwrap();
            }
            let seq = next_seq(&authority);
            let _ = authority
                .submit_command(
                    MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                        seq, run_id, from, to, seq,
                    )),
                    DurabilityPolicy::Immediate,
                )
                .expect("retry transition");
        }

        let attempt_2 = AttemptId::new();
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::AttemptStart(AttemptStartCommand::new(
                    seq,
                    run_id,
                    attempt_2,
                    seq,
                    lease_support::fence_for(authority.projection(), run_id),
                    authority.projection().pending_resume(run_id).map(|c| c.context_id),
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("attempt 2 start");
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::AttemptFinish(AttemptFinishCommand::new(
                    seq,
                    run_id,
                    attempt_2,
                    AttemptOutcome::success(),
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("attempt 2 finish");
        let seq = next_seq(&authority);
        let _ = authority
            .submit_command(
                MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                    seq,
                    run_id,
                    RunState::Running,
                    RunState::Completed,
                    seq,
                )),
                DurabilityPolicy::Immediate,
            )
            .expect("Running->Completed");
    }

    // Phase 4: Final verification.
    {
        let recovery = load_projection_from_storage(&data_dir).expect("final recovery");
        assert_eq!(
            recovery.projection.get_run_state(&run_id),
            Some(&RunState::Completed),
            "run must be Completed after retry across crash boundary"
        );
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

// ---------------------------------------------------------------------------
// Scenario G: High-volume crash — many tasks survive kill -9.
//
// Create 50 tasks rapidly, crash, verify all 50 recover.
// ---------------------------------------------------------------------------

#[test]
fn high_volume_tasks_survive_crash() {
    let data_dir = unique_data_dir("scenario_g");

    let mut task_ids = Vec::new();
    {
        let mut authority = open_authority(&data_dir);

        for i in 0u16..50 {
            let spec = make_task_spec(&i.to_le_bytes());
            task_ids.push(spec.id());
            admit_once(&mut authority, spec, RunId::new());
        }

        simulate_kill9(authority);
    }

    // Recovery.
    {
        let recovery = load_projection_from_storage(&data_dir).expect("bulk recovery");
        assert_eq!(recovery.projection.task_count(), 50, "all 50 tasks must survive kill -9");
        assert_eq!(recovery.projection.latest_sequence(), 51);

        for task_id in &task_ids {
            assert!(
                recovery.projection.get_task(task_id).is_some(),
                "task {task_id} must be present after bulk crash recovery"
            );
        }
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}
