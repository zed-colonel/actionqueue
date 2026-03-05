//! TS-3 Multi-mutation session test.
//!
//! Verifies that bootstrapping once and performing 5+ sequential mutations
//! through a single authority session produces correct projection state after
//! each mutation. This exercises the WAL append → projection apply pipeline
//! across diverse command types within a single session.

mod support;

use actionqueue_core::ids::TaskId;
use actionqueue_core::mutation::{
    DurabilityPolicy, EnginePauseCommand, EngineResumeCommand, MutationAuthority, MutationCommand,
    RunStateTransitionCommand, TaskCancelCommand,
};
use actionqueue_core::run::state::RunState;
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};

/// Helper to compute the next sequence from the authority's projection.
fn next_seq<W: actionqueue_storage::wal::writer::WalWriter>(
    authority: &actionqueue_storage::mutation::authority::StorageMutationAuthority<
        W,
        actionqueue_storage::recovery::reducer::ReplayReducer,
    >,
) -> u64 {
    authority
        .projection()
        .latest_sequence()
        .checked_add(1)
        .expect("sequence should not overflow in test")
}

/// Proves that 7 diverse mutations through a single authority session
/// produce consistent projection state after each operation.
#[test]
fn multi_mutation_session_produces_consistent_projection_after_each_step() {
    let data_dir = support::unique_data_dir("ts-3-multi-mutation");

    // Bootstrap once.
    let recovery =
        actionqueue_storage::recovery::bootstrap::load_projection_from_storage(&data_dir)
            .expect("initial bootstrap should succeed");
    let mut authority = actionqueue_storage::mutation::authority::StorageMutationAuthority::new(
        recovery.wal_writer,
        recovery.projection,
    );

    // Mutation 1: Create task A.
    let task_a_id = TaskId::new();
    let spec_a = TaskSpec::new(
        task_a_id,
        TaskPayload::new(b"task-a-payload".to_vec()),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("valid spec");
    let seq1 = next_seq(&authority);
    let cmd1 = MutationCommand::TaskCreate(actionqueue_core::mutation::TaskCreateCommand::new(
        seq1, spec_a, seq1,
    ));
    let _ = authority
        .submit_command(cmd1, DurabilityPolicy::Immediate)
        .expect("task create should succeed");
    assert_eq!(authority.projection().task_count(), 1);
    assert!(authority.projection().get_task(&task_a_id).is_some());

    // Mutation 2: Create task B.
    let task_b_id = TaskId::new();
    let spec_b = TaskSpec::new(
        task_b_id,
        TaskPayload::new(b"task-b-payload".to_vec()),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("valid spec");
    let seq2 = next_seq(&authority);
    let cmd2 = MutationCommand::TaskCreate(actionqueue_core::mutation::TaskCreateCommand::new(
        seq2, spec_b, seq2,
    ));
    let _ = authority
        .submit_command(cmd2, DurabilityPolicy::Immediate)
        .expect("task create should succeed");
    assert_eq!(authority.projection().task_count(), 2);

    // Mutation 3: Create a run for task A.
    let run_a =
        actionqueue_core::run::run_instance::RunInstance::new_scheduled(task_a_id, seq1, seq1)
            .expect("run instance should be valid");
    let run_a_id = run_a.id();
    let seq3 = next_seq(&authority);
    let cmd3 =
        MutationCommand::RunCreate(actionqueue_core::mutation::RunCreateCommand::new(seq3, run_a));
    let _ = authority
        .submit_command(cmd3, DurabilityPolicy::Immediate)
        .expect("run create should succeed");
    assert_eq!(authority.projection().run_count(), 1);
    assert_eq!(authority.projection().get_run_state(&run_a_id).copied(), Some(RunState::Scheduled));

    // Mutation 4: Promote run A from Scheduled to Ready.
    let seq4 = next_seq(&authority);
    let cmd4 = MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
        seq4,
        run_a_id,
        RunState::Scheduled,
        RunState::Ready,
        seq4,
    ));
    let _ = authority
        .submit_command(cmd4, DurabilityPolicy::Immediate)
        .expect("promote should succeed");
    assert_eq!(authority.projection().get_run_state(&run_a_id).copied(), Some(RunState::Ready));

    // Mutation 5: Pause engine.
    let seq5 = next_seq(&authority);
    let cmd5 = MutationCommand::EnginePause(EnginePauseCommand::new(seq5, seq5));
    let _ =
        authority.submit_command(cmd5, DurabilityPolicy::Immediate).expect("pause should succeed");
    assert!(authority.projection().is_engine_paused());

    // Mutation 6: Resume engine.
    let seq6 = next_seq(&authority);
    let cmd6 = MutationCommand::EngineResume(EngineResumeCommand::new(seq6, seq6));
    let _ =
        authority.submit_command(cmd6, DurabilityPolicy::Immediate).expect("resume should succeed");
    assert!(!authority.projection().is_engine_paused());

    // Mutation 7: Cancel task B.
    let seq7 = next_seq(&authority);
    let cmd7 = MutationCommand::TaskCancel(TaskCancelCommand::new(seq7, task_b_id, seq7));
    let _ =
        authority.submit_command(cmd7, DurabilityPolicy::Immediate).expect("cancel should succeed");
    assert!(authority.projection().is_task_canceled(task_b_id));
    assert!(!authority.projection().is_task_canceled(task_a_id));

    // Final consistency: sequence advanced, both tasks exist, run is Ready.
    assert_eq!(authority.projection().latest_sequence(), seq7);
    assert_eq!(authority.projection().task_count(), 2);
    assert_eq!(authority.projection().run_count(), 1);
    assert_eq!(authority.projection().get_run_state(&run_a_id).copied(), Some(RunState::Ready));

    // Re-bootstrap from storage and verify WAL replay matches.
    drop(authority);
    let recovery2 =
        actionqueue_storage::recovery::bootstrap::load_projection_from_storage(&data_dir)
            .expect("re-bootstrap should succeed");
    let replayed = recovery2.projection;

    assert_eq!(replayed.task_count(), 2);
    assert_eq!(replayed.run_count(), 1);
    assert_eq!(replayed.get_run_state(&run_a_id).copied(), Some(RunState::Ready));
    assert!(replayed.is_task_canceled(task_b_id));
    assert!(!replayed.is_task_canceled(task_a_id));
    assert!(!replayed.is_engine_paused());
    assert_eq!(replayed.latest_sequence(), seq7);

    let _ = std::fs::remove_dir_all(&data_dir);
}
