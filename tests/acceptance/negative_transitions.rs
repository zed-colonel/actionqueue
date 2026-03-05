//! P6-002 Negative state transition tests + P6-005 concurrent mutation conflict test.
//!
//! Verifies that invalid state transitions are rejected at the authority level:
//! - Completed -> Ready: rejected
//! - Failed -> Running: rejected
//! - Canceled -> Ready: rejected
//! - Scheduled -> Running: rejected (must go through Ready first)
//!
//! Also verifies that duplicate AttemptFinished for the same (run, attempt) pair is rejected.

mod support;

use std::str::FromStr;

use actionqueue_core::ids::{AttemptId, RunId, TaskId};
use actionqueue_core::mutation::{
    AttemptFinishCommand, AttemptOutcome, AttemptStartCommand, DurabilityPolicy, MutationAuthority,
    MutationCommand, RunStateTransitionCommand,
};
use actionqueue_core::run::state::RunState;
use actionqueue_storage::mutation::authority::{MutationAuthorityError, MutationValidationError};
use actionqueue_storage::recovery::reducer::ReplayReducerError;

/// Concrete error type for authority operations in these tests.
type AuthorityError = MutationAuthorityError<ReplayReducerError>;

/// Attempts a state transition via authority and returns the raw result.
fn try_transition(
    data_dir: &std::path::Path,
    run_id: RunId,
    from: RunState,
    to: RunState,
) -> Result<actionqueue_core::mutation::MutationOutcome, AuthorityError> {
    let recovery = actionqueue_storage::recovery::bootstrap::load_projection_from_storage(data_dir)
        .expect("storage bootstrap should succeed");
    let mut authority = actionqueue_storage::mutation::authority::StorageMutationAuthority::new(
        recovery.wal_writer,
        recovery.projection,
    );

    let sequence = authority
        .projection()
        .latest_sequence()
        .checked_add(1)
        .expect("sequence should not overflow");
    authority.submit_command(
        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
            sequence, run_id, from, to, sequence,
        )),
        DurabilityPolicy::Immediate,
    )
}

// ---- P6-002: Negative state transition tests ----

/// Completed -> Ready is rejected.
#[test]
fn completed_to_ready_is_rejected() {
    let data_dir = support::unique_data_dir("p6-002-completed-to-ready");
    let task_id_str = "d6d60002-0001-0001-0001-000000000001";
    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // Submit, complete the run through the full lifecycle.
    support::submit_once_task_via_cli(task_id_str, &data_dir);
    let completion = support::complete_once_run_via_authority(&data_dir, task_id);

    // Verify run is Completed.
    support::assert_run_state_from_storage(&data_dir, completion.run_id, RunState::Completed);

    // Attempt Completed -> Ready: must be rejected as InvalidTransition.
    let result = try_transition(&data_dir, completion.run_id, RunState::Completed, RunState::Ready);
    assert!(
        matches!(
            &result,
            Err(MutationAuthorityError::Validation(
                MutationValidationError::InvalidTransition { .. }
            ))
        ),
        "Completed -> Ready must be rejected as InvalidTransition, got: {result:?}"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Failed -> Running is rejected.
#[test]
fn failed_to_running_is_rejected() {
    let data_dir = support::unique_data_dir("p6-002-failed-to-running");
    let task_id_str = "d6d60002-0002-0002-0002-000000000002";
    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // Submit with max_attempts=1, then fail the run.
    support::submit_once_task_with_max_attempts_via_cli(task_id_str, &data_dir, 1);
    let evidence = support::execute_attempt_outcome_sequence_via_authority(
        &data_dir,
        task_id,
        1,
        &[support::AttemptOutcomePlan::TerminalFailure],
    );
    assert_eq!(evidence.final_state, RunState::Failed);

    // Attempt Failed -> Running: must be rejected as InvalidTransition.
    let result = try_transition(&data_dir, evidence.run_id, RunState::Failed, RunState::Running);
    assert!(
        matches!(
            &result,
            Err(MutationAuthorityError::Validation(
                MutationValidationError::InvalidTransition { .. }
            ))
        ),
        "Failed -> Running must be rejected as InvalidTransition, got: {result:?}"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Canceled -> Ready is rejected.
#[test]
fn canceled_to_ready_is_rejected() {
    let data_dir = support::unique_data_dir("p6-002-canceled-to-ready");
    let task_id_str = "d6d60002-0003-0003-0003-000000000003";
    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // Submit and cancel from Ready.
    support::submit_once_task_via_cli(task_id_str, &data_dir);
    let run_id = support::promote_task_run_to_ready_via_authority(&data_dir, task_id);
    support::transition_run_state_via_authority(
        &data_dir,
        run_id,
        RunState::Ready,
        RunState::Canceled,
    );

    // Verify run is Canceled.
    support::assert_run_state_from_storage(&data_dir, run_id, RunState::Canceled);

    // Attempt Canceled -> Ready: must be rejected as InvalidTransition.
    let result = try_transition(&data_dir, run_id, RunState::Canceled, RunState::Ready);
    assert!(
        matches!(
            &result,
            Err(MutationAuthorityError::Validation(
                MutationValidationError::InvalidTransition { .. }
            ))
        ),
        "Canceled -> Ready must be rejected as InvalidTransition, got: {result:?}"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Scheduled -> Running is rejected (must go through Ready first).
#[test]
fn scheduled_to_running_is_rejected() {
    let data_dir = support::unique_data_dir("p6-002-scheduled-to-running");
    let task_id_str = "d6d60002-0004-0004-0004-000000000004";
    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // Submit a task (run starts in Scheduled).
    support::submit_once_task_via_cli(task_id_str, &data_dir);
    let run_id = support::single_run_id_for_task(&data_dir, task_id);

    // Verify run is in Scheduled state.
    support::assert_run_state_from_storage(&data_dir, run_id, RunState::Scheduled);

    // Attempt Scheduled -> Running: must be rejected as InvalidTransition.
    let result = try_transition(&data_dir, run_id, RunState::Scheduled, RunState::Running);
    assert!(
        matches!(
            &result,
            Err(MutationAuthorityError::Validation(
                MutationValidationError::InvalidTransition { .. }
            ))
        ),
        "Scheduled -> Running must be rejected as InvalidTransition, got: {result:?}"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

// ---- P6-005: Concurrent mutation conflict test ----

/// Duplicate AttemptFinished for the same (run_id, attempt_id) is rejected.
///
/// Steps:
/// 1. Create authority with a task and a run.
/// 2. Transition run through Ready -> Leased -> Running.
/// 3. Record AttemptStart.
/// 4. Record AttemptFinished (success).
/// 5. Try to record a second AttemptFinished for the same (run_id, attempt_id).
/// 6. Verify it is rejected.
#[test]
fn duplicate_attempt_finished_is_rejected() {
    let data_dir = support::unique_data_dir("p6-005-dup-attempt-finish");
    let task_id_str = "e6e60005-0001-0001-0001-000000000001";
    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // Submit a Once task.
    support::submit_once_task_via_cli(task_id_str, &data_dir);

    // Promote Scheduled -> Ready.
    let run_id = support::promote_task_run_to_ready_via_authority(&data_dir, task_id);

    // We need a single authority session to keep the projection consistent for this test.
    let recovery =
        actionqueue_storage::recovery::bootstrap::load_projection_from_storage(&data_dir)
            .expect("storage bootstrap should succeed");
    let mut authority = actionqueue_storage::mutation::authority::StorageMutationAuthority::new(
        recovery.wal_writer,
        recovery.projection,
    );

    // Ready -> Leased.
    let leased_seq = next_seq(&authority);
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                leased_seq,
                run_id,
                RunState::Ready,
                RunState::Leased,
                leased_seq,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("Ready -> Leased should succeed");

    // Leased -> Running.
    let running_seq = next_seq(&authority);
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                running_seq,
                run_id,
                RunState::Leased,
                RunState::Running,
                running_seq,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("Leased -> Running should succeed");

    // AttemptStart.
    let attempt_id = AttemptId::from_str("00000000-0000-0000-0000-000000000001")
        .expect("fixed attempt id should parse");
    let start_seq = next_seq(&authority);
    let _ = authority
        .submit_command(
            MutationCommand::AttemptStart(AttemptStartCommand::new(
                start_seq, run_id, attempt_id, start_seq,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("AttemptStart should succeed");

    // First AttemptFinish (success) -- should succeed.
    let finish_seq = next_seq(&authority);
    let _ = authority
        .submit_command(
            MutationCommand::AttemptFinish(AttemptFinishCommand::new(
                finish_seq,
                run_id,
                attempt_id,
                AttemptOutcome::success(),
                finish_seq,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("first AttemptFinish should succeed");

    // After the first AttemptFinish, the active attempt should be cleared.
    // The run is still in Running state (the transition to Completed/Failed
    // is a separate RunStateTransition command).

    // Second AttemptFinish for the same (run_id, attempt_id) -- must be rejected.
    //
    // This should fail because after AttemptFinish the active attempt is cleared,
    // so there is no matching active attempt for the second finish command.
    let dup_finish_seq = next_seq(&authority);
    let dup_result = authority.submit_command(
        MutationCommand::AttemptFinish(AttemptFinishCommand::new(
            dup_finish_seq,
            run_id,
            attempt_id,
            AttemptOutcome::success(),
            dup_finish_seq,
        )),
        DurabilityPolicy::Immediate,
    );

    assert!(
        matches!(
            &dup_result,
            Err(MutationAuthorityError::Validation(
                MutationValidationError::AttemptFinishMissingActive { .. }
            ))
        ),
        "second AttemptFinish must be rejected as AttemptFinishMissingActive, got: {dup_result:?}"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Helper to compute the next sequence from the authority's projection.
fn next_seq<W, P>(
    authority: &actionqueue_storage::mutation::authority::StorageMutationAuthority<W, P>,
) -> u64
where
    W: actionqueue_storage::wal::writer::WalWriter,
    P: actionqueue_storage::mutation::authority::MutationProjection,
{
    authority.projection().latest_sequence().checked_add(1).expect("sequence should not overflow")
}
