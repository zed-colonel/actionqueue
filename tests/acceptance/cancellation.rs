//! P6-001 Cancellation lifecycle acceptance test.
//!
//! Verifies:
//! 1. A task can be submitted and its run canceled from Ready state.
//! 2. A canceled run is in terminal state and cannot be retried.
//! 3. Concurrency key is released when a run is canceled.

mod support;

use std::str::FromStr;

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;
use actionqueue_engine::concurrency::key_gate::{ConcurrencyKey, KeyGate};

/// Proves that a run can be canceled from Ready state and reaches terminal Canceled.
#[test]
fn cancel_from_ready_reaches_terminal_canceled_state() {
    let data_dir = support::unique_data_dir("p6-001-cancel-from-ready");
    let task_id_str = "c6c60001-0001-0001-0001-000000000001";
    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // Submit a Once task.
    let submit = support::submit_once_task_via_cli(task_id_str, &data_dir);
    assert_eq!(submit["runs_created"], 1);

    // Promote Scheduled -> Ready.
    let run_id = support::promote_task_run_to_ready_via_authority(&data_dir, task_id);

    // Verify run is in Ready state.
    support::assert_run_state_from_storage(&data_dir, run_id, RunState::Ready);

    // Cancel from Ready -> Canceled.
    support::transition_run_state_via_authority(
        &data_dir,
        run_id,
        RunState::Ready,
        RunState::Canceled,
    );

    // Verify run is now in Canceled (terminal) state.
    support::assert_run_state_from_storage(&data_dir, run_id, RunState::Canceled);
    assert!(RunState::Canceled.is_terminal(), "Canceled should be a terminal state");

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Proves that a canceled run cannot be re-promoted or transitioned further.
#[test]
fn canceled_run_cannot_be_retried_or_transitioned() {
    let data_dir = support::unique_data_dir("p6-001-canceled-no-retry");
    let task_id_str = "c6c60001-0002-0002-0002-000000000002";
    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // Submit and promote to Ready, then cancel.
    support::submit_once_task_via_cli(task_id_str, &data_dir);
    let run_id = support::promote_task_run_to_ready_via_authority(&data_dir, task_id);
    support::transition_run_state_via_authority(
        &data_dir,
        run_id,
        RunState::Ready,
        RunState::Canceled,
    );

    // Attempt Canceled -> Ready: must be rejected with InvalidTransition.
    let result_ready = try_transition(&data_dir, run_id, RunState::Canceled, RunState::Ready);
    assert_is_invalid_transition(&result_ready, "Canceled -> Ready must be rejected");

    // Attempt Canceled -> Leased: must be rejected with InvalidTransition.
    let result_leased = try_transition(&data_dir, run_id, RunState::Canceled, RunState::Leased);
    assert_is_invalid_transition(&result_leased, "Canceled -> Leased must be rejected");

    // Attempt Canceled -> Running: must be rejected with InvalidTransition.
    let result_running = try_transition(&data_dir, run_id, RunState::Canceled, RunState::Running);
    assert_is_invalid_transition(&result_running, "Canceled -> Running must be rejected");

    // Attempt Canceled -> Scheduled: must be rejected with InvalidTransition.
    let result_scheduled =
        try_transition(&data_dir, run_id, RunState::Canceled, RunState::Scheduled);
    assert_is_invalid_transition(&result_scheduled, "Canceled -> Scheduled must be rejected");

    // Verify run is still in Canceled state.
    support::assert_run_state_from_storage(&data_dir, run_id, RunState::Canceled);

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Proves that a concurrency key is released when a run transitions from Running to Canceled.
#[test]
fn concurrency_key_released_when_run_canceled_from_running() {
    let data_dir = support::unique_data_dir("p6-001-ck-release-on-cancel");
    let task_id_str = "c6c60001-0003-0003-0003-000000000003";
    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // Submit a Once task with a concurrency key.
    support::submit_once_task_with_concurrency_key_via_cli(
        task_id_str,
        &data_dir,
        Some("cancel-test-key"),
    );

    // Promote Scheduled -> Ready.
    let run_id = support::promote_task_run_to_ready_via_authority(&data_dir, task_id);

    // Transition Ready -> Leased.
    support::transition_run_state_via_authority(
        &data_dir,
        run_id,
        RunState::Ready,
        RunState::Leased,
    );

    // Acquire concurrency key and transition Leased -> Running via key gate.
    let mut key_gate = KeyGate::new();
    let enter_result = support::evaluate_and_apply_running_transition_with_key_gate(
        &data_dir,
        run_id,
        Some("cancel-test-key"),
        &mut key_gate,
    );
    assert_eq!(enter_result.gate_outcome, "Acquired");
    assert!(enter_result.transition_applied);

    // Verify the concurrency key is occupied.
    let ck = ConcurrencyKey::new("cancel-test-key");
    assert!(key_gate.is_key_occupied(&ck));
    assert_eq!(key_gate.key_holder(&ck), Some(run_id));

    // Transition Running -> Canceled via key gate lifecycle.
    let cancel_result = support::evaluate_and_apply_running_exit_with_key_gate(
        &data_dir,
        run_id,
        Some("cancel-test-key"),
        RunState::Canceled,
        &mut key_gate,
    );
    assert_eq!(cancel_result.from, RunState::Running);
    assert_eq!(cancel_result.to, RunState::Canceled);
    assert_eq!(cancel_result.gate_outcome, "Released");
    assert!(cancel_result.transition_applied);

    // Verify the concurrency key is now free.
    assert!(!key_gate.is_key_occupied(&ck));
    assert_eq!(key_gate.key_holder(&ck), None);

    // Verify run is in Canceled state durably.
    support::assert_run_state_from_storage(&data_dir, run_id, RunState::Canceled);

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Proves cancellation from additional non-terminal states (Scheduled, Leased) works.
#[test]
fn cancel_from_scheduled_state() {
    let data_dir = support::unique_data_dir("p6-001-cancel-from-scheduled");
    let task_id_str = "c6c60001-0004-0004-0004-000000000004";
    let task_id = TaskId::from_str(task_id_str).expect("fixed task id should parse");

    // Submit a Once task (run starts in Scheduled).
    support::submit_once_task_via_cli(task_id_str, &data_dir);
    let run_id = support::single_run_id_for_task(&data_dir, task_id);

    // Verify run is in Scheduled state.
    support::assert_run_state_from_storage(&data_dir, run_id, RunState::Scheduled);

    // Cancel from Scheduled -> Canceled.
    support::transition_run_state_via_authority(
        &data_dir,
        run_id,
        RunState::Scheduled,
        RunState::Canceled,
    );

    // Verify run is now in Canceled.
    support::assert_run_state_from_storage(&data_dir, run_id, RunState::Canceled);

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Helper: attempts a state transition and returns the result for error testing.
fn try_transition(
    data_dir: &std::path::Path,
    run_id: actionqueue_core::ids::RunId,
    from: RunState,
    to: RunState,
) -> Result<
    actionqueue_core::mutation::MutationOutcome,
    actionqueue_storage::mutation::authority::MutationAuthorityError<
        actionqueue_storage::recovery::reducer::ReplayReducerError,
    >,
> {
    use actionqueue_core::mutation::{
        DurabilityPolicy, MutationAuthority, MutationCommand, RunStateTransitionCommand,
    };

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

/// Asserts that a transition result is an `InvalidTransition` validation error,
/// not a sequence violation or other unrelated error.
fn assert_is_invalid_transition(
    result: &Result<
        actionqueue_core::mutation::MutationOutcome,
        actionqueue_storage::mutation::authority::MutationAuthorityError<
            actionqueue_storage::recovery::reducer::ReplayReducerError,
        >,
    >,
    context: &str,
) {
    use actionqueue_storage::mutation::authority::{
        MutationAuthorityError, MutationValidationError,
    };

    let err =
        result.as_ref().expect_err(&format!("{context}: transition should have been rejected"));
    assert!(
        matches!(
            err,
            MutationAuthorityError::Validation(MutationValidationError::InvalidTransition { .. })
        ),
        "{context}: expected InvalidTransition variant, got: {err}"
    );
}
