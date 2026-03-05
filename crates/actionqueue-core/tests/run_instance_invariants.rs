//! Contract tests for run construction and lifecycle invariant errors.

use actionqueue_core::ids::{RunId, TaskId};
use actionqueue_core::run::{
    RunInstance, RunInstanceConstructionError, RunInstanceError, RunState,
};

#[test]
fn new_ready_with_id_rejects_scheduled_after_created() {
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(1));
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(2));

    let result = RunInstance::new_ready_with_id(run_id, task_id, 2_000, 1_000, 5);

    assert_eq!(
        result,
        Err(RunInstanceConstructionError::ReadyScheduledAtAfterCreatedAt {
            run_id,
            scheduled_at: 2_000,
            created_at: 1_000,
        })
    );
}

#[test]
fn new_scheduled_with_id_rejects_nil_run_id() {
    let run_id = RunId::from_uuid(uuid::Uuid::nil());
    let task_id = TaskId::from_uuid(uuid::Uuid::from_u128(2));

    let result = RunInstance::new_scheduled_with_id(run_id, task_id, 1_000, 1_000);

    assert_eq!(result, Err(RunInstanceConstructionError::InvalidRunId { run_id }));
}

#[test]
fn new_scheduled_with_id_rejects_nil_task_id() {
    let run_id = RunId::from_uuid(uuid::Uuid::from_u128(1));
    let task_id = TaskId::from_uuid(uuid::Uuid::nil());

    let result = RunInstance::new_scheduled_with_id(run_id, task_id, 1_000, 1_000);

    assert_eq!(result, Err(RunInstanceConstructionError::InvalidTaskId { task_id }));
}

#[test]
fn transition_to_returns_typed_error_for_illegal_transition() {
    let mut run =
        RunInstance::new_scheduled(TaskId::new(), 1_000, 1_000).expect("valid scheduled run");

    let result = run.transition_to(RunState::Running);

    assert!(matches!(
        result,
        Err(RunInstanceError::InvalidTransition {
            from: RunState::Scheduled,
            to: RunState::Running,
            ..
        })
    ));
}
