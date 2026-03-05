use actionqueue_core::budget::BudgetDimension;
use actionqueue_core::ids::{AttemptId, RunId, TaskId};
use actionqueue_core::mutation::{
    AppliedMutation, AttemptFinishCommand, AttemptOutcome, AttemptResultKind, AttemptStartCommand,
    BudgetAllocateCommand, BudgetConsumeCommand, BudgetReplenishCommand, DurabilityPolicy,
    EnginePauseCommand, EngineResumeCommand, LeaseAcquireCommand, LeaseExpireCommand,
    LeaseHeartbeatCommand, LeaseReleaseCommand, MutationAuthority, MutationCommand,
    RunCreateCommand, RunResumeCommand, RunStateTransitionCommand, RunSuspendCommand,
    SubscriptionCancelCommand, SubscriptionCreateCommand, TaskCancelCommand, TaskCreateCommand,
};
use actionqueue_core::run::run_instance::RunInstance;
use actionqueue_core::run::state::RunState;
use actionqueue_core::subscription::{EventFilter, SubscriptionId};
use actionqueue_core::task::constraints::TaskConstraints;
use actionqueue_core::task::metadata::TaskMetadata;
use actionqueue_core::task::run_policy::RunPolicy;
use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
use actionqueue_engine::index::scheduled::ScheduledIndex;
use actionqueue_engine::scheduler::attempt_finish::submit_attempt_finish_via_authority;
use actionqueue_engine::scheduler::promotion::{
    promote_scheduled_to_ready, promote_scheduled_to_ready_via_authority, PromotionParams,
};
use actionqueue_executor_local::ExecutorResponse;
use actionqueue_storage::mutation::authority::{
    MutationAuthorityError, MutationProjection, MutationValidationError, StorageMutationAuthority,
};
use actionqueue_storage::recovery::reducer::ReplayReducer;
use actionqueue_storage::recovery::replay::ReplayDriver;
use actionqueue_storage::wal::event::{WalEvent, WalEventType};
use actionqueue_storage::wal::fs_reader::WalFsReader;
use actionqueue_storage::wal::fs_writer::WalFsWriter;
use actionqueue_storage::wal::writer::{WalWriter, WalWriterError};

#[derive(Debug, Default)]
struct InMemoryProjection {
    latest_sequence: u64,
    tasks: std::collections::HashSet<TaskId>,
    canceled_tasks: std::collections::HashSet<TaskId>,
    engine_paused: bool,
    runs: std::collections::HashMap<RunId, RunState>,
    active_attempts: std::collections::HashMap<RunId, AttemptId>,
    active_leases: std::collections::HashMap<RunId, (String, u64)>,
    budget_allocations: std::collections::HashSet<(TaskId, BudgetDimension)>,
    subscriptions: std::collections::HashMap<SubscriptionId, bool>,
    fail_apply: bool,
}

impl MutationProjection for InMemoryProjection {
    type Error = &'static str;

    fn latest_sequence(&self) -> u64 {
        self.latest_sequence
    }

    fn run_state(&self, run_id: &RunId) -> Option<RunState> {
        self.runs.get(run_id).copied()
    }

    fn task_exists(&self, task_id: TaskId) -> bool {
        self.tasks.contains(&task_id)
    }

    fn is_task_canceled(&self, task_id: TaskId) -> bool {
        self.canceled_tasks.contains(&task_id)
    }

    fn is_engine_paused(&self) -> bool {
        self.engine_paused
    }

    fn active_attempt_id(&self, run_id: &RunId) -> Option<AttemptId> {
        self.active_attempts.get(run_id).copied()
    }

    fn active_lease(&self, run_id: &RunId) -> Option<(String, u64)> {
        self.active_leases.get(run_id).cloned()
    }

    fn budget_allocation_exists(&self, task_id: TaskId, dimension: BudgetDimension) -> bool {
        self.budget_allocations.contains(&(task_id, dimension))
    }

    fn subscription_exists(&self, subscription_id: SubscriptionId) -> bool {
        self.subscriptions.contains_key(&subscription_id)
    }

    fn is_subscription_canceled(&self, subscription_id: SubscriptionId) -> bool {
        self.subscriptions.get(&subscription_id).copied().unwrap_or(false)
    }

    fn apply_event(&mut self, event: &WalEvent) -> Result<(), Self::Error> {
        if self.fail_apply {
            return Err("apply failed");
        }

        self.latest_sequence = event.sequence();
        match event.event() {
            WalEventType::TaskCreated { task_spec, .. } => {
                self.tasks.insert(task_spec.id());
            }
            WalEventType::RunCreated { run_instance } => {
                self.runs.insert(run_instance.id(), run_instance.state());
            }
            WalEventType::RunStateChanged { run_id, new_state, .. } => {
                self.runs.insert(*run_id, *new_state);
            }
            WalEventType::AttemptStarted { run_id, attempt_id, .. } => {
                self.active_attempts.insert(*run_id, *attempt_id);
            }
            WalEventType::AttemptFinished { run_id, .. } => {
                self.active_attempts.remove(run_id);
            }
            WalEventType::LeaseAcquired { run_id, owner, expiry, .. } => {
                self.active_leases.insert(*run_id, (owner.clone(), *expiry));
            }
            WalEventType::LeaseHeartbeat { run_id, owner, expiry, .. } => {
                self.active_leases.insert(*run_id, (owner.clone(), *expiry));
            }
            WalEventType::LeaseExpired { run_id, .. }
            | WalEventType::LeaseReleased { run_id, .. } => {
                self.active_leases.remove(run_id);
            }
            WalEventType::TaskCanceled { task_id, .. } => {
                self.canceled_tasks.insert(*task_id);
            }
            WalEventType::EnginePaused { .. } => {
                self.engine_paused = true;
            }
            WalEventType::EngineResumed { .. } => {
                self.engine_paused = false;
            }
            WalEventType::RunSuspended { run_id, .. } => {
                self.runs.insert(*run_id, RunState::Suspended);
            }
            WalEventType::RunResumed { run_id, .. } => {
                self.runs.insert(*run_id, RunState::Ready);
            }
            WalEventType::BudgetAllocated { task_id, dimension, .. } => {
                self.budget_allocations.insert((*task_id, *dimension));
            }
            WalEventType::BudgetConsumed { .. } => {
                // No-op: BudgetTracker handles consumption accounting.
            }
            WalEventType::BudgetReplenished { .. } => {
                // No-op: BudgetTracker handles replenishment accounting.
            }
            WalEventType::SubscriptionCreated { subscription_id, .. } => {
                self.subscriptions.insert(*subscription_id, false);
            }
            WalEventType::SubscriptionTriggered { .. } => {
                // No-op: SubscriptionRegistry handles trigger tracking.
            }
            WalEventType::SubscriptionCanceled { subscription_id, .. } => {
                self.subscriptions.insert(*subscription_id, true);
            }
            _ => {
                // Remaining event types (DependencyDeclared, BudgetExhausted)
                // are not tracked in the lightweight InMemoryProjection test stub.
            }
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
struct RecordingWriter {
    events: Vec<WalEvent>,
    fail_append: bool,
    fail_flush: bool,
}

impl WalWriter for RecordingWriter {
    fn append(&mut self, event: &WalEvent) -> Result<(), WalWriterError> {
        if self.fail_append {
            return Err(WalWriterError::IoError("append failed".to_string()));
        }
        self.events.push(event.clone());
        Ok(())
    }

    fn flush(&mut self) -> Result<(), WalWriterError> {
        if self.fail_flush {
            return Err(WalWriterError::IoError("flush failed".to_string()));
        }
        Ok(())
    }

    fn close(self) -> Result<(), WalWriterError> {
        Ok(())
    }
}

fn task_spec(task_id: TaskId) -> TaskSpec {
    TaskSpec::new(
        task_id,
        TaskPayload::with_content_type(b"payload".to_vec(), "application/octet-stream"),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .expect("task spec should be valid")
}

fn scheduled_run(task_id: TaskId, scheduled_at: u64, created_at: u64) -> RunInstance {
    RunInstance::new_scheduled(task_id, scheduled_at, created_at).expect("run should be valid")
}

#[test]
fn d04_t_p1_valid_command_flows_through_authority() {
    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 100, 100);

    let writer = RecordingWriter::default();
    let projection = InMemoryProjection::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let _ = authority
        .submit_command(
            MutationCommand::TaskCreate(TaskCreateCommand::new(1, task_spec(task_id), 10)),
            DurabilityPolicy::Immediate,
        )
        .expect("task create should succeed");

    let run_created = authority
        .submit_command(
            MutationCommand::RunCreate(RunCreateCommand::new(2, run.clone())),
            DurabilityPolicy::Immediate,
        )
        .expect("run create should succeed");
    assert_eq!(run_created.sequence(), 2);
    assert!(matches!(
        run_created.applied(),
        AppliedMutation::RunCreate { run_id, task_id: tid } if *run_id == run.id() && *tid == task_id
    ));

    let transitioned = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                3,
                run.id(),
                RunState::Scheduled,
                RunState::Ready,
                100,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("transition should succeed");

    assert_eq!(transitioned.sequence(), 3);
    let (writer, projection) = authority.into_parts();
    assert_eq!(writer.events.len(), 3);
    assert_eq!(projection.run_state(&run.id()), Some(RunState::Ready));
}

#[test]
fn d04_t_n1_validation_failure_does_not_append_or_apply() {
    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 100, 100);

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.runs.insert(run.id(), RunState::Scheduled);
    projection.latest_sequence = 2;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
            3,
            run.id(),
            RunState::Running,
            RunState::Ready,
            100,
        )),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(
        result,
        Err(MutationAuthorityError::Validation(
            MutationValidationError::PreviousStateMismatch { .. }
        ))
    ));

    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert_eq!(projection.latest_sequence, 2);
    assert_eq!(projection.run_state(&run.id()), Some(RunState::Scheduled));
}

#[test]
fn d04_t_n2_append_failure_does_not_apply() {
    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 100, 100);

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.runs.insert(run.id(), RunState::Scheduled);
    projection.latest_sequence = 2;

    let writer = RecordingWriter { fail_append: true, ..RecordingWriter::default() };
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
            3,
            run.id(),
            RunState::Scheduled,
            RunState::Ready,
            100,
        )),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(result, Err(MutationAuthorityError::Append(_))));
    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert_eq!(projection.latest_sequence, 2);
    assert_eq!(projection.run_state(&run.id()), Some(RunState::Scheduled));
}

#[test]
fn d04_t_n3_flush_failure_reports_durability_stage() {
    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 100, 100);

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.runs.insert(run.id(), RunState::Scheduled);
    projection.latest_sequence = 2;

    let writer = RecordingWriter { fail_flush: true, ..RecordingWriter::default() };
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
            3,
            run.id(),
            RunState::Scheduled,
            RunState::Ready,
            100,
        )),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(result, Err(MutationAuthorityError::PartialDurability { sequence: 3, .. })));
    let (writer, projection) = authority.into_parts();
    assert_eq!(writer.events.len(), 1);
    assert_eq!(projection.latest_sequence, 2);
    assert_eq!(projection.run_state(&run.id()), Some(RunState::Scheduled));
}

#[test]
fn d04_t_n4_append_success_apply_failure_exposes_replay_recovery_semantics() {
    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 100, 100);

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.runs.insert(run.id(), RunState::Scheduled);
    projection.latest_sequence = 2;
    projection.fail_apply = true;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
            3,
            run.id(),
            RunState::Scheduled,
            RunState::Ready,
            100,
        )),
        DurabilityPolicy::Deferred,
    );

    assert!(matches!(result, Err(MutationAuthorityError::Apply { sequence: 3, .. })));

    let (writer, projection) = authority.into_parts();
    assert_eq!(writer.events.len(), 1);
    assert_eq!(projection.run_state(&run.id()), Some(RunState::Scheduled));

    let mut replay = ReplayReducer::new();
    replay
        .apply(&WalEvent::new(
            1,
            WalEventType::TaskCreated { task_spec: task_spec(task_id), timestamp: 10 },
        ))
        .expect("task apply should succeed");
    replay
        .apply(&WalEvent::new(2, WalEventType::RunCreated { run_instance: run.clone() }))
        .expect("run apply should succeed");
    replay.apply(&writer.events[0]).expect("replay apply should converge with durable event");
    assert_eq!(replay.get_run_state(&run.id()), Some(&RunState::Ready));
}

#[test]
fn d04_t_n5_non_monotonic_sequence_rejected() {
    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 100, 100);

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.runs.insert(run.id(), RunState::Scheduled);
    projection.latest_sequence = 4;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
            4,
            run.id(),
            RunState::Scheduled,
            RunState::Ready,
            100,
        )),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(
        result,
        Err(MutationAuthorityError::Validation(MutationValidationError::NonMonotonicSequence {
            expected: 5,
            provided: 4
        }))
    ));
    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert_eq!(projection.latest_sequence, 4);
}

#[test]
fn p6_011_t_p1_task_cancel_success_appends_canonical_event_and_applies_projection() {
    let task_id = TaskId::new();

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.latest_sequence = 3;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let outcome = authority
        .submit_command(
            MutationCommand::TaskCancel(TaskCancelCommand::new(4, task_id, 5_000)),
            DurabilityPolicy::Immediate,
        )
        .expect("task cancel command should succeed");

    assert_eq!(outcome.sequence(), 4);
    assert!(matches!(
        outcome.applied(),
        AppliedMutation::TaskCancel { task_id: applied_task_id } if *applied_task_id == task_id
    ));

    let (writer, projection) = authority.into_parts();
    assert_eq!(writer.events.len(), 1);
    assert!(matches!(
        writer.events[0].event(),
        WalEventType::TaskCanceled { task_id: event_task_id, timestamp }
            if *event_task_id == task_id && *timestamp == 5_000
    ));
    assert!(projection.is_task_canceled(task_id));
    assert_eq!(projection.latest_sequence, 4);
}

#[test]
fn p6_011_t_n1_task_cancel_unknown_task_is_rejected_pre_append() {
    let missing_task_id = TaskId::new();

    let writer = RecordingWriter::default();
    let projection = InMemoryProjection::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::TaskCancel(TaskCancelCommand::new(1, missing_task_id, 1_000)),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(
        result,
        Err(MutationAuthorityError::Validation(MutationValidationError::UnknownTask { task_id }))
            if task_id == missing_task_id
    ));

    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert_eq!(projection.latest_sequence, 0);
}

#[test]
fn p6_011_t_n2_task_cancel_already_canceled_is_rejected_pre_append() {
    let task_id = TaskId::new();

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.canceled_tasks.insert(task_id);
    projection.latest_sequence = 7;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::TaskCancel(TaskCancelCommand::new(8, task_id, 2_000)),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(
        result,
        Err(MutationAuthorityError::Validation(
            MutationValidationError::TaskAlreadyCanceled { task_id: canceled_task_id }
        )) if canceled_task_id == task_id
    ));

    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert_eq!(projection.latest_sequence, 7);
}

#[test]
fn d04_t_p2_replay_driver_converges_with_authority_written_wal() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir should be created");
    let wal_path = temp_dir.path().join("d04-authority-converge.wal");

    let writer = WalFsWriter::new(wal_path.clone()).expect("wal writer should open");
    let projection = ReplayReducer::new();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 1_000, 1_000);

    let _ = authority
        .submit_command(
            MutationCommand::TaskCreate(TaskCreateCommand::new(1, task_spec(task_id), 10)),
            DurabilityPolicy::Immediate,
        )
        .expect("task create should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunCreate(RunCreateCommand::new(2, run.clone())),
            DurabilityPolicy::Immediate,
        )
        .expect("run create should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                3,
                run.id(),
                RunState::Scheduled,
                RunState::Ready,
                1_000,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("transition should succeed");

    let (_writer, projected) = authority.into_parts();
    let projection_state =
        projected.get_run_state(&run.id()).copied().expect("projection state should exist");

    let reader = WalFsReader::new(wal_path.clone()).expect("wal reader should open");
    let mut replay_driver = ReplayDriver::new(reader, ReplayReducer::new());
    replay_driver.run().expect("replay should succeed");
    let replayed = replay_driver.into_reducer();

    assert_eq!(replayed.get_run_state(&run.id()).copied(), Some(projection_state));
    assert_eq!(replayed.latest_sequence(), 3);
}

#[test]
fn d04_t_p3_engine_scheduler_path_uses_storage_authority_end_to_end() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir should be created");
    let wal_path = temp_dir.path().join("d04-engine-authority-path.wal");

    let writer = WalFsWriter::new(wal_path.clone()).expect("wal writer should open");
    let projection = ReplayReducer::new();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let task_id = TaskId::new();
    let run_due = scheduled_run(task_id, 1_000, 1_000);
    let run_future = scheduled_run(task_id, 1_500, 1_000);

    let _ = authority
        .submit_command(
            MutationCommand::TaskCreate(TaskCreateCommand::new(1, task_spec(task_id), 10)),
            DurabilityPolicy::Immediate,
        )
        .expect("task create should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunCreate(RunCreateCommand::new(2, run_due.clone())),
            DurabilityPolicy::Immediate,
        )
        .expect("run due create should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunCreate(RunCreateCommand::new(3, run_future.clone())),
            DurabilityPolicy::Immediate,
        )
        .expect("run future create should succeed");

    let scheduled_index = ScheduledIndex::from_runs(vec![run_due.clone(), run_future.clone()]);
    let promotion = promote_scheduled_to_ready_via_authority(
        &scheduled_index,
        PromotionParams::new(1_000, 4, 1_000, DurabilityPolicy::Immediate),
        &mut authority,
    )
    .expect("authority-mediated promotion should succeed");

    assert_eq!(promotion.outcomes().len(), 1);
    assert_eq!(promotion.outcomes()[0].sequence(), 4);
    assert_eq!(promotion.remaining_scheduled().len(), 1);
    assert_eq!(promotion.remaining_scheduled()[0].id(), run_future.id());

    let (_writer, projected) = authority.into_parts();
    assert_eq!(projected.get_run_state(&run_due.id()), Some(&RunState::Ready));
    assert_eq!(projected.get_run_state(&run_future.id()), Some(&RunState::Scheduled));

    let reader = WalFsReader::new(wal_path.clone()).expect("wal reader should open");
    let mut replay_driver = ReplayDriver::new(reader, ReplayReducer::new());
    replay_driver.run().expect("replay should succeed");
    let replayed = replay_driver.into_reducer();

    assert_eq!(replayed.latest_sequence(), 4);
    assert_eq!(replayed.get_run_state(&run_due.id()), Some(&RunState::Ready));
    assert_eq!(replayed.get_run_state(&run_future.id()), Some(&RunState::Scheduled));
}

#[test]
fn d04_t_n6_non_authority_scheduler_path_does_not_persist_mutation() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir should be created");
    let wal_path = temp_dir.path().join("d04-n6-no-authority-submit.wal");

    let writer = WalFsWriter::new(wal_path.clone()).expect("wal writer should open");
    let projection = ReplayReducer::new();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 1_000, 1_000);

    let _ = authority
        .submit_command(
            MutationCommand::TaskCreate(TaskCreateCommand::new(1, task_spec(task_id), 10)),
            DurabilityPolicy::Immediate,
        )
        .expect("task create should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunCreate(RunCreateCommand::new(2, run.clone())),
            DurabilityPolicy::Immediate,
        )
        .expect("run create should succeed");

    let scheduled_index = ScheduledIndex::from_runs(vec![run.clone()]);
    let promoted = promote_scheduled_to_ready(&scheduled_index, 1_000)
        .expect("promotion should succeed for valid scheduled runs");
    assert_eq!(promoted.promoted().len(), 1);
    assert_eq!(promoted.promoted()[0].state(), RunState::Ready);

    // No authority command was submitted for promotion, so durable/projection state stays unchanged.
    assert_eq!(authority.projection().latest_sequence(), 2);
    assert_eq!(authority.projection().get_run_state(&run.id()), Some(&RunState::Scheduled));

    drop(authority);

    let reader = WalFsReader::new(wal_path.clone()).expect("wal reader should open");
    let mut replay_driver = ReplayDriver::new(reader, ReplayReducer::new());
    replay_driver.run().expect("replay should succeed");
    let replayed = replay_driver.into_reducer();

    assert_eq!(replayed.latest_sequence(), 2);
    assert_eq!(replayed.get_run_state(&run.id()), Some(&RunState::Scheduled));
}

#[test]
fn f002_t_p1_authority_accepts_attempt_start_and_appends_canonical_event() {
    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 100, 100);
    let attempt_id = AttemptId::new();

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.runs.insert(run.id(), RunState::Running);
    projection.latest_sequence = 3;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let outcome = authority
        .submit_command(
            MutationCommand::AttemptStart(AttemptStartCommand::new(4, run.id(), attempt_id, 1_500)),
            DurabilityPolicy::Immediate,
        )
        .expect("attempt-start command should succeed");

    assert_eq!(outcome.sequence(), 4);
    assert!(matches!(
        outcome.applied(),
        AppliedMutation::AttemptStart { run_id, attempt_id: applied_attempt_id }
            if *run_id == run.id() && *applied_attempt_id == attempt_id
    ));

    let (writer, projection) = authority.into_parts();
    assert_eq!(writer.events.len(), 1);
    assert!(matches!(
        writer.events[0].event(),
        WalEventType::AttemptStarted {
            run_id,
            attempt_id: event_attempt_id,
            timestamp
        } if *run_id == run.id() && *event_attempt_id == attempt_id && *timestamp == 1_500
    ));
    assert_eq!(projection.active_attempt_id(&run.id()), Some(attempt_id));
}

#[test]
fn f002_t_p2_authority_attempt_finish_converges_with_replay_attempt_lineage() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir should be created");
    let wal_path = temp_dir.path().join("f002-attempt-lineage-converge.wal");

    let writer = WalFsWriter::new(wal_path.clone()).expect("wal writer should open");
    let projection = ReplayReducer::new();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 1_000, 1_000);
    let attempt_id = AttemptId::new();

    let _ = authority
        .submit_command(
            MutationCommand::TaskCreate(TaskCreateCommand::new(1, task_spec(task_id), 10)),
            DurabilityPolicy::Immediate,
        )
        .expect("task create should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunCreate(RunCreateCommand::new(2, run.clone())),
            DurabilityPolicy::Immediate,
        )
        .expect("run create should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                3,
                run.id(),
                RunState::Scheduled,
                RunState::Ready,
                1_000,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("scheduled->ready should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                4,
                run.id(),
                RunState::Ready,
                RunState::Leased,
                1_000,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("ready->leased should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                5,
                run.id(),
                RunState::Leased,
                RunState::Running,
                1_000,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("leased->running should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::AttemptStart(AttemptStartCommand::new(6, run.id(), attempt_id, 1_100)),
            DurabilityPolicy::Immediate,
        )
        .expect("attempt start should succeed");

    let attempt_finish = authority
        .submit_command(
            MutationCommand::AttemptFinish(AttemptFinishCommand::new(
                7,
                run.id(),
                attempt_id,
                AttemptOutcome::failure("synthetic failure"),
                1_200,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("attempt finish should succeed");
    assert_eq!(attempt_finish.sequence(), 7);
    assert!(matches!(
        attempt_finish.applied(),
        AppliedMutation::AttemptFinish {
            run_id,
            attempt_id: applied_attempt_id,
            outcome: ref o,
        } if *run_id == run.id() && *applied_attempt_id == attempt_id
            && o.result() == AttemptResultKind::Failure
            && o.error() == Some("synthetic failure")
    ));

    let (_writer, projected) = authority.into_parts();
    let projected_instance =
        projected.get_run_instance(&run.id()).expect("run instance should exist");
    assert_eq!(projected_instance.attempt_count(), 1);
    assert_eq!(projected_instance.current_attempt_id(), None);

    let reader = WalFsReader::new(wal_path.clone()).expect("wal reader should open");
    let mut replay_driver = ReplayDriver::new(reader, ReplayReducer::new());
    replay_driver.run().expect("replay should succeed");
    let replayed = replay_driver.into_reducer();

    let replayed_instance =
        replayed.get_run_instance(&run.id()).expect("replayed run instance should exist");
    assert_eq!(replayed.latest_sequence(), 7);
    assert_eq!(replayed_instance.attempt_count(), projected_instance.attempt_count());
    assert_eq!(replayed_instance.current_attempt_id(), projected_instance.current_attempt_id());
}

#[test]
fn p6_017_t_p1_authority_attempt_finish_timeout_persists_in_projection_and_replay() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir should be created");
    let wal_path = temp_dir.path().join("p6-017-attempt-timeout-parity.wal");

    let writer = WalFsWriter::new(wal_path.clone()).expect("wal writer should open");
    let projection = ReplayReducer::new();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 1_000, 1_000);
    let attempt_id = AttemptId::new();

    let _ = authority
        .submit_command(
            MutationCommand::TaskCreate(TaskCreateCommand::new(1, task_spec(task_id), 10)),
            DurabilityPolicy::Immediate,
        )
        .expect("task create should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunCreate(RunCreateCommand::new(2, run.clone())),
            DurabilityPolicy::Immediate,
        )
        .expect("run create should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                3,
                run.id(),
                RunState::Scheduled,
                RunState::Ready,
                1_000,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("scheduled->ready should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                4,
                run.id(),
                RunState::Ready,
                RunState::Leased,
                1_000,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("ready->leased should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                5,
                run.id(),
                RunState::Leased,
                RunState::Running,
                1_000,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("leased->running should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::AttemptStart(AttemptStartCommand::new(6, run.id(), attempt_id, 1_100)),
            DurabilityPolicy::Immediate,
        )
        .expect("attempt start should succeed");

    let timeout_finish = {
        let __finish_cmd =
            actionqueue_engine::scheduler::attempt_finish::build_attempt_finish_command(
                7,
                run.id(),
                attempt_id,
                &ExecutorResponse::Timeout { timeout_secs: 9 },
                1_200,
            );
        submit_attempt_finish_via_authority(
            __finish_cmd,
            DurabilityPolicy::Immediate,
            &mut authority,
        )
    }
    .expect("attempt timeout finish should succeed");

    assert!(matches!(
        timeout_finish.applied(),
        AppliedMutation::AttemptFinish {
            run_id,
            attempt_id: applied_attempt_id,
            outcome: ref o,
        } if *run_id == run.id()
            && *applied_attempt_id == attempt_id
            && o.result() == AttemptResultKind::Timeout
            && o.error() == Some("attempt timed out after 9s")
    ));

    let (_writer, projected) = authority.into_parts();
    let projected_history =
        projected.get_attempt_history(&run.id()).expect("projection attempt history should exist");
    assert_eq!(projected_history.len(), 1);
    assert_eq!(projected_history[0].attempt_id(), attempt_id);
    assert_eq!(projected_history[0].result(), Some(AttemptResultKind::Timeout));
    assert_eq!(projected_history[0].error(), Some("attempt timed out after 9s"));
    assert_eq!(projected_history[0].finished_at(), Some(1_200));

    let reader = WalFsReader::new(wal_path.clone()).expect("wal reader should open");
    let mut replay_driver = ReplayDriver::new(reader, ReplayReducer::new());
    replay_driver.run().expect("replay should succeed");
    let replayed = replay_driver.into_reducer();

    let replayed_history =
        replayed.get_attempt_history(&run.id()).expect("replay attempt history should exist");
    assert_eq!(replayed_history.len(), 1);
    assert_eq!(replayed_history[0].attempt_id(), attempt_id);
    assert_eq!(replayed_history[0].result(), Some(AttemptResultKind::Timeout));
    assert_eq!(replayed_history[0].error(), Some("attempt timed out after 9s"));
    assert_eq!(replayed_history[0].finished_at(), Some(1_200));
}

#[test]
fn f002_t_p3_authority_lease_lifecycle_chain_converges_with_replay() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir should be created");
    let wal_path = temp_dir.path().join("f002-lease-lifecycle-converge.wal");

    let writer = WalFsWriter::new(wal_path.clone()).expect("wal writer should open");
    let projection = ReplayReducer::new();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 1_000, 1_000);

    let _ = authority
        .submit_command(
            MutationCommand::TaskCreate(TaskCreateCommand::new(1, task_spec(task_id), 10)),
            DurabilityPolicy::Immediate,
        )
        .expect("task create should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunCreate(RunCreateCommand::new(2, run.clone())),
            DurabilityPolicy::Immediate,
        )
        .expect("run create should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                3,
                run.id(),
                RunState::Scheduled,
                RunState::Ready,
                1_000,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("scheduled->ready should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::LeaseAcquire(LeaseAcquireCommand::new(
                4,
                run.id(),
                "worker-a",
                5_000,
                1_050,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("lease acquire should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                5,
                run.id(),
                RunState::Ready,
                RunState::Leased,
                1_050,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("ready->leased should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::LeaseHeartbeat(LeaseHeartbeatCommand::new(
                6,
                run.id(),
                "worker-a",
                6_000,
                1_100,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("lease heartbeat should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::LeaseRelease(LeaseReleaseCommand::new(
                7,
                run.id(),
                "worker-a",
                6_000,
                1_150,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("lease release should succeed");

    let (_writer, projected) = authority.into_parts();
    assert_eq!(projected.latest_sequence(), 7);
    assert_eq!(projected.get_run_state(&run.id()), Some(&RunState::Ready));
    assert!(projected.get_lease(&run.id()).is_none());

    let reader = WalFsReader::new(wal_path.clone()).expect("wal reader should open");
    let mut replay_driver = ReplayDriver::new(reader, ReplayReducer::new());
    replay_driver.run().expect("replay should succeed");
    let replayed = replay_driver.into_reducer();

    assert_eq!(replayed.latest_sequence(), 7);
    assert_eq!(replayed.get_run_state(&run.id()), Some(&RunState::Ready));
    assert!(replayed.get_lease(&run.id()).is_none());
}

#[test]
fn f002_t_n1_unknown_run_attempt_command_is_rejected_pre_append() {
    let missing_run_id = RunId::new();
    let attempt_id = AttemptId::new();

    let writer = RecordingWriter::default();
    let projection = InMemoryProjection::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::AttemptStart(AttemptStartCommand::new(
            1,
            missing_run_id,
            attempt_id,
            1_000,
        )),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(
        result,
        Err(MutationAuthorityError::Validation(
            MutationValidationError::AttemptStartUnknownRun { run_id }
        )) if run_id == missing_run_id
    ));

    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert_eq!(projection.latest_sequence, 0);
}

#[test]
fn f002_t_n2_mismatched_attempt_finish_is_rejected_pre_append() {
    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 100, 100);
    let active_attempt_id = AttemptId::new();
    let provided_attempt_id = AttemptId::new();

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.runs.insert(run.id(), RunState::Running);
    projection.active_attempts.insert(run.id(), active_attempt_id);
    projection.latest_sequence = 5;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::AttemptFinish(AttemptFinishCommand::new(
            6,
            run.id(),
            provided_attempt_id,
            AttemptOutcome::success(),
            1_100,
        )),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(
        result,
        Err(MutationAuthorityError::Validation(
            MutationValidationError::AttemptFinishAttemptMismatch {
                run_id,
                expected_attempt_id,
                provided_attempt_id: observed_provided_attempt_id,
            }
        )) if run_id == run.id()
            && expected_attempt_id == active_attempt_id
            && observed_provided_attempt_id == provided_attempt_id
    ));

    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert_eq!(projection.active_attempt_id(&run.id()), Some(active_attempt_id));
    assert_eq!(projection.latest_sequence, 5);
}

#[test]
fn f002_t_n3_lease_heartbeat_owner_mismatch_is_rejected_pre_append() {
    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 100, 100);

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.runs.insert(run.id(), RunState::Leased);
    projection.active_leases.insert(run.id(), ("worker-a".to_string(), 5_000));
    projection.latest_sequence = 8;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::LeaseHeartbeat(LeaseHeartbeatCommand::new(
            9,
            run.id(),
            "worker-b",
            6_000,
            1_200,
        )),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(
        result,
        Err(MutationAuthorityError::Validation(MutationValidationError::LeaseOwnerMismatch {
            run_id,
            event,
        })) if run_id == run.id()
            && event == actionqueue_storage::mutation::authority::LeaseValidationEvent::Heartbeat
    ));

    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert_eq!(projection.active_lease(&run.id()), Some(("worker-a".to_string(), 5_000)));
    assert_eq!(projection.latest_sequence, 8);
}

#[test]
fn f002_t_n4_lease_close_without_active_lease_is_rejected_pre_append() {
    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 100, 100);

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.runs.insert(run.id(), RunState::Leased);
    projection.latest_sequence = 2;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let release = authority.submit_command(
        MutationCommand::LeaseRelease(LeaseReleaseCommand::new(
            3,
            run.id(),
            "worker-a",
            5_000,
            1_000,
        )),
        DurabilityPolicy::Immediate,
    );
    assert!(matches!(
        release,
        Err(MutationAuthorityError::Validation(MutationValidationError::LeaseMissingActive {
            run_id,
            event,
        })) if run_id == run.id()
            && event == actionqueue_storage::mutation::authority::LeaseValidationEvent::Release
    ));

    let expire = authority.submit_command(
        MutationCommand::LeaseExpire(LeaseExpireCommand::new(
            3,
            run.id(),
            "worker-a",
            5_000,
            1_000,
        )),
        DurabilityPolicy::Immediate,
    );
    assert!(matches!(
        expire,
        Err(MutationAuthorityError::Validation(MutationValidationError::LeaseMissingActive {
            run_id,
            event,
        })) if run_id == run.id()
            && event == actionqueue_storage::mutation::authority::LeaseValidationEvent::Expire
    ));

    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert_eq!(projection.latest_sequence, 2);
}

#[test]
fn f002_t_n5_non_authority_projection_apply_cannot_persist_attempt_or_lease_mutation() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir should be created");
    let wal_path = temp_dir.path().join("f002-non-authority-ephemeral-attempt-lease.wal");

    let writer = WalFsWriter::new(wal_path.clone()).expect("wal writer should open");
    let projection = ReplayReducer::new();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 1_000, 1_000);

    let _ = authority
        .submit_command(
            MutationCommand::TaskCreate(TaskCreateCommand::new(1, task_spec(task_id), 10)),
            DurabilityPolicy::Immediate,
        )
        .expect("task create should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunCreate(RunCreateCommand::new(2, run.clone())),
            DurabilityPolicy::Immediate,
        )
        .expect("run create should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                3,
                run.id(),
                RunState::Scheduled,
                RunState::Ready,
                1_000,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("scheduled->ready should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::LeaseAcquire(LeaseAcquireCommand::new(
                4,
                run.id(),
                "worker-1",
                5_000,
                1_050,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("lease acquire should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                5,
                run.id(),
                RunState::Ready,
                RunState::Leased,
                1_050,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("ready->leased should succeed");
    let _ = authority
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                6,
                run.id(),
                RunState::Leased,
                RunState::Running,
                1_100,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("leased->running should succeed");

    let non_durable_attempt_id = AttemptId::new();
    authority
        .projection_mut()
        .apply_event(&WalEvent::new(
            7,
            WalEventType::AttemptStarted {
                run_id: run.id(),
                attempt_id: non_durable_attempt_id,
                timestamp: 1_200,
            },
        ))
        .expect("non-authority projection-only attempt apply should succeed");
    authority
        .projection_mut()
        .apply_event(&WalEvent::new(
            8,
            WalEventType::LeaseHeartbeat {
                run_id: run.id(),
                owner: "worker-1".to_string(),
                expiry: 6_000,
                timestamp: 1_250,
            },
        ))
        .expect("non-authority projection-only lease heartbeat apply should succeed");

    assert_eq!(authority.projection().latest_sequence(), 8);
    assert_eq!(
        authority
            .projection()
            .get_run_instance(&run.id())
            .and_then(|run_instance| run_instance.current_attempt_id()),
        Some(non_durable_attempt_id)
    );
    assert_eq!(authority.projection().get_lease(&run.id()), Some(&("worker-1".to_string(), 6_000)));

    drop(authority);

    let reader = WalFsReader::new(wal_path.clone()).expect("wal reader should open");
    let mut replay_driver = ReplayDriver::new(reader, ReplayReducer::new());
    replay_driver.run().expect("replay should succeed");
    let replayed = replay_driver.into_reducer();

    assert_eq!(replayed.latest_sequence(), 6);
    assert_eq!(
        replayed
            .get_run_instance(&run.id())
            .and_then(|run_instance| run_instance.current_attempt_id()),
        None,
        "attempt state not appended through authority must not persist"
    );
    assert_eq!(
        replayed.get_run_instance(&run.id()).map(|run_instance| run_instance.attempt_count()),
        Some(0),
        "attempt count should remain unchanged without authority append"
    );
    assert_eq!(
        replayed.get_lease(&run.id()),
        Some(&("worker-1".to_string(), 5_000)),
        "lease heartbeat not appended through authority must not persist"
    );
}

#[test]
fn p6_013_t_p1_engine_pause_and_resume_map_to_canonical_events() {
    let projection = InMemoryProjection { latest_sequence: 0, ..InMemoryProjection::default() };

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let pause = authority
        .submit_command(
            MutationCommand::EnginePause(EnginePauseCommand::new(1, 1_000)),
            DurabilityPolicy::Immediate,
        )
        .expect("engine pause should succeed");
    assert_eq!(pause.sequence(), 1);
    assert!(matches!(pause.applied(), AppliedMutation::EnginePause));

    let resume = authority
        .submit_command(
            MutationCommand::EngineResume(EngineResumeCommand::new(2, 2_000)),
            DurabilityPolicy::Immediate,
        )
        .expect("engine resume should succeed");
    assert_eq!(resume.sequence(), 2);
    assert!(matches!(resume.applied(), AppliedMutation::EngineResume));

    let (writer, projection) = authority.into_parts();
    assert_eq!(writer.events.len(), 2);
    assert!(matches!(writer.events[0].event(), WalEventType::EnginePaused { timestamp: 1_000 }));
    assert!(matches!(writer.events[1].event(), WalEventType::EngineResumed { timestamp: 2_000 }));
    assert!(!projection.is_engine_paused());
}

#[test]
fn p6_013_t_n1_engine_pause_rejected_when_already_paused() {
    let projection = InMemoryProjection {
        latest_sequence: 9,
        engine_paused: true,
        ..InMemoryProjection::default()
    };

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::EnginePause(EnginePauseCommand::new(10, 1_000)),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(
        result,
        Err(MutationAuthorityError::Validation(MutationValidationError::EngineAlreadyPaused))
    ));

    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert!(projection.is_engine_paused());
    assert_eq!(projection.latest_sequence, 9);
}

#[test]
fn p6_013_t_n2_engine_resume_rejected_when_not_paused() {
    let projection = InMemoryProjection {
        latest_sequence: 4,
        engine_paused: false,
        ..InMemoryProjection::default()
    };

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::EngineResume(EngineResumeCommand::new(5, 1_000)),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(
        result,
        Err(MutationAuthorityError::Validation(MutationValidationError::EngineNotPaused))
    ));

    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert!(!projection.is_engine_paused());
    assert_eq!(projection.latest_sequence, 4);
}

// ---------------------------------------------------------------------------
// Sprint 3: Budget and subscription mutation authority tests
// ---------------------------------------------------------------------------

#[test]
fn d04_sprint3_p1_budget_allocate_valid_flow() {
    let task_id = TaskId::new();

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.latest_sequence = 1;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let outcome = authority
        .submit_command(
            MutationCommand::BudgetAllocate(BudgetAllocateCommand::new(
                2,
                task_id,
                BudgetDimension::Token,
                1_000,
                5_000,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("budget allocate should succeed");

    assert_eq!(outcome.sequence(), 2);
    assert!(matches!(
        outcome.applied(),
        AppliedMutation::BudgetAllocate { task_id: tid, dimension, limit }
            if *tid == task_id
                && *dimension == BudgetDimension::Token
                && *limit == 1_000
    ));

    let (writer, projection) = authority.into_parts();
    assert_eq!(writer.events.len(), 1);
    assert!(matches!(
        writer.events[0].event(),
        WalEventType::BudgetAllocated { task_id: tid, dimension, limit, timestamp }
            if *tid == task_id
                && *dimension == BudgetDimension::Token
                && *limit == 1_000
                && *timestamp == 5_000
    ));
    assert!(projection.budget_allocation_exists(task_id, BudgetDimension::Token));
    assert_eq!(projection.latest_sequence, 2);
}

#[test]
fn d04_sprint3_p2_budget_consume_valid_flow() {
    let task_id = TaskId::new();

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.budget_allocations.insert((task_id, BudgetDimension::Token));
    projection.latest_sequence = 2;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let outcome = authority
        .submit_command(
            MutationCommand::BudgetConsume(BudgetConsumeCommand::new(
                3,
                task_id,
                BudgetDimension::Token,
                250,
                6_000,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("budget consume should succeed");

    assert_eq!(outcome.sequence(), 3);
    assert!(matches!(
        outcome.applied(),
        AppliedMutation::BudgetConsume { task_id: tid, dimension, amount }
            if *tid == task_id
                && *dimension == BudgetDimension::Token
                && *amount == 250
    ));

    let (writer, projection) = authority.into_parts();
    assert_eq!(writer.events.len(), 1);
    assert!(matches!(
        writer.events[0].event(),
        WalEventType::BudgetConsumed { task_id: tid, dimension, amount, timestamp }
            if *tid == task_id
                && *dimension == BudgetDimension::Token
                && *amount == 250
                && *timestamp == 6_000
    ));
    assert_eq!(projection.latest_sequence, 3);
}

#[test]
fn d04_sprint3_p3_budget_replenish_valid_flow() {
    let task_id = TaskId::new();

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.budget_allocations.insert((task_id, BudgetDimension::CostCents));
    projection.latest_sequence = 3;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let outcome = authority
        .submit_command(
            MutationCommand::BudgetReplenish(BudgetReplenishCommand::new(
                4,
                task_id,
                BudgetDimension::CostCents,
                5_000,
                7_000,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("budget replenish should succeed");

    assert_eq!(outcome.sequence(), 4);
    assert!(matches!(
        outcome.applied(),
        AppliedMutation::BudgetReplenish { task_id: tid, dimension, new_limit }
            if *tid == task_id
                && *dimension == BudgetDimension::CostCents
                && *new_limit == 5_000
    ));

    let (writer, projection) = authority.into_parts();
    assert_eq!(writer.events.len(), 1);
    assert!(matches!(
        writer.events[0].event(),
        WalEventType::BudgetReplenished { task_id: tid, dimension, new_limit, timestamp }
            if *tid == task_id
                && *dimension == BudgetDimension::CostCents
                && *new_limit == 5_000
                && *timestamp == 7_000
    ));
    assert_eq!(projection.latest_sequence, 4);
}

#[test]
fn d04_sprint3_p4_run_suspend_valid_flow() {
    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 100, 100);

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.runs.insert(run.id(), RunState::Running);
    projection.latest_sequence = 5;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let outcome = authority
        .submit_command(
            MutationCommand::RunSuspend(RunSuspendCommand::new(
                6,
                run.id(),
                Some("budget exhausted".to_string()),
                8_000,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("run suspend should succeed");

    assert_eq!(outcome.sequence(), 6);
    assert!(matches!(
        outcome.applied(),
        AppliedMutation::RunSuspend { run_id } if *run_id == run.id()
    ));

    let (writer, projection) = authority.into_parts();
    assert_eq!(writer.events.len(), 1);
    assert!(matches!(
        writer.events[0].event(),
        WalEventType::RunSuspended { run_id, reason, timestamp }
            if *run_id == run.id()
                && *reason == Some("budget exhausted".to_string())
                && *timestamp == 8_000
    ));
    assert_eq!(projection.run_state(&run.id()), Some(RunState::Suspended));
    assert_eq!(projection.latest_sequence, 6);
}

#[test]
fn d04_sprint3_p5_run_resume_valid_flow() {
    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 100, 100);

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.runs.insert(run.id(), RunState::Suspended);
    projection.latest_sequence = 6;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let outcome = authority
        .submit_command(
            MutationCommand::RunResume(RunResumeCommand::new(7, run.id(), 9_000)),
            DurabilityPolicy::Immediate,
        )
        .expect("run resume should succeed");

    assert_eq!(outcome.sequence(), 7);
    assert!(matches!(
        outcome.applied(),
        AppliedMutation::RunResume { run_id } if *run_id == run.id()
    ));

    let (writer, projection) = authority.into_parts();
    assert_eq!(writer.events.len(), 1);
    assert!(matches!(
        writer.events[0].event(),
        WalEventType::RunResumed { run_id, timestamp }
            if *run_id == run.id() && *timestamp == 9_000
    ));
    assert_eq!(projection.run_state(&run.id()), Some(RunState::Ready));
    assert_eq!(projection.latest_sequence, 7);
}

#[test]
fn d04_sprint3_p6_subscription_create_valid_flow() {
    let task_id = TaskId::new();
    let sub_id = SubscriptionId::new();

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.latest_sequence = 1;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let outcome = authority
        .submit_command(
            MutationCommand::SubscriptionCreate(SubscriptionCreateCommand::new(
                2,
                sub_id,
                task_id,
                EventFilter::TaskCompleted { task_id: TaskId::new() },
                10_000,
            )),
            DurabilityPolicy::Immediate,
        )
        .expect("subscription create should succeed");

    assert_eq!(outcome.sequence(), 2);
    assert!(matches!(
        outcome.applied(),
        AppliedMutation::SubscriptionCreate { subscription_id, task_id: tid }
            if *subscription_id == sub_id && *tid == task_id
    ));

    let (writer, projection) = authority.into_parts();
    assert_eq!(writer.events.len(), 1);
    assert!(matches!(
        writer.events[0].event(),
        WalEventType::SubscriptionCreated { subscription_id, task_id: tid, .. }
            if *subscription_id == sub_id && *tid == task_id
    ));
    assert!(projection.subscription_exists(sub_id));
    assert!(!projection.is_subscription_canceled(sub_id));
    assert_eq!(projection.latest_sequence, 2);
}

#[test]
fn d04_sprint3_p7_subscription_cancel_valid_flow() {
    let task_id = TaskId::new();
    let sub_id = SubscriptionId::new();

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.subscriptions.insert(sub_id, false);
    projection.latest_sequence = 3;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let outcome = authority
        .submit_command(
            MutationCommand::SubscriptionCancel(SubscriptionCancelCommand::new(4, sub_id, 11_000)),
            DurabilityPolicy::Immediate,
        )
        .expect("subscription cancel should succeed");

    assert_eq!(outcome.sequence(), 4);
    assert!(matches!(
        outcome.applied(),
        AppliedMutation::SubscriptionCancel { subscription_id }
            if *subscription_id == sub_id
    ));

    let (writer, projection) = authority.into_parts();
    assert_eq!(writer.events.len(), 1);
    assert!(matches!(
        writer.events[0].event(),
        WalEventType::SubscriptionCanceled { subscription_id, timestamp }
            if *subscription_id == sub_id && *timestamp == 11_000
    ));
    assert!(projection.is_subscription_canceled(sub_id));
    assert_eq!(projection.latest_sequence, 4);
}

// ---------------------------------------------------------------------------
// Sprint 3: Negative (validation failure) tests
// ---------------------------------------------------------------------------

#[test]
fn d04_sprint3_n1_budget_allocate_unknown_task_rejected() {
    let missing_task_id = TaskId::new();

    let writer = RecordingWriter::default();
    let projection = InMemoryProjection::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::BudgetAllocate(BudgetAllocateCommand::new(
            1,
            missing_task_id,
            BudgetDimension::Token,
            500,
            1_000,
        )),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(
        result,
        Err(MutationAuthorityError::Validation(MutationValidationError::UnknownTask { task_id }))
            if task_id == missing_task_id
    ));

    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert_eq!(projection.latest_sequence, 0);
}

#[test]
fn d04_sprint3_n2_budget_consume_unknown_task_rejected() {
    let missing_task_id = TaskId::new();

    let writer = RecordingWriter::default();
    let projection = InMemoryProjection::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::BudgetConsume(BudgetConsumeCommand::new(
            1,
            missing_task_id,
            BudgetDimension::CostCents,
            100,
            2_000,
        )),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(
        result,
        Err(MutationAuthorityError::Validation(MutationValidationError::UnknownTask { task_id }))
            if task_id == missing_task_id
    ));

    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert_eq!(projection.latest_sequence, 0);
}

#[test]
fn d04_sprint3_n3_budget_replenish_not_allocated_rejected() {
    let task_id = TaskId::new();

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.latest_sequence = 1;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::BudgetReplenish(BudgetReplenishCommand::new(
            2,
            task_id,
            BudgetDimension::TimeSecs,
            2_000,
            3_000,
        )),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(
        result,
        Err(MutationAuthorityError::Validation(
            MutationValidationError::BudgetNotAllocated { task_id: tid, dimension }
        )) if tid == task_id && dimension == BudgetDimension::TimeSecs
    ));

    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert_eq!(projection.latest_sequence, 1);
}

#[test]
fn d04_sprint3_n4_run_suspend_wrong_state_rejected() {
    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 100, 100);

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.runs.insert(run.id(), RunState::Ready);
    projection.latest_sequence = 3;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::RunSuspend(RunSuspendCommand::new(4, run.id(), None, 4_000)),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(
        result,
        Err(MutationAuthorityError::Validation(
            MutationValidationError::RunSuspendRequiresRunning { run_id, state }
        )) if run_id == run.id() && state == RunState::Ready
    ));

    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert_eq!(projection.run_state(&run.id()), Some(RunState::Ready));
    assert_eq!(projection.latest_sequence, 3);
}

#[test]
fn d04_sprint3_n5_run_resume_wrong_state_rejected() {
    let task_id = TaskId::new();
    let run = scheduled_run(task_id, 100, 100);

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.runs.insert(run.id(), RunState::Ready);
    projection.latest_sequence = 3;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::RunResume(RunResumeCommand::new(4, run.id(), 5_000)),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(
        result,
        Err(MutationAuthorityError::Validation(
            MutationValidationError::RunResumeRequiresSuspended { run_id, state }
        )) if run_id == run.id() && state == RunState::Ready
    ));

    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert_eq!(projection.run_state(&run.id()), Some(RunState::Ready));
    assert_eq!(projection.latest_sequence, 3);
}

#[test]
fn d04_sprint3_n6_subscription_create_duplicate_rejected() {
    let task_id = TaskId::new();
    let sub_id = SubscriptionId::new();

    let mut projection = InMemoryProjection::default();
    projection.tasks.insert(task_id);
    projection.subscriptions.insert(sub_id, false);
    projection.latest_sequence = 2;

    let writer = RecordingWriter::default();
    let mut authority = StorageMutationAuthority::new(writer, projection);

    let result = authority.submit_command(
        MutationCommand::SubscriptionCreate(SubscriptionCreateCommand::new(
            3,
            sub_id,
            task_id,
            EventFilter::Custom { key: "my-event".to_string() },
            6_000,
        )),
        DurabilityPolicy::Immediate,
    );

    assert!(matches!(
        result,
        Err(MutationAuthorityError::Validation(
            MutationValidationError::SubscriptionAlreadyExists { subscription_id }
        )) if subscription_id == sub_id
    ));

    let (writer, projection) = authority.into_parts();
    assert!(writer.events.is_empty());
    assert_eq!(projection.latest_sequence, 2);
}
