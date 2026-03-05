//! Scheduled-to-ready promotion logic.
//!
//! This module provides functionality for promoting runs from the Scheduled
//! state to the Ready state when their scheduled_at time has passed.

use actionqueue_core::mutation::{
    DurabilityPolicy, MutationAuthority, MutationCommand, MutationOutcome,
    RunStateTransitionCommand,
};
use actionqueue_core::run::run_instance::{RunInstance, RunInstanceError};
use actionqueue_core::run::state::RunState;

use crate::index::scheduled::ScheduledIndex;

/// Result of promoting scheduled runs to ready.
///
/// This structure contains the runs that were promoted from Scheduled to Ready
/// and the remaining runs that are still in Scheduled state ( waiting for their
/// scheduled_at time to pass).
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct PromotionResult {
    /// Runs that were promoted from Scheduled to Ready.
    promoted: Vec<RunInstance>,
    /// Runs that remain in Scheduled state.
    remaining_scheduled: Vec<RunInstance>,
}

impl PromotionResult {
    /// Returns the runs that were promoted from Scheduled to Ready.
    pub fn promoted(&self) -> &[RunInstance] {
        &self.promoted
    }

    /// Returns the runs that remain in Scheduled state.
    pub fn remaining_scheduled(&self) -> &[RunInstance] {
        &self.remaining_scheduled
    }
}

/// Result of authority-mediated scheduled-to-ready promotion.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct AuthorityPromotionResult {
    /// Mutation outcomes returned by the storage-owned authority.
    outcomes: Vec<MutationOutcome>,
    /// Runs that remain in Scheduled state.
    remaining_scheduled: Vec<RunInstance>,
}

impl AuthorityPromotionResult {
    /// Returns the mutation outcomes from promotion.
    pub fn outcomes(&self) -> &[MutationOutcome] {
        &self.outcomes
    }

    /// Returns the runs that remain in Scheduled state.
    pub fn remaining_scheduled(&self) -> &[RunInstance] {
        &self.remaining_scheduled
    }
}

/// Error returned when authority-mediated promotion fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityPromotionError<AuthorityError> {
    /// Promotion command sequencing overflowed `u64` while preparing commands.
    SequenceOverflow,
    /// Authority rejected or failed processing a specific run promotion command.
    Authority {
        /// Run whose promotion command failed.
        run_id: actionqueue_core::ids::RunId,
        /// Underlying authority error.
        source: AuthorityError,
    },
}

impl<E: std::fmt::Display> std::fmt::Display for AuthorityPromotionError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthorityPromotionError::SequenceOverflow => {
                write!(f, "promotion command sequencing overflowed u64")
            }
            AuthorityPromotionError::Authority { run_id, source } => {
                write!(f, "authority error for run {run_id}: {source}")
            }
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for AuthorityPromotionError<E> {}

/// Parameters for authority-mediated promotion that group sequencing and timing context.
pub struct PromotionParams {
    /// Current time used to determine which runs are eligible for promotion.
    current_time: u64,
    /// First WAL sequence to assign to promotion commands.
    first_sequence: u64,
    /// Timestamp carried into durable event payloads.
    event_timestamp: u64,
    /// Durability behavior requested for promotion commands.
    durability: DurabilityPolicy,
}

impl PromotionParams {
    /// Creates new promotion parameters.
    pub fn new(
        current_time: u64,
        first_sequence: u64,
        event_timestamp: u64,
        durability: DurabilityPolicy,
    ) -> Self {
        Self { current_time, first_sequence, event_timestamp, durability }
    }

    /// Returns the current time used for promotion eligibility.
    pub fn current_time(&self) -> u64 {
        self.current_time
    }

    /// Returns the first WAL sequence to assign.
    pub fn first_sequence(&self) -> u64 {
        self.first_sequence
    }

    /// Returns the event timestamp.
    pub fn event_timestamp(&self) -> u64 {
        self.event_timestamp
    }

    /// Returns the durability policy.
    pub fn durability(&self) -> DurabilityPolicy {
        self.durability
    }
}

/// Promotes scheduled runs through the engine-storage mutation authority boundary.
///
/// Unlike [`promote_scheduled_to_ready`], this function is durability-aware and
/// emits semantic commands through a [`MutationAuthority`] implementation.
pub fn promote_scheduled_to_ready_via_authority<A: MutationAuthority>(
    scheduled: &ScheduledIndex,
    params: PromotionParams,
    authority: &mut A,
) -> Result<AuthorityPromotionResult, AuthorityPromotionError<A::Error>> {
    let PromotionParams { current_time, first_sequence, event_timestamp, durability } = params;
    let runs = scheduled.runs();
    let (ready_for_promotion, still_waiting): (Vec<&RunInstance>, Vec<&RunInstance>) =
        runs.iter().partition(|run| run.scheduled_at() <= current_time);

    let mut outcomes = Vec::with_capacity(ready_for_promotion.len());
    for (index, run) in ready_for_promotion.into_iter().enumerate() {
        let offset = u64::try_from(index).map_err(|_| AuthorityPromotionError::SequenceOverflow)?;
        let sequence =
            first_sequence.checked_add(offset).ok_or(AuthorityPromotionError::SequenceOverflow)?;

        let command = MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
            sequence,
            run.id(),
            RunState::Scheduled,
            RunState::Ready,
            event_timestamp,
        ));

        let outcome = authority
            .submit_command(command, durability)
            .map_err(|source| AuthorityPromotionError::Authority { run_id: run.id(), source })?;
        outcomes.push(outcome);
    }

    let remaining_scheduled: Vec<RunInstance> = still_waiting.into_iter().cloned().collect();
    Ok(AuthorityPromotionResult { outcomes, remaining_scheduled })
}

/// Promotes runs from Scheduled to Ready based on the current time.
///
/// This function takes the scheduled index and the current time, then moves
/// runs that are ready for promotion (scheduled_at <= current_time) to the
/// Ready state.
///
/// # Arguments
///
/// * `scheduled` - The scheduled index containing runs in Scheduled state
/// * `current_time` - The current time according to the scheduler clock
///
/// # Returns
///
/// A PromotionResult containing:
/// - `promoted`: Runs that transitioned from Scheduled to Ready
/// - `remaining_scheduled`: Runs that are still waiting for their scheduled_at time
pub fn promote_scheduled_to_ready(
    scheduled: &ScheduledIndex,
    current_time: u64,
) -> Result<PromotionResult, RunInstanceError> {
    let runs = scheduled.runs();

    // Partition runs into those ready for promotion and those still waiting
    let (ready_for_promotion, still_waiting): (Vec<&RunInstance>, Vec<&RunInstance>) =
        runs.iter().partition(|run| run.scheduled_at() <= current_time);

    // Convert ready runs to Ready state
    let mut promoted = Vec::with_capacity(ready_for_promotion.len());
    for run in ready_for_promotion {
        let mut ready_run = run.clone();
        ready_run.promote_to_ready()?;
        promoted.push(ready_run);
    }

    // Convert still waiting runs back to Scheduled state (they should already be Scheduled)
    let remaining_scheduled: Vec<RunInstance> = still_waiting.into_iter().cloned().collect();

    Ok(PromotionResult { promoted, remaining_scheduled })
}

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::TaskId;
    use actionqueue_core::mutation::AppliedMutation;
    use actionqueue_core::run::state::RunState;

    use super::*;

    #[derive(Debug, Default)]
    struct MockAuthority {
        submitted: Vec<MutationCommand>,
    }

    impl MutationAuthority for MockAuthority {
        type Error = &'static str;

        fn submit_command(
            &mut self,
            command: MutationCommand,
            _durability: DurabilityPolicy,
        ) -> Result<MutationOutcome, Self::Error> {
            let (sequence, run_id) = match &command {
                MutationCommand::RunStateTransition(details) => {
                    (details.sequence(), details.run_id())
                }
                MutationCommand::TaskCreate(_)
                | MutationCommand::RunCreate(_)
                | MutationCommand::AttemptStart(_)
                | MutationCommand::AttemptFinish(_)
                | MutationCommand::LeaseAcquire(_)
                | MutationCommand::LeaseHeartbeat(_)
                | MutationCommand::LeaseExpire(_)
                | MutationCommand::LeaseRelease(_)
                | MutationCommand::EnginePause(_)
                | MutationCommand::EngineResume(_)
                | MutationCommand::TaskCancel(_)
                | MutationCommand::DependencyDeclare(_)
                | MutationCommand::RunSuspend(_)
                | MutationCommand::RunResume(_)
                | MutationCommand::BudgetAllocate(_)
                | MutationCommand::BudgetConsume(_)
                | MutationCommand::BudgetReplenish(_)
                | MutationCommand::SubscriptionCreate(_)
                | MutationCommand::SubscriptionCancel(_)
                | MutationCommand::SubscriptionTrigger(_)
                | MutationCommand::ActorRegister(_)
                | MutationCommand::ActorDeregister(_)
                | MutationCommand::ActorHeartbeat(_)
                | MutationCommand::TenantCreate(_)
                | MutationCommand::RoleAssign(_)
                | MutationCommand::CapabilityGrant(_)
                | MutationCommand::CapabilityRevoke(_)
                | MutationCommand::LedgerAppend(_) => {
                    return Err("unexpected command in promotion authority test");
                }
            };
            self.submitted.push(command.clone());
            Ok(MutationOutcome::new(
                sequence,
                AppliedMutation::RunStateTransition {
                    run_id,
                    previous_state: RunState::Scheduled,
                    new_state: RunState::Ready,
                },
            ))
        }
    }

    #[test]
    fn promotes_runs_with_past_scheduled_at() {
        let now = 1000;
        let task_id = TaskId::new();

        let scheduled_runs = vec![
            RunInstance::new_scheduled(task_id, 900, now).expect("valid scheduled run"), /* past, should be promoted */
            RunInstance::new_scheduled(task_id, 950, now).expect("valid scheduled run"), /* past, should be promoted */
        ];

        let scheduled_index = ScheduledIndex::from_runs(scheduled_runs);

        let result = promote_scheduled_to_ready(&scheduled_index, 1000)
            .expect("promotion should succeed for valid scheduled runs");

        assert_eq!(result.promoted().len(), 2);
        assert!(result.promoted().iter().all(|run| run.state() == RunState::Ready));
        assert!(result.remaining_scheduled().is_empty());
    }

    #[test]
    fn does_not_promote_runs_with_future_scheduled_at() {
        let now = 1000;
        let task_id = TaskId::new();

        let scheduled_runs = vec![
            RunInstance::new_scheduled(task_id, 1100, now).expect("valid scheduled run"), /* future, should not be promoted */
            RunInstance::new_scheduled(task_id, 1200, now).expect("valid scheduled run"), /* future, should not be promoted */
        ];

        let scheduled_index = ScheduledIndex::from_runs(scheduled_runs);

        let result = promote_scheduled_to_ready(&scheduled_index, 1000)
            .expect("promotion should succeed for valid scheduled runs");

        assert!(result.promoted().is_empty());
        assert_eq!(result.remaining_scheduled().len(), 2);
        assert!(result.remaining_scheduled().iter().all(|run| run.state() == RunState::Scheduled));
    }

    #[test]
    fn promotes_runs_with_equal_scheduled_at() {
        let now = 1000;
        let task_id = TaskId::new();

        let scheduled_runs = vec![
            RunInstance::new_scheduled(task_id, 1000, now).expect("valid scheduled run"), /* equal, should be promoted */
        ];

        let scheduled_index = ScheduledIndex::from_runs(scheduled_runs);

        let result = promote_scheduled_to_ready(&scheduled_index, 1000)
            .expect("promotion should succeed for valid scheduled runs");

        assert_eq!(result.promoted().len(), 1);
        assert_eq!(result.promoted()[0].state(), RunState::Ready);
    }

    #[test]
    fn mixed_promotion_of_past_and_future_scheduled_runs() {
        let now = 1000;
        let task_id = TaskId::new();

        let scheduled_runs = vec![
            RunInstance::new_scheduled(task_id, 900, now).expect("valid scheduled run"), /* past, should be promoted */
            RunInstance::new_scheduled(task_id, 1100, now).expect("valid scheduled run"), /* future, should not be promoted */
            RunInstance::new_scheduled(task_id, 950, now).expect("valid scheduled run"), /* past, should be promoted */
            RunInstance::new_scheduled(task_id, 1050, now).expect("valid scheduled run"), /* future, should not be promoted */
        ];

        let scheduled_index = ScheduledIndex::from_runs(scheduled_runs);

        let result = promote_scheduled_to_ready(&scheduled_index, 1000)
            .expect("promotion should succeed for valid scheduled runs");

        assert_eq!(result.promoted().len(), 2);
        assert_eq!(result.remaining_scheduled().len(), 2);

        // Check that promoted runs have past scheduled_at times
        assert!(result.promoted().iter().all(|run| run.scheduled_at() <= 1000));

        // Check that remaining scheduled runs have future scheduled_at times
        assert!(result.remaining_scheduled().iter().all(|run| run.scheduled_at() > 1000));
    }

    #[test]
    fn preserves_run_data_during_promotion() {
        let now = 1000;
        let task_id = TaskId::new();

        let scheduled_runs =
            vec![RunInstance::new_scheduled(task_id, 900, now).expect("valid scheduled run")];

        let scheduled_index = ScheduledIndex::from_runs(scheduled_runs.clone());

        let result = promote_scheduled_to_ready(&scheduled_index, 1000)
            .expect("promotion should succeed for valid scheduled runs");

        assert_eq!(result.promoted().len(), 1);

        let promoted_run = &result.promoted()[0];
        let original_run = &scheduled_runs[0];

        // Verify all fields except state are preserved
        assert_eq!(promoted_run.id(), original_run.id());
        assert_eq!(promoted_run.task_id(), original_run.task_id());
        assert_eq!(promoted_run.current_attempt_id(), original_run.current_attempt_id());
        assert_eq!(promoted_run.attempt_count(), original_run.attempt_count());
        assert_eq!(promoted_run.created_at(), original_run.created_at());
        assert_eq!(promoted_run.scheduled_at(), original_run.scheduled_at());
        assert_eq!(promoted_run.state(), RunState::Ready);
    }

    #[test]
    fn empty_index_returns_empty_results() {
        let scheduled_index = ScheduledIndex::new();

        let result = promote_scheduled_to_ready(&scheduled_index, 1000)
            .expect("promotion should succeed for valid scheduled runs");

        assert!(result.promoted().is_empty());
        assert!(result.remaining_scheduled().is_empty());
    }

    #[test]
    fn authority_promotion_emits_transition_commands_for_ready_runs() {
        let now = 1_000;
        let task_id = TaskId::new();
        let scheduled_runs = vec![
            RunInstance::new_scheduled(task_id, 900, now).expect("valid scheduled run"),
            RunInstance::new_scheduled(task_id, 1_100, now).expect("valid scheduled run"),
        ];
        let scheduled_index = ScheduledIndex::from_runs(scheduled_runs);
        let mut authority = MockAuthority::default();

        let result = promote_scheduled_to_ready_via_authority(
            &scheduled_index,
            PromotionParams::new(now, 7, now, DurabilityPolicy::Immediate),
            &mut authority,
        )
        .expect("authority promotion should succeed");

        assert_eq!(result.outcomes().len(), 1);
        assert_eq!(result.outcomes()[0].sequence(), 7);
        assert_eq!(result.remaining_scheduled().len(), 1);
        assert_eq!(authority.submitted.len(), 1);
        assert!(matches!(
            &authority.submitted[0],
            MutationCommand::RunStateTransition(cmd)
                if cmd.sequence() == 7
                && cmd.previous_state() == RunState::Scheduled
                && cmd.new_state() == RunState::Ready
        ));
    }

    #[test]
    fn authority_promotion_empty_scheduled_index_produces_no_outcomes() {
        let scheduled_index = ScheduledIndex::from_runs(Vec::new());
        let mut authority = MockAuthority::default();

        let result = promote_scheduled_to_ready_via_authority(
            &scheduled_index,
            PromotionParams::new(1000, 1, 1000, DurabilityPolicy::Immediate),
            &mut authority,
        )
        .expect("authority promotion of empty index should succeed");

        assert_eq!(result.outcomes().len(), 0);
        assert_eq!(result.remaining_scheduled().len(), 0);
        assert!(authority.submitted.is_empty());
    }

    #[test]
    fn authority_promotion_preserves_future_runs_in_remaining() {
        let now = 100;
        let task_id = TaskId::new();

        let scheduled_runs = vec![
            RunInstance::new_scheduled(task_id, 100, now).expect("valid scheduled run"), /* past/equal, should be promoted */
            RunInstance::new_scheduled(task_id, u64::MAX, now).expect("valid scheduled run"), /* far future, should remain */
        ];
        let scheduled_index = ScheduledIndex::from_runs(scheduled_runs);
        let mut authority = MockAuthority::default();

        let result = promote_scheduled_to_ready_via_authority(
            &scheduled_index,
            PromotionParams::new(200, 1, 200, DurabilityPolicy::Immediate),
            &mut authority,
        )
        .expect("authority promotion should succeed");

        assert_eq!(result.outcomes().len(), 1);
        assert_eq!(result.remaining_scheduled().len(), 1);
        assert_eq!(result.remaining_scheduled()[0].scheduled_at(), u64::MAX);
        assert_eq!(authority.submitted.len(), 1);
    }
}
