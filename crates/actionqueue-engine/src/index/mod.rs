//! Indexing utilities for run instances by state.
//!
//! This module provides index types for grouping and filtering run instances
//! by their lifecycle state.
//!
//! The indexes support the scheduling workflow:
//! - Scheduled: runs waiting to become ready
//! - Ready: runs eligible to be leased
//! - Running: runs currently being executed
//! - Terminal: completed, failed, or canceled runs

pub mod ready;
pub mod running;
pub mod scheduled;
pub mod terminal;

#[cfg(test)]
pub(crate) mod test_util {
    //! Shared test helpers for index module tests.

    use actionqueue_core::ids::TaskId;
    use actionqueue_core::run::run_instance::RunInstance;
    use actionqueue_core::run::state::RunState;

    /// Builds a `RunInstance` in the requested state by driving it through the
    /// required state transitions. Used by all four index test suites.
    #[allow(clippy::too_many_arguments)] // Test helper with naturally many parameters
    pub fn build_run(
        task_id: TaskId,
        state: RunState,
        scheduled_at: u64,
        created_at: u64,
        effective_priority: i32,
        active_attempt: Option<actionqueue_core::ids::AttemptId>,
    ) -> RunInstance {
        let mut run = RunInstance::new_scheduled(task_id, scheduled_at, created_at)
            .expect("test helper run construction should be valid");
        if state == RunState::Ready {
            run.promote_to_ready_with_priority(effective_priority).expect("promote to ready");
        } else if state == RunState::Leased {
            run.promote_to_ready_with_priority(effective_priority).expect("promote to ready");
            run.transition_to(RunState::Leased).expect("ready -> leased");
        } else if state == RunState::Running {
            run.promote_to_ready_with_priority(effective_priority).expect("promote to ready");
            run.transition_to(RunState::Leased).expect("ready -> leased");
            run.transition_to(RunState::Running).expect("leased -> running");
        } else if state == RunState::Completed {
            run.promote_to_ready_with_priority(effective_priority).expect("promote to ready");
            run.transition_to(RunState::Leased).expect("ready -> leased");
            run.transition_to(RunState::Running).expect("leased -> running");
            let aid = active_attempt.unwrap_or_default();
            run.start_attempt(aid).expect("start attempt");
            run.finish_attempt(aid).expect("finish attempt");
            run.transition_to(RunState::Completed).expect("running -> completed");
        } else if state == RunState::Failed {
            run.promote_to_ready_with_priority(effective_priority).expect("promote to ready");
            run.transition_to(RunState::Leased).expect("ready -> leased");
            run.transition_to(RunState::Running).expect("leased -> running");
            let aid = active_attempt.unwrap_or_default();
            run.start_attempt(aid).expect("start attempt");
            run.finish_attempt(aid).expect("finish attempt");
            run.transition_to(RunState::Failed).expect("running -> failed");
        } else if state == RunState::Canceled {
            run.transition_to(RunState::Canceled).expect("cancel");
        }

        if let Some(aid) = active_attempt {
            if run.state() == RunState::Leased {
                run.transition_to(RunState::Running).expect("leased -> running for attempt");
                run.start_attempt(aid).expect("start attempt");
                run.transition_to(RunState::Leased).expect("running -> leased reset");
            } else if run.state() == RunState::Running && run.current_attempt_id().is_none() {
                run.start_attempt(aid).expect("start active attempt");
            }
        }

        run
    }
}
