//! Validated dynamic task creation from handlers.
//!
//! Provides [`SubmissionChannel`] — a typed mpsc channel through which handlers
//! can propose new tasks. The dispatch loop owns all mutations: proposals flow
//! through the channel and are validated through the mutation authority before
//! being WAL-appended. Handlers cannot bypass the mutation authority.
//!
//! # Invariant preservation
//!
//! This design preserves ActionQueue Invariant 5: "External extensions cannot mutate
//! persisted state directly." Handlers propose via [`SubmissionChannel::submit`];
//! the dispatch loop validates and commits.
//!
//! # Submission semantics
//!
//! Submissions are fire-and-forget from the handler's perspective:
//! - If the channel is closed (dispatch loop shut down), the submission is
//!   silently dropped — handlers have no error path for this case.
//! - The dispatch loop processes submissions on the next tick, not immediately.
//! - Handlers must not assume submissions are visible before they return.

use std::sync::Arc;

use actionqueue_core::ids::TaskId;
use actionqueue_core::task::task_spec::TaskSpec;
use actionqueue_executor_local::handler::TaskSubmissionPort;
use tokio::sync::mpsc;

/// A proposed task submission from a handler.
///
/// Created by [`SubmissionChannel::submit`] and consumed by the dispatch loop.
/// Fields are private to preserve the validated-construction pattern used
/// throughout ActionQueue; use [`into_parts`][TaskSubmission::into_parts] to
/// destructure for processing.
#[derive(Debug)]
pub struct TaskSubmission {
    /// The task specification to create. Should have `parent_task_id` set
    /// (via [`TaskSpec::with_parent`]) to associate this child with the
    /// Coordinator's task in the hierarchy.
    task_spec: TaskSpec,
    /// Prerequisite task IDs that must reach terminal success before this
    /// task's runs are promoted to Ready. Empty means no dependencies.
    dependencies: Vec<TaskId>,
}

impl TaskSubmission {
    /// Creates a new task submission.
    pub fn new(task_spec: TaskSpec, dependencies: Vec<TaskId>) -> Self {
        Self { task_spec, dependencies }
    }

    /// Returns a reference to the task specification.
    pub fn task_spec(&self) -> &TaskSpec {
        &self.task_spec
    }

    /// Returns the dependency list.
    pub fn dependencies(&self) -> &[TaskId] {
        &self.dependencies
    }

    /// Consumes the submission, returning its parts.
    pub fn into_parts(self) -> (TaskSpec, Vec<TaskId>) {
        (self.task_spec, self.dependencies)
    }
}

/// Concrete submission channel backed by a tokio mpsc channel.
///
/// Created by the dispatch loop and cloned (cheaply via `Arc`) into each
/// `ExecutorContext`. Implements [`TaskSubmissionPort`] so it can be passed
/// as `Arc<dyn TaskSubmissionPort>` without coupling executor-local to tokio.
pub struct SubmissionChannel {
    tx: mpsc::UnboundedSender<TaskSubmission>,
}

impl TaskSubmissionPort for SubmissionChannel {
    fn submit(&self, task_spec: TaskSpec, dependencies: Vec<TaskId>) {
        let task_id = task_spec.id();
        if let Err(_e) = self.tx.send(TaskSubmission::new(task_spec, dependencies)) {
            tracing::warn!(
                %task_id,
                "submission dropped: dispatch loop receiver closed"
            );
        }
    }
}

/// Receiving end of the submission channel (held by the dispatch loop).
pub struct SubmissionReceiver(mpsc::UnboundedReceiver<TaskSubmission>);

impl SubmissionReceiver {
    /// Attempts to receive a pending submission without blocking.
    ///
    /// Returns `Some` if a submission is pending, `None` if the queue is
    /// empty or the sender side has disconnected.
    pub fn try_recv(&mut self) -> Option<TaskSubmission> {
        match self.0.try_recv() {
            Ok(submission) => Some(submission),
            Err(mpsc::error::TryRecvError::Empty) => None,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                tracing::debug!("submission channel disconnected");
                None
            }
        }
    }
}

/// Creates a matched pair of [`SubmissionChannel`] (handler side) and
/// [`SubmissionReceiver`] (dispatch loop side).
pub fn submission_channel() -> (Arc<SubmissionChannel>, SubmissionReceiver) {
    let (tx, rx) = mpsc::unbounded_channel();
    (Arc::new(SubmissionChannel { tx }), SubmissionReceiver(rx))
}
