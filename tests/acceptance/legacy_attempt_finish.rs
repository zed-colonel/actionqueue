//! Test-only historical WAL fixture construction. Production handlers commit complete dispositions.
#![allow(dead_code)]
use actionqueue_core::ids::{AttemptId, RunId};
use actionqueue_core::mutation::{
    AttemptFinishCommand, AttemptOutcome, DurabilityPolicy, MutationAuthority, MutationCommand,
    MutationOutcome,
};
use actionqueue_executor_local::AttemptDisposition;

/// Maps an executor terminal response into a canonical attempt outcome.
///
/// For successful responses, the handler's opaque output bytes (if any) are
/// threaded through to the `AttemptOutcome` and from there into the WAL and
/// projection, making them queryable from the run's attempt history.
pub fn map_executor_response_to_outcome(response: &AttemptDisposition) -> AttemptOutcome {
    use actionqueue_core::disposition::DispositionOutcome as D;
    match response.outcome() {
        D::Complete => match response.output() {
            Some(actionqueue_core::data_ref::DataRef::Inline(v)) => {
                AttemptOutcome::success_with_output(v.bytes().to_vec())
            }
            _ => AttemptOutcome::success(),
        },
        D::RetryableFailure { error } | D::TerminalFailure { error } => {
            AttemptOutcome::failure(error.message.as_str())
        }
        D::Timeout { error } => AttemptOutcome::timeout(error.message.as_str()),
        D::Suspended { .. } => AttemptOutcome::suspended(),
        D::Awaiting => AttemptOutcome::awaiting(),
    }
}

/// Builds an attempt-finish command from executor response truth.
pub fn build_attempt_finish_command(
    sequence: u64,
    run_id: RunId,
    attempt_id: AttemptId,
    response: &AttemptDisposition,
    timestamp: u64,
) -> AttemptFinishCommand {
    AttemptFinishCommand::new(
        sequence,
        run_id,
        attempt_id,
        map_executor_response_to_outcome(response),
        timestamp,
    )
}

/// Error returned when authority-mediated attempt-finish submission fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptFinishSubmissionError<AuthorityError> {
    /// Storage authority rejected or failed processing attempt-finish command.
    Authority {
        /// Run whose attempt-finish submission failed.
        run_id: RunId,
        /// Underlying authority error.
        source: AuthorityError,
    },
}

impl<E: std::fmt::Display> std::fmt::Display for AttemptFinishSubmissionError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AttemptFinishSubmissionError::Authority { run_id, source } => {
                write!(f, "attempt-finish authority error for run {run_id}: {source}")
            }
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for AttemptFinishSubmissionError<E> {}

impl<E> AttemptFinishSubmissionError<E> {
    /// Extracts the inner authority error, discarding wrapper context.
    pub fn into_source(self) -> E {
        match self {
            Self::Authority { source, .. } => source,
        }
    }
}

/// Builds and submits an attempt-finish command through the mutation authority lane.
///
/// This is the engine-side integration seam that takes a pre-built attempt-finish
/// command and emits it as `MutationCommand::AttemptFinish` through storage-owned
/// authority.
pub fn submit_attempt_finish_via_authority<A: MutationAuthority>(
    finish_command: AttemptFinishCommand,
    durability: DurabilityPolicy,
    authority: &mut A,
) -> Result<MutationOutcome, AttemptFinishSubmissionError<A::Error>> {
    let run_id = finish_command.run_id();
    let command = MutationCommand::AttemptFinish(finish_command);

    authority
        .submit_command(command, durability)
        .map_err(|source| AttemptFinishSubmissionError::Authority { run_id, source })
}
