//! Attempt-finish mutation command derivation from executor outcomes.
//!
//! This module defines the engine-side integration seam that maps executor-local
//! terminal responses into canonical durable attempt result taxonomy used by
//! storage mutation authority payloads.

use actionqueue_core::ids::{AttemptId, RunId};
use actionqueue_core::mutation::{
    AttemptFinishCommand, AttemptOutcome, DurabilityPolicy, MutationAuthority, MutationCommand,
    MutationOutcome,
};
use actionqueue_executor_local::ExecutorResponse;

/// Maps an executor terminal response into a canonical attempt outcome.
///
/// For successful responses, the handler's opaque output bytes (if any) are
/// threaded through to the `AttemptOutcome` and from there into the WAL and
/// projection, making them queryable from the run's attempt history.
pub fn map_executor_response_to_outcome(response: &ExecutorResponse) -> AttemptOutcome {
    match response {
        ExecutorResponse::Success { output } => match output {
            Some(bytes) => AttemptOutcome::success_with_output(bytes.clone()),
            None => AttemptOutcome::success(),
        },
        ExecutorResponse::RetryableFailure { error }
        | ExecutorResponse::TerminalFailure { error } => AttemptOutcome::failure(error),
        ExecutorResponse::Timeout { timeout_secs } => {
            AttemptOutcome::timeout(format!("attempt timed out after {timeout_secs}s"))
        }
        ExecutorResponse::Suspended { output } => match output {
            Some(bytes) => AttemptOutcome::suspended_with_output(bytes.clone()),
            None => AttemptOutcome::suspended(),
        },
    }
}

/// Builds an attempt-finish command from executor response truth.
pub fn build_attempt_finish_command(
    sequence: u64,
    run_id: RunId,
    attempt_id: AttemptId,
    response: &ExecutorResponse,
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

#[cfg(test)]
mod tests {
    use actionqueue_core::ids::{AttemptId, RunId};
    use actionqueue_core::mutation::{
        AppliedMutation, AttemptResultKind, DurabilityPolicy, MutationAuthority, MutationCommand,
        MutationOutcome,
    };
    use actionqueue_executor_local::ExecutorResponse;

    use super::{
        build_attempt_finish_command, map_executor_response_to_outcome,
        submit_attempt_finish_via_authority,
    };

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
            self.submitted.push(command.clone());
            match command {
                MutationCommand::AttemptFinish(details) => Ok(MutationOutcome::new(
                    details.sequence(),
                    AppliedMutation::AttemptFinish {
                        run_id: details.run_id(),
                        attempt_id: details.attempt_id(),
                        outcome: details.outcome().clone(),
                    },
                )),
                _ => Err("unexpected command"),
            }
        }
    }

    #[test]
    fn timeout_response_maps_to_timeout_result_kind() {
        let response = ExecutorResponse::Timeout { timeout_secs: 5 };
        assert_eq!(
            map_executor_response_to_outcome(&response).result(),
            AttemptResultKind::Timeout
        );
    }

    #[test]
    fn failure_responses_map_to_failure_result_kind() {
        let retryable = ExecutorResponse::RetryableFailure { error: "retryable".to_string() };
        let terminal = ExecutorResponse::TerminalFailure { error: "terminal".to_string() };

        assert_eq!(
            map_executor_response_to_outcome(&retryable).result(),
            AttemptResultKind::Failure
        );
        assert_eq!(
            map_executor_response_to_outcome(&terminal).result(),
            AttemptResultKind::Failure
        );
    }

    #[test]
    fn success_response_maps_to_success_result_kind() {
        let response = ExecutorResponse::Success { output: Some(vec![1, 2, 3]) };
        assert_eq!(
            map_executor_response_to_outcome(&response).result(),
            AttemptResultKind::Success
        );
    }

    #[test]
    fn build_attempt_finish_command_populates_result_and_error_from_response() {
        let run_id = RunId::new();
        let attempt_id = AttemptId::new();
        let response = ExecutorResponse::Timeout { timeout_secs: 9 };

        let command = build_attempt_finish_command(11, run_id, attempt_id, &response, 1_234);

        assert_eq!(command.sequence(), 11);
        assert_eq!(command.run_id(), run_id);
        assert_eq!(command.attempt_id(), attempt_id);
        assert_eq!(command.result(), AttemptResultKind::Timeout);
        assert_eq!(command.error(), Some("attempt timed out after 9s"));
        assert_eq!(command.timestamp(), 1_234);
    }

    #[test]
    fn submit_attempt_finish_via_authority_maps_timeout_and_submits_canonical_command() {
        let run_id = RunId::new();
        let attempt_id = AttemptId::new();
        let response = ExecutorResponse::Timeout { timeout_secs: 5 };
        let mut authority = MockAuthority::default();

        let finish_cmd = build_attempt_finish_command(17, run_id, attempt_id, &response, 1_700);
        let outcome = submit_attempt_finish_via_authority(
            finish_cmd,
            DurabilityPolicy::Immediate,
            &mut authority,
        )
        .expect("authority submission should succeed");

        assert_eq!(outcome.sequence(), 17);
        assert!(matches!(
            outcome.applied(),
            AppliedMutation::AttemptFinish {
                run_id: applied_run_id,
                attempt_id: applied_attempt_id,
                outcome: ref o,
            } if *applied_run_id == run_id
                && *applied_attempt_id == attempt_id
                && o.result() == AttemptResultKind::Timeout
                && o.error() == Some("attempt timed out after 5s")
        ));

        assert_eq!(authority.submitted.len(), 1);
        assert!(matches!(
            &authority.submitted[0],
            MutationCommand::AttemptFinish(details)
                if details.sequence() == 17
                    && details.run_id() == run_id
                    && details.attempt_id() == attempt_id
                    && details.result() == AttemptResultKind::Timeout
                    && details.error() == Some("attempt timed out after 5s")
                    && details.timestamp() == 1_700
        ));
    }

    #[test]
    fn success_with_output_maps_to_outcome_with_output() {
        let response = ExecutorResponse::Success { output: Some(b"data".to_vec()) };
        let outcome = map_executor_response_to_outcome(&response);
        assert_eq!(outcome.result(), AttemptResultKind::Success);
        assert_eq!(outcome.output(), Some(b"data".as_slice()));
    }

    #[test]
    fn success_without_output_maps_to_outcome_without_output() {
        let response = ExecutorResponse::Success { output: None };
        let outcome = map_executor_response_to_outcome(&response);
        assert_eq!(outcome.result(), AttemptResultKind::Success);
        assert!(outcome.output().is_none());
    }

    #[test]
    fn failure_maps_to_outcome_without_output() {
        let response = ExecutorResponse::RetryableFailure { error: "err".to_string() };
        let outcome = map_executor_response_to_outcome(&response);
        assert_eq!(outcome.result(), AttemptResultKind::Failure);
        assert!(outcome.output().is_none());
    }

    #[test]
    fn submit_attempt_finish_via_authority_maps_failure_to_failure_taxonomy() {
        let run_id = RunId::new();
        let attempt_id = AttemptId::new();
        let response =
            ExecutorResponse::RetryableFailure { error: "transient network failure".to_string() };
        let mut authority = MockAuthority::default();

        let finish_cmd = build_attempt_finish_command(21, run_id, attempt_id, &response, 2_100);
        let outcome = submit_attempt_finish_via_authority(
            finish_cmd,
            DurabilityPolicy::Immediate,
            &mut authority,
        )
        .expect("authority submission should succeed");

        assert!(matches!(
            outcome.applied(),
            AppliedMutation::AttemptFinish {
                run_id: applied_run_id,
                attempt_id: applied_attempt_id,
                outcome: ref o,
            } if *applied_run_id == run_id
                && *applied_attempt_id == attempt_id
                && o.result() == AttemptResultKind::Failure
                && o.error() == Some("transient network failure")
        ));
    }
}
