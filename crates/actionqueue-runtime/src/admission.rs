//! Handler-independent admission service shared by embedded, workflow, and CLI callers.
use actionqueue_core::{
    admission::{AdmissionRejection, EnsureTaskOutcome, EnsureTaskRequest},
    mutation::{
        AdmissionCommitCommand, AppliedMutation, DurabilityPolicy, MutationAuthority,
        MutationCommand,
    },
    time::clock::Clock,
};
use actionqueue_engine::admission::{plan_admission, AdmissionPlanningError};
use actionqueue_storage::{
    mutation::authority::{MutationAuthorityError, StorageMutationAuthority},
    recovery::reducer::{ReplayReducer, ReplayReducerError},
    wal::writer::WalWriter,
};
/// Admission rejection, derivation failure, or uncertain storage result.
#[derive(Debug)]
pub enum AdmissionError {
    /// Pure or store-dependent admission rejection (including typed conflict).
    Rejected(AdmissionRejection),
    /// Run policy could not be planned.
    Derivation(actionqueue_engine::derive::DerivationError),
    /// Storage failure; an uncertain write requires recovery before retrying.
    Storage(MutationAuthorityError<ReplayReducerError>),
}
impl std::fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(e) => write!(f, "{e}"),
            Self::Derivation(e) => write!(f, "{e}"),
            Self::Storage(e) => write!(f, "{e}"),
        }
    }
}
impl std::error::Error for AdmissionError {}
impl From<MutationAuthorityError<ReplayReducerError>> for AdmissionError {
    fn from(e: MutationAuthorityError<ReplayReducerError>) -> Self {
        match e {
            MutationAuthorityError::Admission(e) => Self::Rejected(e),
            e => Self::Storage(e),
        }
    }
}
/// Ensures one durable admission. A duplicate does not consult the clock, derive runs,
/// append, replace attribution, or apply creation-only configurable limits.
pub fn ensure_task<W: WalWriter>(
    authority: &mut StorageMutationAuthority<W, ReplayReducer>,
    request: EnsureTaskRequest,
    clock: &impl Clock,
) -> Result<EnsureTaskOutcome, AdmissionError> {
    if let Some(outcome) = authority.lookup_admission(&request)? {
        return Ok(outcome);
    }
    authority
        .admission_limits()
        .validate_spec(request.task_spec(), request.dependencies().len())
        .map_err(AdmissionError::Rejected)?;
    let digest = request.digest().map_err(AdmissionError::Rejected)?;
    let control = request.control_context().cloned();
    let timestamp = clock.now();
    let plan = plan_admission(request, digest, timestamp).map_err(|e| match e {
        AdmissionPlanningError::Rejected(e) => AdmissionError::Rejected(e),
        AdmissionPlanningError::Derivation(e) => AdmissionError::Derivation(e),
    })?;
    let sequence = authority.projection().latest_sequence().checked_add(1).ok_or_else(|| {
        AdmissionError::Storage(MutationAuthorityError::Validation(
            actionqueue_storage::mutation::authority::MutationValidationError::SequenceOverflow,
        ))
    })?;
    let result = authority.submit_command(
        MutationCommand::AdmissionCommit(AdmissionCommitCommand::new(
            sequence, plan, control, timestamp,
        )),
        DurabilityPolicy::Immediate,
    )?;
    match result.applied() {
        AppliedMutation::Admission(outcome) => Ok(outcome.clone()),
        _ => unreachable!("admission authority outcome"),
    }
}
