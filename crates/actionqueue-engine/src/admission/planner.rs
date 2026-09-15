//! Initial run planning using one captured timestamp and the existing policy derivation.
use actionqueue_core::admission::{
    AdmissionDigest, AdmissionPlan, AdmissionRejection, EnsureTaskRequest,
};
/// Pure planning failures, before storage mutation.
#[derive(Debug)]
pub enum AdmissionPlanningError {
    /// A structural ceiling or invariant failed.
    Rejected(AdmissionRejection),
    /// Initial policy derivation failed.
    Derivation(crate::derive::DerivationError),
}
impl std::fmt::Display for AdmissionPlanningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(e) => write!(f, "{e}"),
            Self::Derivation(e) => write!(f, "{e}"),
        }
    }
}
impl std::error::Error for AdmissionPlanningError {}
/// Plans the complete initial window; replay always uses persisted run identities.
/// The request was bounds-checked at construction; the authority applies its
/// configured limits at commit.
pub fn plan_admission(
    request: EnsureTaskRequest,
    digest: AdmissionDigest,
    timestamp: u64,
) -> Result<AdmissionPlan, AdmissionPlanningError> {
    let runs = crate::derive::derive_runs(
        &crate::time::clock::MockClock::new(timestamp),
        request.task_spec().id(),
        request.task_spec().run_policy(),
        0,
        timestamp,
    )
    .map_err(AdmissionPlanningError::Derivation)?
    .into_derived();
    AdmissionPlan::new(request, runs, digest).map_err(AdmissionPlanningError::Rejected)
}
