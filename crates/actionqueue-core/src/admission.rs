//! Pure admission planning. Store-dependent rejection checks land in AQ-04.
use crate::bounded::ContentHash;
use crate::causal::{CausalContext, ControlMutationContext};
use crate::ids::{AdmissionKey, TaskId};
use crate::run::RunInstance;
use crate::task::task_spec::{TaskSpec, TaskSpecError};
/// Digest of versioned canonical admission bytes (ADR-002).
/// Canonical encoding and hashing are AQ-04 obligations, not core operations.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct AdmissionDigest(ContentHash);
impl AdmissionDigest {
    /// Wraps a structurally validated digest.
    pub fn new(hash: ContentHash) -> Self {
        Self(hash)
    }
    /// Returns the declared digest.
    pub fn hash(&self) -> &ContentHash {
        &self.0
    }
}
/// Pure rejection vocabulary; existence, ancestry and tenant checks require the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionRejection {
    /// Existing key has different canonical content.
    Conflict {
        /// Conflicting key.
        admission_key: AdmissionKey,
        /// Stored digest.
        existing_digest: AdmissionDigest,
        /// Proposed digest.
        proposed_digest: AdmissionDigest,
    },
    /// Task invariants failed.
    InvalidTask(TaskSpecError),
    /// Invalid structural parent.
    InvalidParent,
    /// Parent has already terminated.
    TerminalParent,
    /// Dependency is absent from the store.
    UnknownDependency,
    /// Self-dependency or store-detected cycle.
    DependencyCycle,
    /// Task and referenced objects belong to different tenants.
    TenantMismatch,
    /// A hard collection ceiling was exceeded.
    TooLarge,
    /// A planned run belongs to a different task.
    RunTaskMismatch,
    /// The same run identity appears more than once in one plan.
    DuplicateRun,
}
impl std::fmt::Display for AdmissionRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "admission rejected: {self:?}")
    }
}
impl std::error::Error for AdmissionRejection {}

pub(crate) fn validate_dependencies(
    spec: &TaskSpec,
    dependencies: &mut Vec<TaskId>,
) -> Result<(), AdmissionRejection> {
    spec.validate().map_err(AdmissionRejection::InvalidTask)?;
    if spec.parent_task_id() == Some(spec.id()) {
        return Err(AdmissionRejection::InvalidParent);
    }
    if dependencies.len() > crate::limits::MAX_DEPENDENCIES_PER_TASK {
        return Err(AdmissionRejection::TooLarge);
    }
    if dependencies.contains(&spec.id()) {
        return Err(AdmissionRejection::DependencyCycle);
    }
    dependencies.sort_by_key(|id| *id.as_uuid());
    dependencies.dedup();
    Ok(())
}
/// Validated EnsureTaskRequest; construction performs only pure structural checks.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "EnsureTaskRequestWire"))]
pub struct EnsureTaskRequest {
    /// Admission key.
    admission_key: AdmissionKey,
    /// Task spec.
    task_spec: TaskSpec,
    /// Dependencies.
    dependencies: Vec<TaskId>,
    /// Causal context.
    causal_context: CausalContext,
    /// Control context.
    control_context: Option<ControlMutationContext>,
}
impl EnsureTaskRequest {
    /// Validates the specification and normalizes dependency order.
    pub fn new(
        admission_key: AdmissionKey,
        task_spec: TaskSpec,
        mut dependencies: Vec<TaskId>,
        causal_context: CausalContext,
        control_context: Option<ControlMutationContext>,
    ) -> Result<Self, AdmissionRejection> {
        validate_dependencies(&task_spec, &mut dependencies)?;
        Ok(Self { admission_key, task_spec, dependencies, causal_context, control_context })
    }
    /// Returns admission key.
    pub fn admission_key(&self) -> &AdmissionKey {
        &self.admission_key
    }
    /// Returns task spec.
    pub fn task_spec(&self) -> &TaskSpec {
        &self.task_spec
    }
    /// Returns dependencies.
    pub fn dependencies(&self) -> &[TaskId] {
        &self.dependencies
    }
    /// Returns causal context.
    pub fn causal_context(&self) -> &CausalContext {
        &self.causal_context
    }
    /// Returns control context.
    pub fn control_context(&self) -> Option<&ControlMutationContext> {
        self.control_context.as_ref()
    }
}
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct EnsureTaskRequestWire {
    /// Admission key.
    admission_key: AdmissionKey,
    /// Task spec.
    task_spec: TaskSpec,
    /// Dependencies.
    dependencies: Vec<TaskId>,
    /// Causal context.
    causal_context: CausalContext,
    /// Control context.
    control_context: Option<ControlMutationContext>,
}
#[cfg(feature = "serde")]
impl TryFrom<EnsureTaskRequestWire> for EnsureTaskRequest {
    type Error = AdmissionRejection;
    fn try_from(w: EnsureTaskRequestWire) -> Result<Self, Self::Error> {
        Self::new(w.admission_key, w.task_spec, w.dependencies, w.causal_context, w.control_context)
    }
}
/// Validated AdmissionPlan; construction performs only pure structural checks.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "AdmissionPlanWire"))]
pub struct AdmissionPlan {
    /// Admission key.
    admission_key: AdmissionKey,
    /// Task spec.
    task_spec: TaskSpec,
    /// Runs.
    runs: Vec<RunInstance>,
    /// Dependencies.
    dependencies: Vec<TaskId>,
    /// Causal context.
    causal_context: CausalContext,
    /// Digest.
    digest: AdmissionDigest,
}
impl AdmissionPlan {
    /// Validates run ownership, count, and identity uniqueness. Control
    /// attribution belongs to the commit command.
    pub fn new(
        request: EnsureTaskRequest,
        runs: Vec<RunInstance>,
        digest: AdmissionDigest,
    ) -> Result<Self, AdmissionRejection> {
        if runs.len() > crate::limits::MAX_RUNS_PER_ADMISSION {
            return Err(AdmissionRejection::TooLarge);
        }
        if runs.iter().any(|run| run.task_id() != request.task_spec.id()) {
            return Err(AdmissionRejection::RunTaskMismatch);
        }
        let mut seen = std::collections::HashSet::with_capacity(runs.len());
        if runs.iter().any(|run| !seen.insert(run.id())) {
            return Err(AdmissionRejection::DuplicateRun);
        }
        Ok(Self {
            admission_key: request.admission_key,
            task_spec: request.task_spec,
            runs,
            dependencies: request.dependencies,
            causal_context: request.causal_context,
            digest,
        })
    }
    /// Returns admission key.
    pub fn admission_key(&self) -> &AdmissionKey {
        &self.admission_key
    }
    /// Returns task spec.
    pub fn task_spec(&self) -> &TaskSpec {
        &self.task_spec
    }
    /// Returns runs.
    pub fn runs(&self) -> &[RunInstance] {
        &self.runs
    }
    /// Returns dependencies.
    pub fn dependencies(&self) -> &[TaskId] {
        &self.dependencies
    }
    /// Returns causal context.
    pub fn causal_context(&self) -> &CausalContext {
        &self.causal_context
    }
    /// Returns digest.
    pub fn digest(&self) -> &AdmissionDigest {
        &self.digest
    }
}
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct AdmissionPlanWire {
    /// Admission key.
    admission_key: AdmissionKey,
    /// Task spec.
    task_spec: TaskSpec,
    /// Runs.
    runs: Vec<RunInstance>,
    /// Dependencies.
    dependencies: Vec<TaskId>,
    /// Causal context.
    causal_context: CausalContext,
    /// Digest.
    digest: AdmissionDigest,
}
#[cfg(feature = "serde")]
impl TryFrom<AdmissionPlanWire> for AdmissionPlan {
    type Error = AdmissionRejection;
    fn try_from(w: AdmissionPlanWire) -> Result<Self, Self::Error> {
        let request = EnsureTaskRequest::new(
            w.admission_key,
            w.task_spec,
            w.dependencies,
            w.causal_context,
            None,
        )?;
        Self::new(request, w.runs, w.digest)
    }
}
/// Result of ensuring a task, with the canonical key and digest.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum EnsureTaskOutcome {
    /// Created admission.
    Created {
        /// Task id.
        task_id: TaskId,
        /// Admission key.
        admission_key: AdmissionKey,
        /// Digest.
        digest: AdmissionDigest,
    },
    /// AlreadyExists admission.
    AlreadyExists {
        /// Task id.
        task_id: TaskId,
        /// Admission key.
        admission_key: AdmissionKey,
        /// Digest.
        digest: AdmissionDigest,
    },
}
