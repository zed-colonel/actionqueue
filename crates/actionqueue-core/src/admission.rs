//! Bounded, canonical task admission contracts.
use crate::bounded::ContentHash;
use crate::causal::{CausalContext, ControlMutationContext};
use crate::ids::{AdmissionKey, TaskId};
use crate::run::RunInstance;
use crate::task::task_spec::{TaskSpec, TaskSpecError};
pub mod canonical;
/// Digest of the explicitly versioned canonical admission bytes (ADR-002/003).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "AdmissionDigestWire"))]
pub struct AdmissionDigest {
    canonical_version: u32,
    hash: ContentHash,
}
impl AdmissionDigest {
    /// Declares a v1 SHA-256 digest. Commit always recomputes and verifies it.
    pub fn new(hash: ContentHash) -> Self {
        Self { canonical_version: 1, hash }
    }
    /// Validates a declared canonical version. Unsupported algorithms are rejected by ContentHash.
    pub fn versioned(version: u32, hash: ContentHash) -> Result<Self, AdmissionRejection> {
        if version != 1 && version != 2 {
            return Err(AdmissionRejection::UnsupportedCanonicalVersion(version));
        }
        Ok(Self { canonical_version: version, hash })
    }
    /// Returns the canonical version.
    pub fn canonical_version(&self) -> u32 {
        self.canonical_version
    }
    /// Returns the declared digest.
    pub fn hash(&self) -> &ContentHash {
        &self.hash
    }
}
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
struct AdmissionDigestWire {
    canonical_version: u32,
    hash: ContentHash,
}
#[cfg(feature = "serde")]
impl TryFrom<AdmissionDigestWire> for AdmissionDigest {
    type Error = AdmissionRejection;
    fn try_from(w: AdmissionDigestWire) -> Result<Self, Self::Error> {
        Self::versioned(w.canonical_version, w.hash)
    }
}
/// Pure rejection vocabulary; existence, ancestry and tenant checks require the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionRejection {
    /// Existing key has different canonical content.
    Conflict {
        /// Original task identity.
        existing_task_id: TaskId,
        /// Conflicting key.
        admission_key: AdmissionKey,
        /// Stored digest.
        existing_digest: AdmissionDigest,
        /// Proposed digest.
        proposed_digest: AdmissionDigest,
    },
    /// Digest did not match the canonical request.
    InvalidDigest,
    /// Canonical version is unsupported.
    UnsupportedCanonicalVersion(u32),
    /// A task UUID already exists (no foreign admission details are returned).
    TaskIdCollision,
    /// A run UUID already exists.
    RunIdCollision,
    /// Run state, count, timestamp, or schedule is inconsistent with initial admission.
    InvalidRuns,
    /// Invalid non-task reference identity.
    InvalidIdentity,
    /// Hierarchy would exceed eight edges.
    HierarchyDepth,
    /// A required store or binary feature is unavailable.
    UnsupportedFeature,
    /// Admission must be synced immediately.
    ImmediateDurabilityRequired,
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
    crate::limits::AdmissionLimits::default().validate_spec(spec, dependencies.len())?;
    spec.validate().map_err(AdmissionRejection::InvalidTask)?;
    if spec.parent_task_id().is_some_and(|id| id.is_nil())
        || spec.tenant_id().is_some_and(|id| id.as_uuid().is_nil())
        || dependencies.iter().any(|id| id.is_nil())
    {
        return Err(AdmissionRejection::InvalidIdentity);
    }
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
        if let Some(link) = causal_context.causation() {
            if link.parent_task_id().is_some_and(|id| id.is_nil())
                || link.parent_run_id().is_some_and(|id| id.as_uuid().is_nil())
                || link.parent_attempt_id().is_some_and(|id| id.as_uuid().is_nil())
            {
                return Err(AdmissionRejection::InvalidIdentity);
            }
        }
        Ok(Self { admission_key, task_spec, dependencies, causal_context, control_context })
    }
    /// Computes canonical v1 meaning, ignoring lookup key and control attribution.
    pub fn digest(&self) -> Result<AdmissionDigest, AdmissionRejection> {
        canonical::CanonicalAdmissionV2::new(self)?.digest()
    }
    /// Stable convenience identity for callers retaining a preallocated task UUID.
    pub fn for_task(
        task_spec: TaskSpec,
        dependencies: Vec<TaskId>,
    ) -> Result<Self, AdmissionRejection> {
        let identity = format!("task/{}", task_spec.id());
        Self::new(
            AdmissionKey::new(identity.clone()).expect("bounded UUID key"),
            task_spec,
            dependencies,
            CausalContext::new(
                crate::ids::TraceId::new(identity.clone()).expect("bounded UUID trace"),
                crate::ids::CorrelationId::new(identity).expect("bounded UUID correlation"),
            ),
            None,
        )
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
#[serde(deny_unknown_fields)]
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
        /// Original durable admission sequence.
        sequence: u64,
        /// Task id.
        task_id: TaskId,
        /// Admission key.
        admission_key: AdmissionKey,
        /// Digest.
        digest: AdmissionDigest,
    },
    /// AlreadyExists admission.
    AlreadyExists {
        /// Original durable admission sequence.
        sequence: u64,
        /// Task id.
        task_id: TaskId,
        /// Admission key.
        admission_key: AdmissionKey,
        /// Digest.
        digest: AdmissionDigest,
    },
}

impl EnsureTaskOutcome {
    /// Original task identity.
    pub fn task_id(&self) -> TaskId {
        match self {
            Self::Created { task_id, .. } | Self::AlreadyExists { task_id, .. } => *task_id,
        }
    }
    /// Original durable sequence.
    pub fn sequence(&self) -> u64 {
        match self {
            Self::Created { sequence, .. } | Self::AlreadyExists { sequence, .. } => *sequence,
        }
    }
    /// Whether this call created a new admission.
    pub fn is_created(&self) -> bool {
        matches!(self, Self::Created { .. })
    }
}
