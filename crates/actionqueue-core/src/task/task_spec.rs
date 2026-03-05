//! Task specification - the user's durable intent.

use crate::ids::{TaskId, TenantId};
use crate::task::constraints::{TaskConstraints, TaskConstraintsError};
use crate::task::metadata::TaskMetadata;
use crate::task::run_policy::{RunPolicy, RunPolicyError};

/// Typed validation errors for [`TaskSpec`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskSpecError {
    /// The provided task identifier is nil/empty and therefore invalid at task admission.
    InvalidTaskId {
        /// Rejected task identifier.
        task_id: TaskId,
    },
    /// The provided constraints payload violates task-constraint invariants.
    InvalidConstraints(TaskConstraintsError),
    /// The provided run policy violates contract-level run-policy invariants.
    InvalidRunPolicy(RunPolicyError),
}

impl std::fmt::Display for TaskSpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskSpecError::InvalidTaskId { task_id } => {
                write!(f, "invalid task identifier for task admission: {task_id}")
            }
            TaskSpecError::InvalidConstraints(error) => {
                write!(f, "invalid task constraints: {error}")
            }
            TaskSpecError::InvalidRunPolicy(error) => {
                write!(f, "invalid run policy: {error}")
            }
        }
    }
}

impl std::error::Error for TaskSpecError {}

/// Opaque payload bytes bundled with an optional content-type hint.
///
/// Groups the two fields that are always set together when constructing or
/// updating a task's executable content.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TaskPayload {
    /// Opaque payload bytes to be executed.
    bytes: Vec<u8>,
    /// Optional content type hint for the payload.
    content_type: Option<String>,
}

impl TaskPayload {
    /// Creates a new payload with no content-type hint.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes, content_type: None }
    }

    /// Creates a new payload with a content-type hint.
    pub fn with_content_type(bytes: Vec<u8>, content_type: impl Into<String>) -> Self {
        Self { bytes, content_type: Some(content_type.into()) }
    }

    /// Returns the opaque payload bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the optional content-type hint.
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }
}

/// The user's durable intent - what should be executed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[must_use]
pub struct TaskSpec {
    /// Unique identifier for this task.
    id: TaskId,
    /// Opaque payload bytes and optional content-type hint.
    payload: TaskPayload,
    /// The run policy governs how many times this task should execute.
    run_policy: RunPolicy,
    /// Constraints that control execution behavior.
    constraints: TaskConstraints,
    /// Metadata for organization and priority.
    metadata: TaskMetadata,
    /// Optional parent task identifier for task hierarchy.
    ///
    /// When set, this task is a child of the named parent. The workflow
    /// extension (`actionqueue-workflow`) enforces cascading cancellation and
    /// completion gating based on this relationship.
    #[cfg_attr(feature = "serde", serde(default))]
    parent_task_id: Option<TaskId>,
    /// Optional tenant identifier for multi-tenant isolation.
    ///
    /// When set, this task is scoped to the named tenant. The platform
    /// extension (`actionqueue-platform`) enforces tenant isolation at query
    /// and mutation boundaries. `None` in single-tenant mode.
    #[cfg_attr(feature = "serde", serde(default))]
    tenant_id: Option<TenantId>,
}

impl TaskSpec {
    /// Creates a task specification with validated constraints.
    ///
    /// `parent_task_id` defaults to `None`. Use [`with_parent`](Self::with_parent)
    /// to attach a parent before submission.
    pub fn new(
        id: TaskId,
        payload: TaskPayload,
        run_policy: RunPolicy,
        constraints: TaskConstraints,
        metadata: TaskMetadata,
    ) -> Result<Self, TaskSpecError> {
        let spec = Self {
            id,
            payload,
            run_policy,
            constraints,
            metadata,
            parent_task_id: None,
            tenant_id: None,
        };
        spec.validate()?;
        Ok(spec)
    }

    /// Attaches a parent task identifier, returning the modified spec.
    ///
    /// Used by the workflow extension to declare parent-child relationships.
    /// The parent must exist in the projection when the child is submitted.
    ///
    /// Uses `debug_assert` (not `Result`) because this is only called from
    /// internal workflow submission paths where the parent ID has already been
    /// validated against the projection. A nil parent indicates a logic error
    /// in the submission pipeline, not invalid user input.
    pub fn with_parent(mut self, parent_task_id: TaskId) -> Self {
        debug_assert!(!parent_task_id.is_nil());
        self.parent_task_id = Some(parent_task_id);
        self
    }

    /// Validates this task specification against invariant-sensitive checks.
    pub fn validate(&self) -> Result<(), TaskSpecError> {
        Self::validate_task_id(self.id)?;
        self.run_policy.validate().map_err(TaskSpecError::InvalidRunPolicy)?;
        self.constraints.validate().map_err(TaskSpecError::InvalidConstraints)
    }

    /// Validates task identifier shape invariants at task-admission boundaries.
    fn validate_task_id(task_id: TaskId) -> Result<(), TaskSpecError> {
        if task_id.is_nil() {
            return Err(TaskSpecError::InvalidTaskId { task_id });
        }

        Ok(())
    }

    /// Returns this task's unique identifier.
    pub fn id(&self) -> TaskId {
        self.id
    }

    /// Returns the task payload bytes.
    pub fn payload(&self) -> &[u8] {
        self.payload.bytes()
    }

    /// Returns the optional payload content-type hint.
    pub fn content_type(&self) -> Option<&str> {
        self.payload.content_type()
    }

    /// Returns a reference to the full task payload (bytes + content-type).
    pub fn task_payload(&self) -> &TaskPayload {
        &self.payload
    }

    /// Returns the run policy snapshot for this task.
    pub fn run_policy(&self) -> &RunPolicy {
        &self.run_policy
    }

    /// Returns the constraints snapshot for this task.
    pub fn constraints(&self) -> &TaskConstraints {
        &self.constraints
    }

    /// Returns task metadata.
    pub fn metadata(&self) -> &TaskMetadata {
        &self.metadata
    }

    /// Returns the optional parent task identifier.
    pub fn parent_task_id(&self) -> Option<TaskId> {
        self.parent_task_id
    }

    /// Returns the optional tenant identifier.
    pub fn tenant_id(&self) -> Option<TenantId> {
        self.tenant_id
    }

    /// Attaches a tenant identifier, returning the modified spec.
    pub fn with_tenant(mut self, tenant_id: TenantId) -> Self {
        self.tenant_id = Some(tenant_id);
        self
    }

    /// Replaces task constraints after validating invariants.
    pub fn set_constraints(&mut self, constraints: TaskConstraints) -> Result<(), TaskSpecError> {
        Self::validate_task_id(self.id)?;
        constraints.validate().map_err(TaskSpecError::InvalidConstraints)?;
        self.constraints = constraints;
        Ok(())
    }

    /// Replaces task metadata.
    pub fn set_metadata(&mut self, metadata: TaskMetadata) {
        self.metadata = metadata;
    }

    /// Replaces payload bytes and optional content-type hint.
    pub fn set_payload(&mut self, payload: TaskPayload) {
        self.payload = payload;
    }

    /// Replaces run policy after validating policy-shape invariants.
    pub fn set_run_policy(&mut self, run_policy: RunPolicy) -> Result<(), TaskSpecError> {
        Self::validate_task_id(self.id)?;
        run_policy.validate().map_err(TaskSpecError::InvalidRunPolicy)?;
        self.run_policy = run_policy;
        Ok(())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for TaskSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct TaskSpecWire {
            id: TaskId,
            payload: TaskPayload,
            run_policy: RunPolicy,
            constraints: TaskConstraints,
            metadata: TaskMetadata,
            #[serde(default)]
            parent_task_id: Option<TaskId>,
            #[serde(default)]
            tenant_id: Option<TenantId>,
        }

        let wire = TaskSpecWire::deserialize(deserializer)?;
        let spec = TaskSpec {
            id: wire.id,
            payload: wire.payload,
            run_policy: wire.run_policy,
            constraints: wire.constraints,
            metadata: wire.metadata,
            parent_task_id: wire.parent_task_id,
            tenant_id: wire.tenant_id,
        };
        spec.validate().map_err(serde::de::Error::custom)?;
        Ok(spec)
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::ids::TaskId;
    use crate::task::constraints::TaskConstraints;
    use crate::task::metadata::TaskMetadata;
    use crate::task::run_policy::{RepeatPolicy, RunPolicy, RunPolicyError};
    use crate::task::task_spec::{TaskPayload, TaskSpec, TaskSpecError};

    #[test]
    fn task_spec_new_rejects_nil_task_id() {
        let task_id = TaskId::from_uuid(Uuid::nil());

        let result = TaskSpec::new(
            task_id,
            TaskPayload::new(b"payload".to_vec()),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        );

        assert_eq!(result, Err(TaskSpecError::InvalidTaskId { task_id }));
    }

    #[test]
    fn task_spec_validate_rejects_nil_task_id_from_externally_shaped_payload() {
        let task_id = TaskId::from_uuid(Uuid::nil());
        let externally_shaped = TaskSpec {
            id: task_id,
            payload: TaskPayload::new(b"payload".to_vec()),
            run_policy: RunPolicy::Once,
            constraints: TaskConstraints::default(),
            metadata: TaskMetadata::default(),
            parent_task_id: None,
            tenant_id: None,
        };

        assert_eq!(externally_shaped.validate(), Err(TaskSpecError::InvalidTaskId { task_id }));
    }

    #[test]
    fn repeat_policy_rejects_zero_count() {
        let result = RepeatPolicy::new(0, 60);
        assert_eq!(result, Err(RunPolicyError::InvalidRepeatCount { count: 0 }));
    }

    #[test]
    fn repeat_policy_rejects_zero_interval() {
        let result = RepeatPolicy::new(3, 0);
        assert_eq!(result, Err(RunPolicyError::InvalidRepeatIntervalSecs { interval_secs: 0 }));
    }

    #[test]
    fn set_run_policy_accepts_valid_repeat_payload() {
        let mut task_spec = TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(b"payload".to_vec()),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .expect("baseline task spec should be valid");

        task_spec
            .set_run_policy(RunPolicy::repeat(4, 30).expect("repeat policy should be valid"))
            .expect("run policy mutation should succeed");

        assert_eq!(task_spec.run_policy(), &RunPolicy::Repeat(RepeatPolicy::new(4, 30).unwrap()));
    }

    #[test]
    fn set_run_policy_rejects_mutation_for_nil_task_id() {
        let task_id = TaskId::from_uuid(Uuid::nil());
        let mut externally_shaped = TaskSpec {
            id: task_id,
            payload: TaskPayload::new(b"payload".to_vec()),
            run_policy: RunPolicy::Once,
            constraints: TaskConstraints::default(),
            metadata: TaskMetadata::default(),
            parent_task_id: None,
            tenant_id: None,
        };

        let result = externally_shaped
            .set_run_policy(RunPolicy::repeat(2, 60).expect("repeat policy should be valid"));

        assert_eq!(result, Err(TaskSpecError::InvalidTaskId { task_id }));
        assert_eq!(externally_shaped.run_policy(), &RunPolicy::Once);
    }
}
