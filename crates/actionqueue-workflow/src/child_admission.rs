//! Pure child proposals and durable coordination. No I/O or mutable submission channel.
//! Retain task IDs and local keys across retries (for example in a durable checkpoint).
//! Keys are scoped by storage to the parent task and run, independent of attempt identity.
use actionqueue_core::{
    admission::AdmissionRejection, causal::CausalOverride, continuation::*, disposition::*, ids::*,
    task::task_spec::*,
};
/// Construct a child with explicit lifecycle policy; attribution is inherited by storage.
// Preserve the explicit dependencies of this existing boundary API.
#[allow(clippy::too_many_arguments)]
pub fn child(
    parent: TaskId,
    key: AdmissionKey,
    task: TaskSpec,
    dependencies: Vec<TaskId>,
    policy: ChildLifecyclePolicy,
    attribution: CausalOverride,
) -> Result<ChildAdmission, AdmissionRejection> {
    ChildAdmission::new(key, task.with_parent_policy(parent, policy), dependencies, attribution)
}
/// Atomically admit a bounded batch and wait on direct children, including prior batches.
pub fn awaiting_children(
    wait: WaitSpec,
    checkpoint: Option<CheckpointRef>,
    children: Vec<ChildAdmission>,
) -> Result<AttemptDisposition, DispositionError> {
    AttemptDisposition::new(
        DispositionOutcome::Awaiting,
        DispositionParts {
            checkpoint,
            wait: Some(wait),
            child_admissions: children,
            ..Default::default()
        },
    )
}
