//! Schema 3 task: frozen schema 2 followed by the child lifecycle tag.
use super::{codec::DecodeError, task_v2::TaskSpecV2};
use actionqueue_core::task::task_spec::{ChildLifecyclePolicy, TaskSpec};
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TaskSpecV3 {
    base: TaskSpecV2,
    child_policy: u8,
}
impl From<&TaskSpec> for TaskSpecV3 {
    fn from(s: &TaskSpec) -> Self {
        Self {
            base: s.into(),
            child_policy: match s.child_lifecycle_policy() {
                ChildLifecyclePolicy::Required => 0,
                ChildLifecyclePolicy::Detached => 1,
            },
        }
    }
}
impl TryFrom<TaskSpecV3> for TaskSpec {
    type Error = DecodeError;
    fn try_from(s: TaskSpecV3) -> Result<Self, Self::Error> {
        let task: TaskSpec = s.base.try_into()?;
        let policy = match s.child_policy {
            0 => ChildLifecyclePolicy::Required,
            1 => ChildLifecyclePolicy::Detached,
            _ => return Err(DecodeError::Decode("invalid child lifecycle policy".into())),
        };
        match task.parent_task_id() {
            Some(parent) => Ok(task.with_parent_policy(parent, policy)),
            None if policy == ChildLifecyclePolicy::Required => Ok(task),
            _ => Err(DecodeError::Decode("detached policy requires parent".into())),
        }
    }
}
