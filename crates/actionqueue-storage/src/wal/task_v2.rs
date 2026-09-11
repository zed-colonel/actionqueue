//! Frozen schema 2 task payload: schema 1 task followed by explicit wait-key policy.
use super::{codec::DecodeError, task_v1::TaskSpecV1};
use actionqueue_core::task::{constraints::ConcurrencyKeyWaitPolicy, task_spec::TaskSpec};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
pub(super) struct TaskSpecV2 {
    base: TaskSpecV1,
    wait_policy: u8,
}
impl From<&TaskSpec> for TaskSpecV2 {
    fn from(s: &TaskSpec) -> Self {
        Self {
            base: s.into(),
            wait_policy: match s.constraints().concurrency_key_wait_policy() {
                ConcurrencyKeyWaitPolicy::ReleaseWhileAwaiting => 0,
                ConcurrencyKeyWaitPolicy::HoldWhileAwaiting => 1,
            },
        }
    }
}
impl TryFrom<TaskSpecV2> for TaskSpec {
    type Error = DecodeError;
    fn try_from(s: TaskSpecV2) -> Result<Self, Self::Error> {
        let mut task: TaskSpec = s.base.try_into()?;
        let policy = match s.wait_policy {
            0 => ConcurrencyKeyWaitPolicy::ReleaseWhileAwaiting,
            1 => ConcurrencyKeyWaitPolicy::HoldWhileAwaiting,
            _ => return Err(DecodeError::Decode("invalid wait-key policy".into())),
        };
        let mut constraints = task.constraints().clone();
        constraints.set_concurrency_key_wait_policy(policy);
        let parent = task.parent_task_id();
        let tenant = task.tenant_id();
        task = TaskSpec::new(
            task.id(),
            task.task_payload().clone(),
            task.run_policy().clone(),
            constraints,
            task.metadata().clone(),
        )
        .map_err(|e| DecodeError::Decode(e.to_string()))?;
        if let Some(id) = parent {
            task = task.with_parent(id);
        }
        if let Some(id) = tenant {
            task = task.with_tenant(id);
        }
        Ok(task)
    }
}
