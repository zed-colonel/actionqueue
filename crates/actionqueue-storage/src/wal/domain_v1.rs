//! Frozen domain shapes used inside v1 payloads. Runtime struct evolution cannot
//! silently append fields to these durable layouts.
use actionqueue_core::{
    executor::ExecutorTraits,
    ids::{AttemptId, RunId, TaskId},
    run::{state::RunState, RunInstance},
    task::{
        constraints::{ConcurrencyKeyHoldPolicy, TaskConstraints},
        metadata::TaskMetadata,
        safety::SafetyLevel,
        task_spec::TaskPayload,
    },
};
use serde::{Deserialize, Serialize};

use super::codec::DecodeError;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PayloadV1 {
    bytes: Vec<u8>,
    content_type: Option<String>,
}
impl From<&TaskPayload> for PayloadV1 {
    fn from(p: &TaskPayload) -> Self {
        Self { bytes: p.bytes().to_vec(), content_type: p.content_type().map(str::to_owned) }
    }
}
impl From<PayloadV1> for TaskPayload {
    fn from(p: PayloadV1) -> Self {
        match p.content_type {
            Some(t) => Self::with_content_type(p.bytes, t),
            None => Self::new(p.bytes),
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MetadataV1 {
    tags: Vec<String>,
    priority: i32,
    description: Option<String>,
}
impl From<&TaskMetadata> for MetadataV1 {
    fn from(m: &TaskMetadata) -> Self {
        Self {
            tags: m.tags().to_vec(),
            priority: m.priority(),
            description: m.description().map(str::to_owned),
        }
    }
}
impl From<MetadataV1> for TaskMetadata {
    fn from(m: MetadataV1) -> Self {
        Self::new(m.tags, m.priority, m.description)
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConstraintsV1 {
    max_attempts: u32,
    timeout_secs: Option<u64>,
    concurrency_key: Option<String>,
    concurrency_key_hold_policy: ConcurrencyKeyHoldPolicy,
    safety_level: SafetyLevel,
    required_executor_traits: Option<ExecutorTraits>,
}
impl From<&TaskConstraints> for ConstraintsV1 {
    fn from(c: &TaskConstraints) -> Self {
        Self {
            max_attempts: c.max_attempts(),
            timeout_secs: c.timeout_secs(),
            concurrency_key: c.concurrency_key().map(str::to_owned),
            concurrency_key_hold_policy: c.concurrency_key_hold_policy(),
            safety_level: c.safety_level(),
            required_executor_traits: c.required_executor_traits().cloned(),
        }
    }
}
impl TryFrom<ConstraintsV1> for TaskConstraints {
    type Error = DecodeError;
    fn try_from(c: ConstraintsV1) -> Result<Self, Self::Error> {
        serde_json::from_value(
            serde_json::to_value(c).map_err(|e| DecodeError::Decode(e.to_string()))?,
        )
        .map_err(|e| DecodeError::Decode(e.to_string()))
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RunV1 {
    id: RunId,
    task_id: TaskId,
    state: RunState,
    current_attempt_id: Option<AttemptId>,
    attempt_count: u32,
    created_at: u64,
    scheduled_at: u64,
    effective_priority: i32,
    last_state_change_at: u64,
}
impl From<&RunInstance> for RunV1 {
    fn from(r: &RunInstance) -> Self {
        Self {
            id: r.id(),
            task_id: r.task_id(),
            state: r.state(),
            current_attempt_id: r.current_attempt_id(),
            attempt_count: r.attempt_count(),
            created_at: r.created_at(),
            scheduled_at: r.scheduled_at(),
            effective_priority: r.effective_priority(),
            last_state_change_at: r.last_state_change_at(),
        }
    }
}
impl TryFrom<RunV1> for RunInstance {
    type Error = DecodeError;
    fn try_from(r: RunV1) -> Result<Self, Self::Error> {
        // The core deserializer validates exact persisted fields without reconstructing events.
        serde_json::from_value(
            serde_json::to_value(r).map_err(|e| DecodeError::Decode(e.to_string()))?,
        )
        .map_err(|e| DecodeError::Decode(e.to_string()))
    }
}
