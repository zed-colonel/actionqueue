use super::domain_v1::{ConstraintsV1, MetadataV1, PayloadV1};
// Frozen task payload layout, including feature-independent scheduling identifiers.
use super::codec::DecodeError;
use actionqueue_core::{
    ids::{TaskId, TenantId},
    task::{run_policy::RunPolicy, task_spec::TaskSpec},
};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
pub(super) struct TaskSpecV1 {
    id: TaskId,
    payload: PayloadV1,
    policy: PolicyV1,
    constraints: ConstraintsV1,
    metadata: MetadataV1,
    parent_task_id: Option<TaskId>,
    tenant_id: Option<TenantId>,
}
/// All builds encode every field; disabled kinds are rejected, never reindexed.
#[derive(Serialize, Deserialize)]
struct PolicyV1 {
    kind: u8,
    count: u32,
    interval_secs: u64,
    expression: Option<String>,
    max_occurrences: Option<u32>,
}
impl From<&TaskSpec> for TaskSpecV1 {
    fn from(s: &TaskSpec) -> Self {
        let mut policy = PolicyV1 {
            kind: 0,
            count: 0,
            interval_secs: 0,
            expression: None,
            max_occurrences: None,
        };
        match s.run_policy() {
            RunPolicy::Once => {}
            RunPolicy::Repeat(r) => {
                policy.kind = 1;
                policy.count = r.count();
                policy.interval_secs = r.interval_secs();
            }
            #[cfg(feature = "workflow")]
            RunPolicy::Cron(c) => {
                policy.kind = 2;
                policy.expression = Some(c.expression().into());
                policy.max_occurrences = c.max_occurrences();
            }
        }
        Self {
            id: s.id(),
            payload: PayloadV1::from(s.task_payload()),
            policy,
            constraints: ConstraintsV1::from(s.constraints()),
            metadata: MetadataV1::from(s.metadata()),
            parent_task_id: s.parent_task_id(),
            tenant_id: s.tenant_id(),
        }
    }
}
impl TryFrom<TaskSpecV1> for TaskSpec {
    type Error = DecodeError;
    fn try_from(s: TaskSpecV1) -> Result<Self, Self::Error> {
        let invalid = |e: String| DecodeError::Decode(e);
        let p = s.policy;
        let policy = match p.kind {
            0 if p.count == 0
                && p.interval_secs == 0
                && p.expression.is_none()
                && p.max_occurrences.is_none() =>
            {
                RunPolicy::Once
            }
            1 if p.expression.is_none() && p.max_occurrences.is_none() => {
                RunPolicy::repeat(p.count, p.interval_secs).map_err(|e| invalid(e.to_string()))?
            }
            #[cfg(feature = "workflow")]
            2 if p.count == 0 && p.interval_secs == 0 => {
                let mut cron = actionqueue_core::task::run_policy::CronPolicy::new(
                    p.expression.ok_or_else(|| invalid("missing cron expression".into()))?,
                )
                .map_err(|e| invalid(e.to_string()))?;
                if let Some(max) = p.max_occurrences {
                    cron = cron.with_max_occurrences(max).map_err(|e| invalid(e.to_string()))?;
                }
                RunPolicy::Cron(cron)
            }
            _ => return Err(invalid("unsupported or invalid scheduling policy v1".into())),
        };
        let mut spec = TaskSpec::new(
            s.id,
            s.payload.into(),
            policy,
            s.constraints.try_into()?,
            s.metadata.into(),
        )
        .map_err(|e| invalid(e.to_string()))?;
        if let Some(parent) = s.parent_task_id {
            if parent.is_nil() {
                return Err(invalid("nil parent".into()));
            }
            spec = spec.with_parent(parent);
        }
        if let Some(tenant) = s.tenant_id {
            if tenant.as_uuid().is_nil() {
                return Err(invalid("nil tenant".into()));
            }
            spec = spec.with_tenant(tenant);
        }
        Ok(spec)
    }
}
