//! Derived exact-reference lookup. Rebuilt by admission insertion on replay and hydration.
use actionqueue_core::ids::{TaskId, TenantId};
use std::collections::{BTreeSet, HashMap};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReferenceField {
    Trace,
    Correlation,
    Origin,
}
#[derive(Debug, Clone, Default)]
pub struct InspectionIndex {
    tasks: HashMap<(Option<TenantId>, ReferenceField, String), BTreeSet<TaskId>>,
}
impl InspectionIndex {
    pub(crate) fn insert(&mut self, r: &crate::mutation::admission::AdmissionRecord) {
        let c = r.request().causal_context();
        for (field, value) in [
            (ReferenceField::Trace, Some(c.trace_id().as_str())),
            (ReferenceField::Correlation, Some(c.correlation_id().as_str())),
            (ReferenceField::Origin, c.origin_ref().map(|v| v.expose())),
        ] {
            if let Some(value) = value {
                self.tasks
                    .entry((r.tenant_id(), field, value.into()))
                    .or_default()
                    .insert(r.task_id());
            }
        }
    }
}
impl super::reducer::ReplayReducer {
    /// Exact, namespace-local lookup; no parsing or normalization of references.
    pub fn tasks_by_reference(
        &self,
        tenant: Option<TenantId>,
        field: ReferenceField,
        value: &str,
    ) -> impl Iterator<Item = TaskId> + '_ {
        self.inspection_index
            .tasks
            .get(&(tenant, field, value.into()))
            .into_iter()
            .flat_map(|ids| ids.iter().copied())
    }
}

impl super::reducer::ReplayReducer {
    /// Control targets are recorded identities, never inferred from wall-clock timestamps.
    pub fn task_control_sequence(&self, task: TaskId) -> Option<u64> {
        self.cancellations
            .iter()
            .filter(|c| c.target == actionqueue_core::mutation::CancelTarget::Task(task))
            .map(|c| c.sequence)
            .max()
            .or_else(|| self.task_admission(task).map(|a| a.sequence()))
    }
}
