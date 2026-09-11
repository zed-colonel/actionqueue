//! Shared ancestry checks for live admission, replay and snapshot hydration.
use actionqueue_core::continuation::{SignalEnvelope, SignalRejection as R};

use crate::recovery::reducer::ReplayReducer;
impl ReplayReducer {
    pub(crate) fn validate_signal_references(&self, e: &SignalEnvelope) -> Result<(), R> {
        if e.tenant_id.is_some() && !cfg!(feature = "platform") {
            return Err(R::UnsupportedFeature);
        }
        if e.tenant_id.is_some_and(|t| !self.tenant_exists(t)) {
            return Err(R::TenantMismatch);
        }
        if let Some(c) = &e.causation {
            if let Some(id) = c.parent_task_id() {
                let task = self.get_task(&id).ok_or(R::InvalidCausation)?;
                if task.tenant_id() != e.tenant_id {
                    return Err(R::TenantMismatch);
                }
            }
            if let Some(id) = c.parent_run_id() {
                let run = self.get_run_instance(&id).ok_or(R::InvalidCausation)?;
                if Some(run.task_id()) != c.parent_task_id() {
                    return Err(R::InvalidCausation);
                }
                if let Some(attempt) = c.parent_attempt_id() {
                    if self
                        .get_attempt_history(&id)
                        .is_none_or(|h| !h.iter().any(|a| a.attempt_id == attempt))
                    {
                        return Err(R::InvalidCausation);
                    }
                }
            }
        }
        Ok(())
    }
}
