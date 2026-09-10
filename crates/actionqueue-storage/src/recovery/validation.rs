//! Semantic validation shared by target append preparation and target replay.
use super::reducer::{capability_key, ReplayReducer, ReplayReducerError};
use crate::wal::event::WalEventType as E;
impl ReplayReducer {
    pub(crate) fn validate_target_event(&self, e: &E) -> Result<(), ReplayReducerError> {
        let invalid = || ReplayReducerError::CorruptedData;
        let task = |id| if self.tasks.contains_key(&id) { Ok(()) } else { Err(invalid()) };
        match e {
            E::TaskCreated { task_spec, .. } => {
                if let Some(parent) = task_spec.parent_task_id() {
                    task(parent)?;
                }
                if let Some(tenant) = task_spec.tenant_id() {
                    if !self.tenants.contains_key(&tenant) {
                        return Err(invalid());
                    }
                }
            }
            E::RunCreated { run_instance } => {
                task(run_instance.task_id())?;
                if run_instance.attempt_count() != 0 || run_instance.current_attempt_id().is_some()
                {
                    return Err(invalid());
                }
            }
            E::DependencyDeclared { task_id, depends_on, .. } => {
                task(*task_id)?;
                let mut seen = std::collections::HashSet::new();
                for id in depends_on {
                    task(*id)?;
                    if id == task_id || !seen.insert(id) {
                        return Err(invalid());
                    }
                }
                // Reject cycles in the union of durable declarations before publication.
                let mut pending = depends_on.clone();
                let mut visited = std::collections::HashSet::new();
                while let Some(id) = pending.pop() {
                    if id == *task_id {
                        return Err(invalid());
                    }
                    if visited.insert(id) {
                        if let Some(deps) = self.dependency_declarations.get(&id) {
                            pending.extend(deps);
                        }
                    }
                }
            }
            E::BudgetAllocated { task_id, dimension, .. } => {
                task(*task_id)?;
                if self.budgets.contains_key(&(*task_id, *dimension)) {
                    return Err(invalid());
                }
            }
            E::BudgetConsumed { task_id, dimension, amount, .. } => {
                self.budgets
                    .get(&(*task_id, *dimension))
                    .ok_or_else(invalid)?
                    .consumed
                    .checked_add(*amount)
                    .ok_or_else(invalid)?;
            }
            E::BudgetReplenished { task_id, dimension, .. }
            | E::BudgetExhausted { task_id, dimension, .. } => {
                if !self.budgets.contains_key(&(*task_id, *dimension)) {
                    return Err(invalid());
                }
            }
            E::SubscriptionCreated { subscription_id, task_id, .. } => {
                task(*task_id)?;
                if self.subscriptions.contains_key(subscription_id) {
                    return Err(invalid());
                }
            }
            E::SubscriptionTriggered { subscription_id, .. }
            | E::SubscriptionCanceled { subscription_id, .. } => {
                if self
                    .subscriptions
                    .get(subscription_id)
                    .ok_or_else(invalid)?
                    .canceled_at
                    .is_some()
                {
                    return Err(invalid());
                }
            }
            E::ActorRegistered {
                actor_id,
                identity,
                executor_traits,
                heartbeat_interval_secs,
                tenant_id,
                ..
            } => {
                if actor_id.as_uuid().is_nil()
                    || identity.is_empty()
                    || *heartbeat_interval_secs == 0
                    || self.is_actor_active(*actor_id)
                {
                    return Err(invalid());
                }
                actionqueue_core::executor::ExecutorTraits::new(executor_traits.clone())
                    .map_err(|_| invalid())?;
                if tenant_id.is_some_and(|id| !self.tenants.contains_key(&id)) {
                    return Err(invalid());
                }
            }
            E::ActorDeregistered { actor_id, .. } | E::ActorHeartbeat { actor_id, .. } => {
                if !self.is_actor_active(*actor_id) {
                    return Err(invalid());
                }
            }
            E::TenantCreated { tenant_id, name, .. } => {
                if tenant_id.as_uuid().is_nil()
                    || name.is_empty()
                    || self.tenants.contains_key(tenant_id)
                {
                    return Err(invalid());
                }
            }
            E::RoleAssigned { actor_id, tenant_id, .. }
            | E::CapabilityGranted { actor_id, tenant_id, .. } => {
                if !self.actors.contains_key(actor_id) || !self.tenants.contains_key(tenant_id) {
                    return Err(invalid());
                }
            }
            E::CapabilityRevoked { actor_id, capability, tenant_id, .. } => {
                if !self.actor_has_capability(*actor_id, &capability_key(capability), *tenant_id) {
                    return Err(invalid());
                }
            }
            E::LedgerEntryAppended { entry_id, tenant_id, ledger_key, actor_id, .. } => {
                if entry_id.as_uuid().is_nil()
                    || ledger_key.is_empty()
                    || !self.tenants.contains_key(tenant_id)
                    || self.ledger_entries.iter().any(|e| e.entry_id == *entry_id)
                    || actor_id.is_some_and(|id| !self.actors.contains_key(&id))
                {
                    return Err(invalid());
                }
            }
            _ => {}
        }
        Ok(())
    }
}
