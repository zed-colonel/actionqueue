//! Storage identity, compatibility, and lifetime ownership.
pub mod error;
pub mod manifest;
pub mod session;
pub use error::StoreError;
pub use manifest::{capabilities, StoreManifest};
pub use session::{open_store, OpenOptions, StoreSession};

pub(crate) fn check_event_profile(
    event: &crate::wal::event::WalEventType,
    profile: &[String],
) -> Result<(), StoreError> {
    use crate::wal::event::WalEventType as E;
    let required = match event {
        E::BudgetAllocated { .. }
        | E::BudgetConsumed { .. }
        | E::BudgetExhausted { .. }
        | E::BudgetReplenished { .. }
        | E::SubscriptionCreated { .. }
        | E::SubscriptionTriggered { .. }
        | E::SubscriptionCanceled { .. } => Some("budget"),
        E::ActorRegistered { .. } | E::ActorDeregistered { .. } | E::ActorHeartbeat { .. } => {
            Some("actor")
        }
        E::TenantCreated { .. }
        | E::RoleAssigned { .. }
        | E::CapabilityGranted { .. }
        | E::CapabilityRevoked { .. }
        | E::LedgerEntryAppended { .. } => Some("platform"),
        E::TaskCreated { task_spec, .. } => {
            if task_spec.tenant_id().is_some() {
                require_feature(profile, "platform")?;
            }
            if task_spec.parent_task_id().is_some() { /* hierarchy is a retained base operation */ }
            #[cfg(feature = "workflow")]
            if matches!(
                task_spec.run_policy(),
                actionqueue_core::task::run_policy::RunPolicy::Cron(_)
            ) {
                require_feature(profile, "workflow")?;
            }
            None
        }
        _ => None,
    };
    if let Some(feature) = required {
        require_feature(profile, feature)?;
    }
    Ok(())
}
fn require_feature(profile: &[String], feature: &str) -> Result<(), StoreError> {
    if profile.iter().any(|f| f == feature) {
        Ok(())
    } else {
        Err(StoreError::UnsupportedFeatures(vec![feature.into()]))
    }
}

pub mod backup;
pub use backup::{backup_store, inspect_store, restore_store, BackupDescriptor, StoreInspection};

#[doc(hidden)]
pub mod fault;
