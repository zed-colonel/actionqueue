//! ID newtypes used by core domain entities.

/// Actor identifier type.
pub mod actor_id;
/// Attempt identifier type.
pub mod attempt_id;
/// Department identifier type.
pub mod department_id;
/// Ledger entry identifier type.
pub mod ledger_entry_id;
/// Run identifier type.
pub mod run_id;
/// Task identifier type.
pub mod task_id;
/// Tenant identifier type.
pub mod tenant_id;

pub use actor_id::ActorId;
pub use attempt_id::AttemptId;
pub use department_id::{DepartmentId, DepartmentIdError};
pub use ledger_entry_id::LedgerEntryId;
pub use run_id::RunId;
pub use task_id::TaskId;
pub use tenant_id::TenantId;
