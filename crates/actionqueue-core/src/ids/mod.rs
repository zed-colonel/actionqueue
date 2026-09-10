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
/// AdmissionKey identity.
pub mod admission_key;
pub use admission_key::AdmissionKey;
/// SignalId identity.
pub mod signal_id;
pub use signal_id::SignalId;
/// TraceId identity.
pub mod trace_id;
pub use trace_id::TraceId;
/// CorrelationId identity.
pub mod correlation_id;
pub use correlation_id::CorrelationId;
/// WaitId identity.
pub mod wait_id;
pub use wait_id::WaitId;
/// CheckpointId identity.
pub mod checkpoint_id;
pub use checkpoint_id::CheckpointId;
/// SignalSequence identity.
pub mod signal_sequence;
pub use signal_sequence::SignalSequence;
