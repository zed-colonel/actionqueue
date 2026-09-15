#![forbid(unsafe_code)]
//! Storage utilities and abstractions for the ActionQueue system.
//!
//! This crate provides storage facilities for the ActionQueue system, including
//! Write-Ahead Log (WAL) persistence, snapshot management, and recovery replay.
//!
//! # Overview
//!
//! The storage crate defines the persistence layer for the ActionQueue system:
//!
//! - [`mutation`] - Storage-owned WAL-first mutation authority
//! - [`wal`] - Write-Ahead Log for event persistence
//! - [`snapshot`] - State snapshots for recovery acceleration
//! - [`recovery`] - WAL replay and state reconstruction
//!
//! # Example
//!
//! ```
//! use actionqueue_core::admission::{AdmissionPlan, EnsureTaskRequest};
//! use actionqueue_core::causal::CausalContext;
//! use actionqueue_core::ids::TaskId;
//! use actionqueue_core::ids::{AdmissionKey, CorrelationId, TraceId};
//! use actionqueue_core::mutation::{
//!     AdmissionCommitCommand, DurabilityPolicy, MutationAuthority, MutationCommand,
//! };
//! use actionqueue_core::run::RunInstance;
//! use actionqueue_core::task::constraints::TaskConstraints;
//! use actionqueue_core::task::metadata::TaskMetadata;
//! use actionqueue_core::task::run_policy::RunPolicy;
//! use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
//! use actionqueue_storage::recovery::reducer::ReplayReducer;
//! use actionqueue_storage::wal::fs_writer::WalFsWriter;
//!
//! let root = std::env::temp_dir().join(format!("aq-example-{}", TaskId::new()));
//! let session = actionqueue_storage::store::open_store(
//!     &root,
//!     actionqueue_storage::store::OpenOptions::Initialize { features: vec![] },
//! )
//! .unwrap();
//! let mut authority = session.into_authority().unwrap();
//!
//! let task_id = TaskId::new();
//! let task_spec = TaskSpec::new(
//!     task_id,
//!     TaskPayload::with_content_type(b"example-payload".to_vec(), "application/octet-stream"),
//!     RunPolicy::Once,
//!     TaskConstraints::default(),
//!     TaskMetadata::default(),
//! )
//! .expect("task spec should be valid");
//!
//! authority.set_control_context(Some(actionqueue_core::control::HostControlContext {
//!     actor_id: None,
//!     scope: actionqueue_core::control::ControlScope::SingleTenant,
//!     attribution: actionqueue_core::causal::ControlMutationContext::new(
//!         actionqueue_core::bounded::OpaqueRef::new("example-host").unwrap(),
//!     ),
//! }));
//! let request = EnsureTaskRequest::new(
//!     AdmissionKey::new("example/1").unwrap(),
//!     task_spec.clone(),
//!     vec![],
//!     CausalContext::new(TraceId::new("trace/1").unwrap(), CorrelationId::new("work/1").unwrap()),
//!     None,
//! )
//! .unwrap();
//! let digest = request.digest().unwrap();
//! let plan = AdmissionPlan::new(
//!     request,
//!     vec![RunInstance::new_scheduled(task_id, 0, 0).unwrap()],
//!     digest,
//! )
//! .unwrap();
//! authority
//!     .submit_command(
//!         MutationCommand::AdmissionCommit(AdmissionCommitCommand::new(2, plan, None, 0)),
//!         DurabilityPolicy::Immediate,
//!     )
//!     .expect("authority command should succeed");
//!
//! # // Clean up
//! # let _ = std::fs::remove_dir_all(root);
//! ```

pub mod mutation;
pub mod recovery;
pub mod snapshot;
pub mod wal;

/// Target store identity and ownership.
pub mod store;

#[cfg(feature = "serde")]
mod structural_filter;
