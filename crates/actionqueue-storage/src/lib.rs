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
//! use actionqueue_core::ids::TaskId;
//! use actionqueue_core::mutation::{
//!     DurabilityPolicy, MutationAuthority, MutationCommand, TaskCreateCommand,
//! };
//! use actionqueue_core::task::constraints::TaskConstraints;
//! use actionqueue_core::task::metadata::TaskMetadata;
//! use actionqueue_core::task::run_policy::RunPolicy;
//! use actionqueue_core::task::task_spec::{TaskPayload, TaskSpec};
//! use actionqueue_storage::mutation::StorageMutationAuthority;
//! use actionqueue_storage::recovery::reducer::ReplayReducer;
//! use actionqueue_storage::wal::fs_writer::WalFsWriter;
//!
//! // Build a unique temporary WAL path for this process.
//! let unique = std::time::SystemTime::now()
//!     .duration_since(std::time::UNIX_EPOCH)
//!     .expect("clock should be after unix epoch")
//!     .as_nanos();
//! let wal_path = std::env::temp_dir().join(format!("actionqueue-storage-example-{unique}.wal"));
//!
//! // Authority lane owns durable ordering: validate -> append -> durability -> apply.
//! let wal_writer = WalFsWriter::new(wal_path.clone()).expect("failed to create WAL writer");
//! let projection = ReplayReducer::new();
//! let mut authority = StorageMutationAuthority::new(wal_writer, projection);
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
//! authority
//!     .submit_command(
//!         MutationCommand::TaskCreate(TaskCreateCommand::new(1, task_spec, 0)),
//!         DurabilityPolicy::Immediate,
//!     )
//!     .expect("authority command should succeed");
//!
//! # // Clean up
//! # let _ = std::fs::remove_file(wal_path);
//! ```

pub mod mutation;
pub mod recovery;
pub mod snapshot;
pub mod wal;
