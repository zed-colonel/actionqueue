//! Storage-owned mutation authority surfaces.
//!
//! Durable lifecycle mutations must route through the authority lane defined in
//! [`authority`]. Replay reducers remain replay-only projection components.

pub mod authority;

pub use authority::{
    MutationAuthorityError, MutationProjection, MutationValidationError, StorageMutationAuthority,
};
