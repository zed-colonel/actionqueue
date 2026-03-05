//! Authoritative daemon time surfaces.
//!
//! This module defines the daemon-owned clock abstraction used by daemon
//! runtime wiring and metrics derivation paths that require deterministic time
//! sourcing.

pub mod clock;
