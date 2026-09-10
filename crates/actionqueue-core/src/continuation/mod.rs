//! Pure continuation shapes; persistence and execution arrive in later work items.
pub mod signal;
pub use signal::*;
pub mod wait;
pub use wait::*;
pub mod checkpoint;
pub use checkpoint::*;
pub mod resume;
pub use resume::*;
