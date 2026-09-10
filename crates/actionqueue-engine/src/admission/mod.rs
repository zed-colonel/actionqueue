//! Bounded initial admission planning. Attribution passes through without interpretation.
pub mod planner;
pub use planner::{plan_admission, AdmissionPlanningError};
