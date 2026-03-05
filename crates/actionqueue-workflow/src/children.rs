//! Dispatch-time snapshot building for child task states.
//!
//! The re-exported types from `actionqueue-executor-local` are the canonical
//! snapshot types; this module provides the builder that queries the projection
//! to construct them.

use actionqueue_core::ids::TaskId;
pub use actionqueue_executor_local::children::{ChildState, ChildrenSnapshot};
use actionqueue_storage::recovery::reducer::ReplayReducer;

/// Builds a [`ChildrenSnapshot`] by querying the projection for all tasks
/// whose `parent_task_id` matches `parent_id`.
///
/// Returns `None` if there are no children (avoids allocating an empty snapshot).
/// Returns `Some(snapshot)` with all child run states at the current projection
/// sequence — this is an immutable point-in-time view safe to pass to handlers
/// running on `spawn_blocking`.
pub fn build_children_snapshot(
    projection: &ReplayReducer,
    parent_id: TaskId,
) -> Option<ChildrenSnapshot> {
    let children: Vec<ChildState> = projection
        .task_records()
        .filter(|tr| tr.task_spec().parent_task_id() == Some(parent_id))
        .map(|tr| {
            let child_id = tr.task_spec().id();
            let run_states: Vec<_> = projection
                .run_instances()
                .filter(|r| r.task_id() == child_id)
                .map(|r| (r.id(), r.state()))
                .collect();
            ChildState::new(child_id, run_states)
        })
        .collect();

    if children.is_empty() {
        None
    } else {
        Some(ChildrenSnapshot::new(children))
    }
}
