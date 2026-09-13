//! Remote scheduler ownership is serialized with every HTTP mutation.
use actionqueue_core::control::ControlError;
use actionqueue_storage::{
    mutation::StorageMutationAuthority, recovery::reducer::ReplayReducer, wal::writer::WalWriter,
};

use super::{ControlMutationAuthority, RouterState};

pub(crate) fn maintain_locked<W: WalWriter>(
    state: &RouterState,
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
) -> Result<(), ControlError> {
    #[cfg(feature = "actor")]
    let result = actionqueue_runtime::remote::maintain(a, state.clock.now(), state.remote_policy);
    #[cfg(not(feature = "actor"))]
    let result = actionqueue_runtime::waits::reconcile_batch(
        a,
        state.clock.now(),
        actionqueue_runtime::waits::MATCH_BATCH,
    )
    .map(|_| ())
    .map_err(|_| ControlError::Mutation("reconciliation failed".into()));
    if result.is_err() || a.recovery_required() {
        state.operational_failed.store(true, std::sync::atomic::Ordering::Release);
    }
    // Even a failed multi-record pass may have made durable progress.
    super::sync_projection(state, a)
        .map_err(|_| ControlError::Mutation("projection unavailable".into()))?;
    result
}
/// Executes one pass explicitly, also useful for hosts with their own timer.
pub fn tick(state: &RouterState) -> Result<(), ControlError> {
    let Some(authority): Option<&ControlMutationAuthority> = state.control_authority.as_ref()
    else {
        return Ok(());
    };
    let Ok(_permit) = state.authority_lane.try_acquire() else {
        return Ok(());
    };
    let mut authority =
        authority.lock().map_err(|_| ControlError::Mutation("authority poisoned".into()))?;
    if state.operational_failed.load(std::sync::atomic::Ordering::Acquire) {
        return Err(ControlError::Mutation("recovery required".into()));
    }
    maintain_locked(state, &mut authority)
}
pub(crate) fn start(state: &RouterState) {
    if !state.background_maintenance
        || state.control_authority.is_none()
        || state.maintenance_stopping.load(std::sync::atomic::Ordering::Acquire)
    {
        return;
    }
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    if state.maintenance_started.swap(true, std::sync::atomic::Ordering::AcqRel) {
        return;
    }
    let weak = std::sync::Arc::downgrade(state);
    let task = runtime.spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            interval.tick().await;
            let Some(state) = weak.upgrade() else {
                break;
            };
            if state.maintenance_stopping.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            let _ = tokio::task::spawn_blocking(move || {
                if tick(&state).is_err() {
                    state.operational_failed.store(true, std::sync::atomic::Ordering::Release);
                    tracing::error!("continuation maintenance failed");
                }
            })
            .await;
        }
    });
    *state.maintenance_task.lock().unwrap_or_else(|e| e.into_inner()) = Some(task);
}

/// Stop scheduling maintenance and await the last blocking pass. Hosts call this
/// after draining HTTP requests, before expecting the store lock to be released.
pub async fn shutdown(state: &RouterState) {
    state.maintenance_stopping.store(true, std::sync::atomic::Ordering::Release);
    let task = state.maintenance_task.lock().unwrap_or_else(|e| e.into_inner()).take();
    if let Some(task) = task {
        let _ = task.await;
    }
}
