//! Remote scheduler ownership is serialized with every HTTP mutation.
use super::{ControlMutationAuthority, RouterState};
use actionqueue_core::control::ControlError;
use actionqueue_storage::{
    mutation::StorageMutationAuthority, recovery::reducer::ReplayReducer, wal::writer::WalWriter,
};

pub(crate) fn maintain_locked<W: WalWriter>(
    state: &RouterState,
    a: &mut StorageMutationAuthority<W, ReplayReducer>,
) -> Result<(), ControlError> {
    let result = actionqueue_runtime::remote::maintain(a, state.clock.now(), state.remote_policy);
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
    let mut authority =
        authority.lock().map_err(|_| ControlError::Mutation("authority poisoned".into()))?;
    maintain_locked(state, &mut authority)
}
pub(crate) fn start(state: &RouterState) {
    if state.control_authority.is_none() {
        return;
    }
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        return;
    };
    if state.maintenance_started.swap(true, std::sync::atomic::Ordering::AcqRel) {
        return;
    }
    let weak = std::sync::Arc::downgrade(state);
    runtime.spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            interval.tick().await;
            let Some(state) = weak.upgrade() else {
                break;
            };
            if let Err(error) = tick(&state) {
                tracing::error!(%error, "remote scheduler maintenance failed");
            }
        }
    });
}
