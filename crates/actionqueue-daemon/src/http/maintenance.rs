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
    super::publish_progress(state, a)
        .map_err(|_| ControlError::Mutation("projection unavailable".into()))?;
    result
}
/// Executes one pass explicitly, also useful for hosts with their own timer.
pub fn tick(state: &RouterState) -> Result<(), ControlError> {
    if state.maintenance_stopping.load(std::sync::atomic::Ordering::Acquire) {
        return Ok(());
    }
    if state.operational_failed.load(std::sync::atomic::Ordering::Acquire) {
        return Err(ControlError::Mutation("recovery required".into()));
    }
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
    if state.maintenance_stopping.load(std::sync::atomic::Ordering::Acquire) {
        return Ok(());
    }
    maintain_locked(state, &mut authority)?;
    let due = state.snapshot_event_threshold.is_some_and(|threshold| {
        authority.store_session().is_some_and(|s| {
            authority.projection().latest_sequence().saturating_sub(s.published_sequence())
                >= threshold
        })
    });
    if !due || state.snapshot_inflight.swap(true, std::sync::atomic::Ordering::AcqRel) {
        return Ok(());
    }
    struct PublicationGuard<'a>(&'a RouterState);
    impl Drop for PublicationGuard<'_> {
        fn drop(&mut self) {
            self.0.snapshot_inflight.store(false, std::sync::atomic::Ordering::Release);
            self.0.snapshot_completed.notify_waiters();
        }
    }
    let _publication = PublicationGuard(state);
    let candidate = authority.capture_checkpoint(state.clock.now());
    if authority.recovery_required() {
        state.operational_failed.store(true, std::sync::atomic::Ordering::Release);
        return Err(ControlError::Mutation("checkpoint WAL flush failed".into()));
    }
    // Keep the captured roots immutable; release the mutation lane before encoding.
    drop(authority);
    drop(_permit);
    match candidate.and_then(actionqueue_storage::snapshot::checkpoint::publish_checkpoint) {
        Ok(c) => tracing::debug!(sequence = c.sequence, "checkpoint published"),
        Err(e) => {
            tracing::error!(error=%e,"checkpoint publication failed; durable progress remains pending")
        }
    }
    Ok(())
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
            if state.maintenance_stopping.load(std::sync::atomic::Ordering::Acquire)
                || state.operational_failed.load(std::sync::atomic::Ordering::Acquire)
            {
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
    // An embedding host may also have called synchronous tick on its own worker.
    // Cross the authority lock before checking the off-lane publication flag.
    if let Some(authority) = state.control_authority.clone() {
        let _ = tokio::task::spawn_blocking(move || {
            drop(authority.lock().unwrap_or_else(|e| e.into_inner()));
        })
        .await;
    }
    loop {
        let completed = state.snapshot_completed.notified();
        tokio::pin!(completed);
        completed.as_mut().enable();
        if !state.snapshot_inflight.load(std::sync::atomic::Ordering::Acquire) {
            break;
        }
        completed.await;
    }
}

#[cfg(test)]
mod checkpoint_tests {
    #[test]
    fn daemon_retries_snapshot_failures_and_serializes_publication() {
        use crate::{bootstrap::bootstrap, config::DaemonConfig};
        use actionqueue_core::{
            bounded::OpaqueRef, causal::ControlMutationContext, control::*, mutation::*,
        };
        let dir = tempfile::tempdir().unwrap();
        let owner = bootstrap(DaemonConfig {
            data_dir: dir.path().to_owned(),
            snapshot_event_threshold: Some(2),
            metrics_bind: None,
            ..Default::default()
        })
        .unwrap();
        let state = owner.router_state();
        {
            let mut a = state.control_authority.as_ref().unwrap().lock().unwrap();
            a.set_control_context(Some(HostControlContext {
                actor_id: None,
                scope: ControlScope::Store,
                attribution: ControlMutationContext::new(
                    OpaqueRef::new("daemon-publication").unwrap(),
                ),
            }));
            let seq = a.projection().latest_sequence() + 1;
            let _ = a
                .submit_command(
                    MutationCommand::EnginePause(EnginePauseCommand::new(seq, 1)),
                    DurabilityPolicy::Immediate,
                )
                .unwrap();
        }
        state.snapshot_inflight.store(true, std::sync::atomic::Ordering::Release);
        super::tick(state).unwrap();
        assert!(!dir.path().join("snapshots/snapshot.bin").exists());
        state.snapshot_inflight.store(false, std::sync::atomic::Ordering::Release);
        actionqueue_storage::store::fault::fail_once("snapshot_before_rename");
        super::tick(state).unwrap();
        assert!(!state.operational_failed.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(state.store_session.as_ref().unwrap().published_sequence(), 0);
        super::tick(state).unwrap();
        assert_eq!(state.store_session.as_ref().unwrap().published_sequence(), 2);
        assert!(!state.snapshot_inflight.load(std::sync::atomic::Ordering::Acquire));
        drop(owner);
        assert_eq!(
            actionqueue_storage::store::verify_store(dir.path()).unwrap().snapshot_sequence,
            2
        );
    }
}

#[cfg(test)]
mod shutdown_tests {
    #[tokio::test]
    async fn shutdown_waits_for_an_off_lane_publication() {
        let dir = tempfile::tempdir().unwrap();
        let owner = crate::bootstrap::bootstrap(crate::config::DaemonConfig {
            data_dir: dir.path().to_owned(),
            metrics_bind: None,
            ..Default::default()
        })
        .unwrap();
        let state = owner.router_state().clone();
        state.snapshot_inflight.store(true, std::sync::atomic::Ordering::Release);
        let mut shutdown = tokio::spawn({
            let state = state.clone();
            async move {
                super::shutdown(&state).await;
            }
        });
        assert!(tokio::time::timeout(std::time::Duration::from_millis(20), &mut shutdown)
            .await
            .is_err());
        state.snapshot_inflight.store(false, std::sync::atomic::Ordering::Release);
        state.snapshot_completed.notify_waiters();
        shutdown.await.unwrap();
        // Further host timer ticks are inert after shutdown has begun.
        super::tick(&state).unwrap();
        assert!(!dir.path().join("snapshots/snapshot.bin").exists());
        drop(state);
        drop(owner);
    }
}
