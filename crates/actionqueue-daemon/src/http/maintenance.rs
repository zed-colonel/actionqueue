//! Remote scheduler ownership is serialized with every HTTP mutation.
//!
//! Each maintenance pass also publishes a snapshot checkpoint once the configured
//! number of durable WAL events has accumulated since the last published one. The
//! checkpoint is captured under the mutation lane and encoded and published after
//! the lane is released; failed publications are retried with bounded backoff.
use actionqueue_core::control::ControlError;
use actionqueue_storage::{
    mutation::StorageMutationAuthority, recovery::reducer::ReplayReducer, wal::writer::WalWriter,
};

use super::{ControlMutationAuthority, RouterState};
use std::time::{Duration, Instant};

const INITIAL_RETRY: Duration = Duration::from_secs(1);
const MAX_RETRY: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(crate) struct SnapshotRetry {
    next_attempt: Option<Instant>,
    delay: Option<Duration>,
}
impl SnapshotRetry {
    fn ready(&self, now: Instant) -> bool {
        self.next_attempt.is_none_or(|deadline| now >= deadline)
    }
    fn failed(&mut self, now: Instant) -> Duration {
        let delay = self.delay.map_or(INITIAL_RETRY, |d| (d * 2).min(MAX_RETRY));
        self.delay = Some(delay);
        self.next_attempt = Some(now + delay);
        delay
    }
}

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
///
/// Returns `Ok` without doing anything once [`shutdown`] has begun, while another
/// holder has the mutation lane, or when the router has no control authority. It
/// returns an error once the daemon requires recovery.
///
/// When a snapshot is due, the pass captures a checkpoint under the mutation lane,
/// then encodes, fsyncs and renames it synchronously on the calling thread after
/// releasing the lane. Host timers must therefore call this from a blocking
/// context. A failed publication is logged, not returned: durable progress stays
/// pending and later passes retry after a backoff of 1 second, doubling to at most
/// 60 seconds. A WAL flush failure during capture fences the daemon and returns
/// the error.
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
    if !due
        || !state.snapshot_retry.lock().unwrap_or_else(|e| e.into_inner()).ready(Instant::now())
        || state.snapshot_inflight.swap(true, std::sync::atomic::Ordering::AcqRel)
    {
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
    let candidate = match authority.capture_checkpoint(state.clock.now()) {
        Err(e) if authority.recovery_required() => {
            state.operational_failed.store(true, std::sync::atomic::Ordering::Release);
            return Err(ControlError::Mutation(format!("checkpoint capture failed: {e}")));
        }
        candidate => candidate,
    };
    // Keep the captured roots immutable; release the mutation lane before encoding.
    drop(authority);
    drop(_permit);
    match candidate.and_then(actionqueue_storage::snapshot::checkpoint::publish_checkpoint) {
        Ok(c) => {
            *state.snapshot_retry.lock().unwrap_or_else(|e| e.into_inner()) =
                SnapshotRetry::default();
            tracing::debug!(sequence = c.sequence, "checkpoint published");
        }
        Err(e) => {
            let delay = state
                .snapshot_retry
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .failed(Instant::now());
            // Only attempted publications log failures; cooldown ticks are silent.
            tracing::error!(error=%e, retry_after_seconds=delay.as_secs(), "checkpoint publication failed; durable progress remains pending");
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
                if let Err(error) = tick(&state) {
                    state.operational_failed.store(true, std::sync::atomic::Ordering::Release);
                    tracing::error!(%error, "daemon maintenance failed");
                }
            })
            .await;
        }
    });
    *state.maintenance_task.lock().unwrap_or_else(|e| e.into_inner()) = Some(task);
}

/// Stop scheduling maintenance and await the last blocking pass. Hosts call this
/// after draining HTTP requests, before expecting the store lock to be released.
///
/// Later [`tick`] calls do nothing. Shutdown also waits for any snapshot
/// publication still running off the mutation lane, including one started by a
/// host-driven `tick` on another thread.
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
    use super::*;

    #[test]
    fn snapshot_retry_is_exponential_bounded_and_reset_after_success() {
        let mut retry = SnapshotRetry::default();
        let mut now = Instant::now();
        assert!(retry.ready(now));
        for seconds in [1, 2, 4, 8, 16, 32, 60, 60] {
            let delay = retry.failed(now);
            assert_eq!(delay, Duration::from_secs(seconds));
            assert!(!retry.ready(now + delay - Duration::from_nanos(1)));
            now += delay;
            assert!(retry.ready(now));
        }
        retry = SnapshotRetry::default();
        assert!(retry.ready(now));
        assert_eq!(retry.failed(now), INITIAL_RETRY);
    }

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
        let deadline = state.snapshot_retry.lock().unwrap().next_attempt;
        assert_eq!(state.snapshot_retry.lock().unwrap().delay, Some(INITIAL_RETRY));
        {
            let mut a = state.control_authority.as_ref().unwrap().lock().unwrap();
            let sequence = a.projection().latest_sequence() + 1;
            let _ = a
                .submit_command(
                    MutationCommand::EngineResume(EngineResumeCommand::new(sequence, 2)),
                    DurabilityPolicy::Immediate,
                )
                .unwrap();
        }
        // A second fault stays armed during cooldown: these ticks do no encoding
        // or publication and leave the first retry deadline untouched.
        actionqueue_storage::store::fault::fail_once("snapshot_before_rename");
        for _ in 0..10 {
            super::tick(state).unwrap();
            assert_eq!(state.snapshot_retry.lock().unwrap().next_attempt, deadline);
            assert_eq!(state.store_session.as_ref().unwrap().published_sequence(), 0);
            assert_eq!(state.shared_projection.read().unwrap().latest_sequence(), 3);
        }
        state.snapshot_retry.lock().unwrap().next_attempt = Some(Instant::now());
        super::tick(state).unwrap();
        assert_eq!(state.snapshot_retry.lock().unwrap().delay, Some(INITIAL_RETRY * 2));
        assert_eq!(state.store_session.as_ref().unwrap().published_sequence(), 0);
        state.snapshot_retry.lock().unwrap().next_attempt = Some(Instant::now());
        super::tick(state).unwrap();
        assert_eq!(state.store_session.as_ref().unwrap().published_sequence(), 3);
        assert_eq!(state.snapshot_retry.lock().unwrap().delay, None);
        assert!(!state.snapshot_inflight.load(std::sync::atomic::Ordering::Acquire));
        drop(owner);
        assert_eq!(
            actionqueue_storage::store::verify_store(dir.path()).unwrap().snapshot_sequence,
            3
        );
    }

    #[test]
    fn checkpoint_wal_flush_failure_fences_the_daemon_and_reports_its_cause() {
        use crate::{bootstrap::bootstrap, config::DaemonConfig};
        use actionqueue_core::{
            bounded::OpaqueRef, causal::ControlMutationContext, control::*, mutation::*,
        };
        let dir = tempfile::tempdir().unwrap();
        let owner = bootstrap(DaemonConfig {
            data_dir: dir.path().to_owned(),
            snapshot_event_threshold: Some(1),
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
                attribution: ControlMutationContext::new(OpaqueRef::new("daemon-flush").unwrap()),
            }));
            let seq = a.projection().latest_sequence() + 1;
            let _ = a
                .submit_command(
                    MutationCommand::EnginePause(EnginePauseCommand::new(seq, 1)),
                    DurabilityPolicy::Immediate,
                )
                .unwrap();
        }
        actionqueue_storage::store::fault::fail_once("wal_before_sync");
        let error = super::tick(state).unwrap_err();
        assert!(
            matches!(&error, ControlError::Mutation(m)
                if m.starts_with("checkpoint capture failed: ") && m.contains("wal_before_sync")),
            "{error}"
        );
        assert!(state.operational_failed.load(std::sync::atomic::Ordering::Acquire));
        assert!(!state.snapshot_inflight.load(std::sync::atomic::Ordering::Acquire));
        assert!(!dir.path().join("snapshots/snapshot.bin").exists());
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
