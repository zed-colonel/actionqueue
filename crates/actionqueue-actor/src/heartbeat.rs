//! Per-actor heartbeat tracking and timeout detection.

use std::collections::HashMap;

use actionqueue_core::actor::HeartbeatPolicy;
use actionqueue_core::ids::ActorId;
use tracing;

/// Per-actor heartbeat state.
struct HeartbeatState {
    last_heartbeat_at: u64,
    policy: HeartbeatPolicy,
}

/// Tracks last heartbeat timestamps and detects timeout conditions.
///
/// The dispatch loop calls [`check_timeouts`](HeartbeatMonitor::check_timeouts)
/// once per tick to find actors whose heartbeat has expired.
#[derive(Default)]
pub struct HeartbeatMonitor {
    heartbeats: HashMap<ActorId, HeartbeatState>,
}

impl HeartbeatMonitor {
    /// Creates an empty monitor.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records actor registration with initial heartbeat timestamp.
    pub fn record_registration(&mut self, actor_id: ActorId, policy: HeartbeatPolicy, now: u64) {
        tracing::debug!(
            %actor_id,
            timeout_secs = policy.timeout_secs(),
            "heartbeat tracking started"
        );
        self.heartbeats.insert(actor_id, HeartbeatState { last_heartbeat_at: now, policy });
    }

    /// Records a heartbeat for an active actor.
    pub fn record_heartbeat(&mut self, actor_id: ActorId, timestamp: u64) {
        if let Some(state) = self.heartbeats.get_mut(&actor_id) {
            tracing::trace!(%actor_id, timestamp, "heartbeat recorded");
            state.last_heartbeat_at = timestamp;
        }
    }

    /// Removes tracking for an actor (called on deregistration).
    pub fn remove(&mut self, actor_id: ActorId) {
        self.heartbeats.remove(&actor_id);
    }

    /// Returns actor IDs whose heartbeat has timed out at `now`.
    ///
    /// An actor times out when `now >= last_heartbeat_at + timeout_secs`.
    pub fn check_timeouts(&self, now: u64) -> Vec<ActorId> {
        let timed_out: Vec<ActorId> = self
            .heartbeats
            .iter()
            .filter(|(_, state)| {
                let deadline = state.last_heartbeat_at.saturating_add(state.policy.timeout_secs());
                now >= deadline
            })
            .map(|(&id, _)| id)
            .collect();
        for &actor_id in &timed_out {
            tracing::warn!(%actor_id, now, "actor heartbeat timeout detected");
        }
        timed_out
    }

    /// Returns `true` if the actor's heartbeat is within the timeout window.
    pub fn is_alive(&self, actor_id: ActorId, now: u64) -> bool {
        self.heartbeats.get(&actor_id).is_some_and(|state| {
            let deadline = state.last_heartbeat_at.saturating_add(state.policy.timeout_secs());
            now < deadline
        })
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::actor::HeartbeatPolicy;
    use actionqueue_core::ids::ActorId;

    use super::HeartbeatMonitor;

    fn policy(interval_secs: u64) -> HeartbeatPolicy {
        HeartbeatPolicy::new(interval_secs, 3)
    }

    #[test]
    fn actor_alive_within_window() {
        let mut monitor = HeartbeatMonitor::new();
        let id = ActorId::new();
        monitor.record_registration(id, policy(10), 1000);
        // timeout = 10 * 3 = 30 secs. At t=1029, still alive.
        assert!(monitor.is_alive(id, 1029));
    }

    #[test]
    fn actor_times_out_after_deadline() {
        let mut monitor = HeartbeatMonitor::new();
        let id = ActorId::new();
        monitor.record_registration(id, policy(10), 1000);
        // timeout = 30. At t=1030, timed out.
        let timed_out = monitor.check_timeouts(1030);
        assert!(timed_out.contains(&id));
        assert!(!monitor.is_alive(id, 1030));
    }

    #[test]
    fn heartbeat_resets_timeout() {
        let mut monitor = HeartbeatMonitor::new();
        let id = ActorId::new();
        monitor.record_registration(id, policy(10), 1000);
        monitor.record_heartbeat(id, 1025);
        // New deadline: 1025 + 30 = 1055. At t=1054, alive.
        assert!(monitor.is_alive(id, 1054));
        assert!(monitor.check_timeouts(1054).is_empty());
    }

    #[test]
    fn remove_stops_tracking() {
        let mut monitor = HeartbeatMonitor::new();
        let id = ActorId::new();
        monitor.record_registration(id, policy(10), 1000);
        monitor.remove(id);
        assert!(!monitor.is_alive(id, 1030));
        assert!(monitor.check_timeouts(9999).is_empty());
    }
}
