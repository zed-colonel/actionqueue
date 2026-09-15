//! Bounded, process-local admission conflict budget at the authenticated host boundary.
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use actionqueue_core::{
    admission::EnsureTaskRequest,
    control::{HostControlContext, QueueAction},
    ids::{ActorId, TenantId},
};
use actionqueue_storage::mutation::control::{authorize_projection, check_scope};

use super::RouterState;

const BURST: u32 = 8;
const WINDOW: Duration = Duration::from_secs(30);
const MAX_SCOPES: usize = 1024;
type Scope = (Option<TenantId>, Option<ActorId>);

#[derive(Default)]
pub(crate) struct AdmissionThrottle {
    scopes: HashMap<Scope, (Instant, u32)>,
}
impl AdmissionThrottle {
    /// Called only for a known changed digest, never for a new key or exact retry.
    fn allow_conflict(&mut self, scope: Scope, now: Instant) -> bool {
        self.scopes.retain(|_, (start, _)| now.duration_since(*start) < WINDOW);
        // Do not evict live budgets: rotating identities cannot reset another budget.
        if !self.scopes.contains_key(&scope) && self.scopes.len() == MAX_SCOPES {
            return false;
        }
        let (_, used) = self.scopes.entry(scope).or_insert((now, 0));
        if *used >= BURST {
            return false;
        }
        *used += 1;
        true
    }
}

/// Reject only a proven conflict using one published projection and its grants.
/// All other requests still pass through fresh authorization in the authority lane.
pub(crate) fn throttled(
    state: &RouterState,
    host: &HostControlContext,
    q: &EnsureTaskRequest,
) -> bool {
    let Ok(p) = state.shared_projection.read() else { return false };
    let platform = state
        .store_session
        .as_ref()
        .is_some_and(|s| s.manifest().features.iter().any(|f| f == "platform"));
    let Ok(tenant) = authorize_projection(&p, platform, host, QueueAction::AdmitTask) else {
        return false;
    };
    if check_scope(tenant, q.task_spec().tenant_id()).is_err() {
        return false;
    }
    let Some(existing) = p.admission(tenant, q.admission_key()) else { return false };
    let Ok(digest) = q.digest() else { return false };
    if existing.digest() == &digest {
        return false;
    }
    !state
        .admission_throttle
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .allow_conflict((tenant, host.actor_id), Instant::now())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sequential_conflicts_scope_isolation_and_cooldown() {
        let mut limiter = AdmissionThrottle::default();
        let now = Instant::now();
        let scope = (Some(TenantId::new()), Some(ActorId::new()));
        for _ in 0..BURST {
            assert!(limiter.allow_conflict(scope, now));
        }
        assert!(!limiter.allow_conflict(scope, now + WINDOW - Duration::from_nanos(1)));
        assert!(limiter.allow_conflict((Some(TenantId::new()), scope.1), now));
        assert!(limiter.allow_conflict((scope.0, Some(ActorId::new())), now));
        assert!(limiter.allow_conflict(scope, now + WINDOW));
    }
    #[test]
    fn capacity_is_bounded_without_evicting_live_budgets() {
        let mut limiter = AdmissionThrottle::default();
        let now = Instant::now();
        for _ in 0..MAX_SCOPES {
            assert!(limiter.allow_conflict((Some(TenantId::new()), None), now));
        }
        assert!(!limiter.allow_conflict((None, None), now));
        assert_eq!(limiter.scopes.len(), MAX_SCOPES);
        assert!(limiter.allow_conflict((None, None), now + WINDOW));
        assert_eq!(limiter.scopes.len(), 1);
    }
}
