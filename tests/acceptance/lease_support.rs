//! Lease fence for the next attempt start. A run without a lease gets a fence no
//! authority accepts, so a missing lease surfaces as a rejection, not a panic.
#![allow(dead_code)]
use actionqueue_core::{ids::RunId, mutation::LeaseFence};
use actionqueue_storage::recovery::reducer::ReplayReducer;

pub fn fence_for(projection: &ReplayReducer, run: RunId) -> LeaseFence {
    projection
        .get_lease_metadata(&run)
        .map(|l| LeaseFence::new(l.owner().into(), l.granted_at_sequence()))
        .unwrap_or_else(|| LeaseFence::new("missing".into(), 0))
}
