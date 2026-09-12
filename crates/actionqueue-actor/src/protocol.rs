//! Versioned remote execution envelopes. Authenticated actor identity is supplied
//! separately by the host and cannot be replaced by any body field.
use actionqueue_core::{
    disposition::AttemptDisposition,
    disposition_digest::DispositionDigest,
    ids::{AttemptId, RunId},
    mutation::LeaseFence,
};
/// Initial remote protocol version.
pub const PROTOCOL_VERSION: u32 = 1;
/// Supported execution contract revision.
pub const CONTRACT_REVISION: &str = "AQ-CONT-1-r2";
/// Validate compatibility at registration, claim and result boundaries.
pub fn supported(protocol: u32, revision: &str) -> bool {
    protocol == PROTOCOL_VERSION && revision == CONTRACT_REVISION
}
/// Stable lease owner derived from authenticated identity, never a display label.
pub fn lease_owner(
    actor: actionqueue_core::ids::ActorId,
) -> actionqueue_core::mutation::LeaseOwner {
    format!("actor:{actor}").into()
}
/// Claim identity doubles as its exact-retry identity.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteClaim {
    pub protocol_version: u32,
    pub contract_revision: String,
    pub run_id: RunId,
    pub attempt_id: AttemptId,
}
/// Complete fenced result, with all proposed subordinate effects under its digest.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteAttemptResult {
    pub protocol_version: u32,
    pub contract_revision: String,
    pub run_id: RunId,
    pub attempt_id: AttemptId,
    pub lease_fence: LeaseFence,
    pub disposition_digest: DispositionDigest,
    pub disposition: AttemptDisposition,
}
