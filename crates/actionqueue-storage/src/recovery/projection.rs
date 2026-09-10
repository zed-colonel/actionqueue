//! Exact v1 projection image, validated hydration, and canonical SHA-256.
use super::reducer::*;
use crate::{
    snapshot::{mapping::*, model::Snapshot},
    store::StoreError,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionDigest {
    pub algorithm: String,
    pub version: u32,
    pub hex: String,
}
#[derive(Debug, Clone)]
pub(crate) struct ProjectionImageV1(pub Snapshot);
fn invalid(e: impl std::fmt::Display) -> StoreError {
    StoreError::InvalidStore(e.to_string())
}
/// Canonical tree encoding: typed one-byte tags; 64-bit LE lengths and integers;
/// UTF-8 strings; objects sorted by UTF-8 field names; chronological arrays retained.
/// The JSON value is an intermediate typed tree, never serializer output bytes.
fn canonical(value: &serde_json::Value, out: &mut Vec<u8>) -> Result<(), StoreError> {
    use serde_json::Value as V;
    match value {
        V::Null => out.push(0),
        V::Bool(v) => out.extend_from_slice(&[1, u8::from(*v)]),
        V::Number(n) => {
            if let Some(v) = n.as_u64() {
                out.push(2);
                out.extend_from_slice(&v.to_le_bytes());
            } else if let Some(v) = n.as_i64() {
                out.push(3);
                out.extend_from_slice(&v.to_le_bytes());
            } else {
                return Err(invalid("floating point is not permitted in projection v1"));
            }
        }
        V::String(v) => {
            out.push(4);
            out.extend_from_slice(&(v.len() as u64).to_le_bytes());
            out.extend_from_slice(v.as_bytes());
        }
        V::Array(v) => {
            out.push(5);
            out.extend_from_slice(&(v.len() as u64).to_le_bytes());
            for item in v {
                canonical(item, out)?;
            }
        }
        V::Object(v) => {
            out.push(6);
            out.extend_from_slice(&(v.len() as u64).to_le_bytes());
            let mut entries: Vec<_> = v.iter().collect();
            entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            for (key, value) in entries {
                canonical(&V::String(key.clone()), out)?;
                canonical(value, out)?;
            }
        }
    }
    Ok(())
}
fn key<T: Serialize>(v: &T) -> Vec<u8> {
    let mut bytes = Vec::new();
    canonical(&serde_json::to_value(v).expect("projection value"), &mut bytes)
        .expect("integer projection");
    bytes
}
pub(crate) fn normalize(snapshot: &mut Snapshot) {
    snapshot.timestamp = 0;
    snapshot.tasks.sort_by_key(|r| *r.task_spec.id().as_uuid());
    snapshot.runs.sort_by_key(|r| *r.run_instance.id().as_uuid());
    for d in &mut snapshot.dependency_declarations {
        d.depends_on.sort_by_key(|id| *id.as_uuid());
    }
    snapshot.dependency_declarations.sort_by_key(|d| *d.task_id.as_uuid());
    snapshot.budgets.sort_by_key(key);
    snapshot.subscriptions.sort_by_key(|s| *s.subscription_id.as_uuid());
    snapshot.actors.sort_by_key(|a| *a.actor_id.as_uuid());
    snapshot.tenants.sort_by_key(|t| *t.tenant_id.as_uuid());
    snapshot.role_assignments.sort_by_key(key);
    snapshot.capability_grants.sort_by_key(key);
}
pub(crate) fn snapshot_digest(snapshot: &Snapshot) -> Result<ProjectionDigest, StoreError> {
    let mut image = snapshot.clone();
    normalize(&mut image);
    let mut bytes = b"AQ-CONT-1\0projection\0v1\0".to_vec();
    canonical(&serde_json::to_value(image).map_err(invalid)?, &mut bytes)?;
    Ok(ProjectionDigest {
        algorithm: "sha256".into(),
        version: 1,
        hex: format!("{:x}", Sha256::digest(bytes)),
    })
}
fn convert<S: Serialize, T: serde::de::DeserializeOwned>(v: S) -> Result<T, StoreError> {
    serde_json::from_value(serde_json::to_value(v).map_err(invalid)?).map_err(invalid)
}
impl ReplayReducer {
    pub(crate) fn projection_image(&self) -> Result<ProjectionImageV1, StoreError> {
        let snapshot =
            crate::snapshot::build::build_snapshot_from_projection(self, 0).map_err(invalid)?;
        Ok(ProjectionImageV1(snapshot))
    }
    pub fn projection_digest(&self) -> Result<ProjectionDigest, StoreError> {
        snapshot_digest(&self.projection_image()?.0)
    }
    pub(crate) fn from_projection_image(image: ProjectionImageV1) -> Result<Self, StoreError> {
        let s = image.0;
        validate_snapshot(&s).map_err(invalid)?;
        let original_digest = snapshot_digest(&s)?;
        let mut r = Self::new();
        r.latest_sequence = s.metadata.wal_sequence;
        for task in s.tasks {
            let id = task.task_spec.id();
            if let Some(t) = task.canceled_at {
                r.task_canceled_at.insert(id, t);
            }
            r.tasks.insert(id, convert(task)?);
        }
        for run in s.runs {
            let id = run.run_instance.id();
            if !r.tasks.contains_key(&run.run_instance.task_id()) {
                return Err(invalid("orphan run"));
            }
            r.runs.insert(id, run.run_instance.state());
            r.runs_by_task.entry(run.run_instance.task_id()).or_default().push(id);
            r.run_history.insert(id, map_snapshot_run_history(run.state_history));
            r.attempt_history.insert(id, map_snapshot_attempt_history(run.attempts));
            if let Some(l) = map_snapshot_lease_metadata(run.lease) {
                r.leases.insert(id, (l.owner.clone(), l.expiry));
                r.lease_metadata.insert(id, l);
            }
            r.run_instances.insert(id, run.run_instance);
        }
        r.engine_paused = s.engine.paused;
        r.engine_paused_at = s.engine.paused_at;
        r.engine_resumed_at = s.engine.resumed_at;
        for d in s.dependency_declarations {
            r.dependency_declared_at.insert(d.task_id, d.declared_at);
            r.dependency_declarations.insert(d.task_id, d.depends_on.into_iter().collect());
        }
        for b in s.budgets {
            r.budgets.insert((b.task_id, b.dimension), convert(b)?);
        }
        for sub in s.subscriptions {
            r.subscriptions.insert(sub.subscription_id, convert(sub)?);
        }
        for a in s.actors {
            r.actors.insert(a.actor_id, convert(a)?);
        }
        for t in s.tenants {
            r.tenants.insert(t.tenant_id, convert(t)?);
        }
        for role in s.role_assignments {
            r.role_assignments.insert((role.actor_id, role.tenant_id), convert(role)?);
        }
        for c in s.capability_grants {
            r.capability_grants.insert(
                (c.actor_id, super::reducer::capability_key(&c.capability), c.tenant_id),
                convert(c)?,
            );
        }
        for entry in s.ledger_entries {
            r.ledger_entries.push(convert(entry)?);
        }
        if r.projection_digest()? != original_digest {
            return Err(invalid("projection image loses state during hydration"));
        }
        Ok(r)
    }
}
