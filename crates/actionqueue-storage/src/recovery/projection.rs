//! Exact v9 projection image, validated hydration, and canonical SHA-256.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::reducer::*;
use crate::{
    snapshot::{mapping::*, model::Snapshot},
    store::StoreError,
};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionDigest {
    pub algorithm: String,
    pub version: u32,
    pub hex: String,
}
#[derive(Debug, Clone)]
pub(crate) struct ProjectionImageV8(pub Snapshot);
fn invalid(e: impl std::fmt::Display) -> StoreError {
    StoreError::InvalidStore(e.to_string())
}
/// Canonical tree encoding: typed one-byte tags; 64-bit LE lengths and integers;
/// UTF-8 strings; objects sorted by UTF-8 field names; chronological arrays retained.
/// The JSON value is an intermediate typed tree, never serializer output bytes.
fn canonical(value: &serde_json::Value, out: &mut impl std::io::Write) -> Result<(), StoreError> {
    use serde_json::Value as V;
    match value {
        V::Null => out.write_all(&[0])?,
        V::Bool(v) => out.write_all(&[1, u8::from(*v)])?,
        V::Number(n) => {
            if let Some(v) = n.as_u64() {
                out.write_all(&[2])?;
                out.write_all(&v.to_le_bytes())?;
            } else if let Some(v) = n.as_i64() {
                out.write_all(&[3])?;
                out.write_all(&v.to_le_bytes())?;
            } else {
                return Err(invalid("floating point is not permitted in projection v9"));
            }
        }
        V::String(v) => {
            out.write_all(&[4])?;
            out.write_all(&(v.len() as u64).to_le_bytes())?;
            out.write_all(v.as_bytes())?;
        }
        V::Array(v) => {
            out.write_all(&[5])?;
            out.write_all(&(v.len() as u64).to_le_bytes())?;
            for item in v {
                canonical(item, out)?;
            }
        }
        V::Object(v) => {
            out.write_all(&[6])?;
            out.write_all(&(v.len() as u64).to_le_bytes())?;
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
struct HashWriter(Sha256);
impl std::io::Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn value<T: Serialize>(v: &T, out: &mut impl std::io::Write) -> Result<(), StoreError> {
    // At most one record-sized tree; never a whole projection tree or byte buffer.
    canonical(&serde_json::to_value(v).map_err(invalid)?, out)
}
fn array<T: Serialize>(
    items: impl ExactSizeIterator<Item = T>,
    out: &mut impl std::io::Write,
) -> Result<(), StoreError> {
    out.write_all(&[5])?;
    out.write_all(&(items.len() as u64).to_le_bytes())?;
    for item in items {
        value(&item, out)?;
    }
    Ok(())
}
pub(crate) fn snapshot_digest(s: &Snapshot) -> Result<ProjectionDigest, StoreError> {
    use std::io::Write;
    let mut out = HashWriter(Sha256::new());
    out.write_all(b"AQ-CONT-1\0projection\0v9\0")?;
    out.write_all(&[6])?;
    out.write_all(&(25u64).to_le_bytes())?;
    value(&"actors", &mut out)?;
    {
        let mut items: Vec<_> = s.actors.iter().collect();
        items.sort_by_key(|r| *r.actor_id.as_uuid());
        array(items.into_iter(), &mut out)?;
    }
    value(&"administrative_pending", &mut out)?;
    {
        let mut items: Vec<_> = s.administrative_pending.iter().collect();
        items.sort_by_key(|(r, _)| *r);
        array(items.into_iter(), &mut out)?;
    }
    value(&"administrative_wakes", &mut out)?;
    {
        let mut items: Vec<_> = s.administrative_wakes.iter().collect();
        items.sort_by_key(|w| w.context_id);
        array(items.into_iter(), &mut out)?;
    }
    value(&"admissions", &mut out)?;
    {
        let mut items: Vec<_> = s.admissions.iter().collect();
        items.sort_by_key(|r| *r.task_id().as_uuid());
        array(items.into_iter(), &mut out)?;
    }
    value(&"budgets", &mut out)?;
    {
        let mut items: Vec<_> = s.budgets.iter().collect();
        items.sort_by_cached_key(|v| key(*v));
        array(items.into_iter(), &mut out)?;
    }
    value(&"cancellations", &mut out)?;
    {
        let mut items: Vec<_> = s.cancellations.iter().collect();
        items.sort_by_key(|c| c.sequence);
        array(items.into_iter(), &mut out)?;
    }
    value(&"capability_grants", &mut out)?;
    {
        let mut items: Vec<_> = s.capability_grants.iter().collect();
        items.sort_by_cached_key(|v| key(*v));
        array(items.into_iter(), &mut out)?;
    }
    value(&"control_history", &mut out)?;
    {
        let mut items: Vec<_> = s.control_history.iter().collect();
        items.sort_by_key(|(s, _)| *s);
        array(items.into_iter(), &mut out)?;
    }
    value(&"dependency_declarations", &mut out)?;
    {
        let mut items = s.dependency_declarations.clone();
        for d in &mut items {
            d.depends_on.sort_by_key(|id| *id.as_uuid());
        }
        items.sort_by_key(|d| *d.task_id.as_uuid());
        array(items.iter(), &mut out)?;
    }
    value(&"dispatch_sequences", &mut out)?;
    {
        let mut items: Vec<_> = s.dispatch_sequences.iter().collect();
        items.sort_by_key(|(r, _)| *r);
        array(items.into_iter(), &mut out)?;
    }
    value(&"engine", &mut out)?;
    value(&s.engine, &mut out)?;
    value(&"key_reservations", &mut out)?;
    {
        let mut items: Vec<_> = s.key_reservations.iter().collect();
        items.sort_by_key(|(r, _)| *r);
        array(items.into_iter(), &mut out)?;
    }
    value(&"last_signal_sequence", &mut out)?;
    value(&s.last_signal_sequence, &mut out)?;
    value(&"ledger_entries", &mut out)?;
    array(s.ledger_entries.iter(), &mut out)?;
    value(&"metadata", &mut out)?;
    value(&s.metadata, &mut out)?;
    value(&"pending_resumes", &mut out)?;
    {
        let mut items: Vec<_> = s.pending_resumes.iter().collect();
        items.sort_by_key(|(r, _)| *r);
        array(items.into_iter(), &mut out)?;
    }
    value(&"role_assignments", &mut out)?;
    {
        let mut items: Vec<_> = s.role_assignments.iter().collect();
        items.sort_by_cached_key(|v| key(*v));
        array(items.into_iter(), &mut out)?;
    }
    value(&"runs", &mut out)?;
    {
        let mut items: Vec<_> = s.runs.iter().collect();
        items.sort_by_key(|r| *r.run_instance.id().as_uuid());
        array(items.into_iter(), &mut out)?;
    }
    value(&"signals", &mut out)?;
    {
        let mut items: Vec<_> = s.signals.iter().collect();
        items.sort_by_key(|r| r.sequence());
        array(items.into_iter(), &mut out)?;
    }
    value(&"subscriptions", &mut out)?;
    {
        let mut items: Vec<_> = s.subscriptions.iter().collect();
        items.sort_by_key(|r| *r.subscription_id.as_uuid());
        array(items.into_iter(), &mut out)?;
    }
    value(&"tasks", &mut out)?;
    {
        let mut items: Vec<_> = s.tasks.iter().collect();
        items.sort_by_key(|r| *r.task_spec.id().as_uuid());
        array(items.into_iter(), &mut out)?;
    }
    value(&"tenants", &mut out)?;
    {
        let mut items: Vec<_> = s.tenants.iter().collect();
        items.sort_by_key(|r| *r.tenant_id.as_uuid());
        array(items.into_iter(), &mut out)?;
    }
    value(&"timestamp", &mut out)?;
    value(&0u64, &mut out)?;
    value(&"version", &mut out)?;
    value(&s.version, &mut out)?;
    value(&"waits", &mut out)?;
    {
        let mut items: Vec<_> = s.waits.iter().collect();
        items.sort_by_key(|w| w.spec.wait_id());
        array(items.into_iter(), &mut out)?;
    }
    Ok(ProjectionDigest {
        algorithm: "sha256".into(),
        version: 9,
        hex: format!("{:x}", out.0.finalize()),
    })
}
impl ReplayReducer {
    /// Canonical hashing sorts runs by identity; operational per-task order is
    /// validated separately so an equal digest cannot hide a reordered checkpoint.
    pub(crate) fn same_operational_order(&self, other: &Self) -> bool {
        self.runs_by_task.len() == other.runs_by_task.len()
            && self
                .runs_by_task
                .iter()
                .all(|(task, runs)| other.runs_by_task.get(task) == Some(runs))
    }
    pub(crate) fn projection_image(&self) -> Result<ProjectionImageV8, StoreError> {
        let snapshot =
            crate::snapshot::build::build_snapshot_from_projection(self, 0).map_err(invalid)?;
        Ok(ProjectionImageV8(snapshot))
    }
    pub fn projection_digest(&self) -> Result<ProjectionDigest, StoreError> {
        snapshot_digest(&self.projection_image()?.0)
    }
    pub(crate) fn from_projection_image(image: ProjectionImageV8) -> Result<Self, StoreError> {
        let s = image.0;
        let started = std::time::Instant::now();
        let signals = validate_snapshot_with_signals(&s).map_err(invalid)?;
        super::work::snapshot_phase(|p| {
            p.mapping_validation_seconds += started.elapsed().as_secs_f64()
        });
        let started = std::time::Instant::now();

        let mut r = Self::new();
        r.latest_sequence = s.metadata.wal_sequence;
        let (controls, tasks, runs, admissions) = std::thread::scope(|scope| {
            // Independent immutable sections build disjoint roots. Scoped workers
            // join before the projection becomes visible; no mutation lane exists here.
            let controls = scope.spawn(move || -> Result<_, StoreError> {
                let mut map = super::collections::BTreeMap::new();
                for (sequence, context) in s.control_history {
                    if sequence == 0
                        || sequence > s.metadata.wal_sequence
                        || map.insert(sequence, context).is_some()
                    {
                        return Err(invalid("invalid control history"));
                    }
                }
                Ok(map)
            });
            let tasks = scope.spawn(move || {
                let canceled = s
                    .tasks
                    .iter()
                    .filter_map(|t| t.canceled_at.map(|at| (t.task_spec.id(), at)))
                    .collect();
                let tasks = s
                    .tasks
                    .into_iter()
                    .map(|t| {
                        (
                            t.task_spec.id(),
                            TaskRecord {
                                task_spec: t.task_spec,
                                created_at: t.created_at,
                                updated_at: t.updated_at,
                                canceled_at: t.canceled_at,
                            },
                        )
                    })
                    .collect();
                (tasks, canceled)
            });
            let admissions = scope.spawn(move || {
                let mut index = super::inspection::InspectionIndex::default();
                index.hydrate_references(s.admissions.iter());
                let by_task = s
                    .admissions
                    .iter()
                    .map(|a| (a.task_id(), (a.tenant_id(), a.key().clone())))
                    .collect();
                let admissions = s
                    .admissions
                    .into_iter()
                    .map(|a| ((a.tenant_id(), a.key().clone()), a))
                    .collect();
                (admissions, by_task, index)
            });
            let runs = scope.spawn(move || {
                let mut part = Self::new();
                let mut runs = Vec::new();
                let mut by_task: std::collections::HashMap<_, Vec<_>> =
                    std::collections::HashMap::new();
                let mut histories = Vec::new();
                let mut attempts = Vec::new();
                let mut leases = Vec::new();
                let mut metadata = Vec::new();
                let mut instances = Vec::new();
                for run in s.runs {
                    let id = run.run_instance.id();
                    runs.push((id, run.run_instance.state()));
                    by_task.entry(run.run_instance.task_id()).or_default().push(id);
                    histories.push((id, map_snapshot_run_history(run.state_history).into()));
                    if !run.attempts.is_empty() {
                        attempts.push((id, map_snapshot_attempt_history(run.attempts).into()));
                    }
                    if let Some(l) = map_snapshot_lease_metadata(run.lease) {
                        leases.push((id, (l.owner.clone(), l.expiry)));
                        metadata.push((id, l));
                    }
                    instances.push((id, run.run_instance));
                }
                part.runs = runs.into_iter().collect();
                part.runs_by_task =
                    by_task.into_iter().map(|(id, runs)| (id, runs.into())).collect();
                part.run_history = histories.into_iter().collect();
                part.attempt_history = attempts.into_iter().collect();
                part.leases = leases.into_iter().collect();
                part.lease_metadata = metadata.into_iter().collect();
                part.run_instances = instances.into_iter().collect();
                part
            });
            let join_error = || invalid("checkpoint hydration worker panicked");
            Ok::<_, StoreError>((
                controls.join().map_err(|_| join_error())??,
                tasks.join().map_err(|_| join_error())?,
                runs.join().map_err(|_| join_error())?,
                admissions.join().map_err(|_| join_error())?,
            ))
        })?;
        r.control_history = controls;
        (r.tasks, r.task_canceled_at) = tasks;
        r.runs = runs.runs;
        r.runs_by_task = runs.runs_by_task;
        r.run_history = runs.run_history;
        r.attempt_history = runs.attempt_history;
        r.leases = runs.leases;
        r.lease_metadata = runs.lease_metadata;
        r.run_instances = runs.run_instances;
        (r.admissions, r.admission_by_task, r.inspection_index) = admissions;
        r.engine_paused = s.engine.paused;
        r.engine_paused_at = s.engine.paused_at;
        r.engine_resumed_at = s.engine.resumed_at;
        for d in s.dependency_declarations {
            r.dependency_declared_at.insert(d.task_id, d.declared_at);
            r.dependency_declarations.insert(d.task_id, d.depends_on.into_iter().collect());
        }
        for b in s.budgets {
            r.budgets.insert(
                (b.task_id, b.dimension),
                BudgetRecord {
                    dimension: b.dimension,
                    limit: b.limit,
                    consumed: b.consumed,
                    allocated_at: b.allocated_at,
                    exhausted: b.exhausted,
                },
            );
        }
        for sub in s.subscriptions {
            if sub.matched_sequence.is_some_and(|seq| seq == 0 || seq > r.latest_sequence) {
                return Err(invalid("invalid subscription match sequence"));
            }
            r.subscriptions.insert(
                sub.subscription_id,
                SubscriptionRecord {
                    matched_sequence: sub.matched_sequence,
                    subscription_id: sub.subscription_id,
                    task_id: sub.task_id,
                    filter: sub.filter,
                    created_at: sub.created_at,
                    triggered_at: sub.triggered_at,
                    canceled_at: sub.canceled_at,
                },
            );
        }
        for a in s.actors {
            r.actors.insert(
                a.actor_id,
                ActorRecord {
                    actor_id: a.actor_id,
                    identity: a.identity,
                    executor_traits: a.executor_traits,
                    department: a.department,
                    heartbeat_interval_secs: a.heartbeat_interval_secs,
                    tenant_id: a.tenant_id,
                    registered_at: a.registered_at,
                    last_heartbeat_at: a.last_heartbeat_at,
                    deregistered_at: a.deregistered_at,
                },
            );
        }
        for t in s.tenants {
            r.tenants.insert(
                t.tenant_id,
                TenantRecord { tenant_id: t.tenant_id, name: t.name, created_at: t.created_at },
            );
        }
        for role in s.role_assignments {
            r.role_assignments.insert(
                (role.actor_id, role.tenant_id),
                RoleAssignmentRecord {
                    actor_id: role.actor_id,
                    role: role.role,
                    tenant_id: role.tenant_id,
                    assigned_at: role.assigned_at,
                },
            );
        }
        for c in s.capability_grants {
            r.capability_grants.insert(
                (c.actor_id, super::reducer::capability_key(&c.capability), c.tenant_id),
                CapabilityGrantRecord {
                    actor_id: c.actor_id,
                    capability: c.capability,
                    tenant_id: c.tenant_id,
                    granted_at: c.granted_at,
                    revoked_at: c.revoked_at,
                },
            );
        }
        for entry in s.ledger_entries {
            r.ledger_entries.push(LedgerEntryRecord {
                entry_id: entry.entry_id,
                tenant_id: entry.tenant_id,
                ledger_key: entry.ledger_key,
                actor_id: entry.actor_id,
                payload: entry.payload,
                timestamp: entry.timestamp,
            });
        }
        r.signals = signals;
        for record in &s.signals {
            r.signals
                .account_control(record, r.control_history.get(&record.wal_sequence()))
                .map_err(invalid)?;
        }
        for record in r.signals.records() {
            r.validate_signal_references(record.envelope()).map_err(invalid)?;
        }
        super::work::snapshot_phase(|p| {
            p.object_hydration_seconds += started.elapsed().as_secs_f64()
        });
        let started = std::time::Instant::now();
        r.rebuild_derived();
        r.hydrate_waits(&s.waits, &s.pending_resumes, &s.key_reservations, &s.cancellations)
            .map_err(invalid)?;
        for (run, seq) in s.dispatch_sequences {
            if seq == 0
                || seq > r.latest_sequence
                || !r.runs.contains_key(&run)
                || r.dispatch_sequences.insert(run, seq).is_some()
            {
                return Err(invalid("invalid dispatch sequence"));
            }
        }
        r.hydrate_inspection_ownership();
        let produced: Vec<_> = r
            .attempt_history
            .values()
            .flatten()
            .filter_map(|a| a.disposition.as_deref())
            .filter(|d| d.disposition.wait().is_none())
            .filter_map(|d| {
                d.disposition.checkpoint().map(|c| (d.run_id, d.attempt_id, d.sequence, c.clone()))
            })
            .collect();
        for (run, attempt, sequence, checkpoint) in produced {
            r.index_produced_checkpoint(run, attempt, sequence, &checkpoint).map_err(invalid)?;
        }
        r.validate_disposition_history().map_err(invalid)?;
        r.hydrate_resume(&s.administrative_wakes, &s.administrative_pending).map_err(invalid)?;
        super::work::snapshot_phase(|p| {
            p.index_construction_seconds += started.elapsed().as_secs_f64()
        });
        Ok(r)
    }
}
