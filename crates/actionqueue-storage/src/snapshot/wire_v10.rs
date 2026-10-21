//! Schema 10: ordered, length-delimited postcard records. Field order and numeric tags
//! are durable. Frozen WAL DTOs are reused rather than serializing runtime shapes.
//! Tagged continuation sections use the explicitly retained logical-v9 JSON subcodec.
use super::model::*;
use crate::{
    recovery::inspection::InspectionImage,
    wal::{
        admission_v3::AdmissionRecordV3, checkpoint::WalCursor, signal_v1::ControlV1,
        task_v3::TaskSpecV3, wait_v1::CancelV1,
    },
};
use actionqueue_core::{
    control::*,
    ids::*,
    run::{RunInstance, RunState},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

const MAX_RECORDS: usize = 1_000_000;

fn check_count(n: usize) -> Result<(), String> {
    if n > MAX_RECORDS {
        return Err("checkpoint collection bound exceeded".into());
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Binding {
    pub store: [u8; 16],
    pub manifest: [u8; 32],
    pub commitment_version: u8,
    pub cursor: WalCursor,
}
pub(crate) struct Checkpoint {
    pub binding: Binding,
    pub snapshot: Snapshot,
    pub inspection: InspectionImage,
}
#[derive(Serialize, Deserialize)]
struct Header {
    binding: Binding,
    timestamp: u64,
}
#[derive(Serialize, Deserialize)]
struct Task {
    spec: TaskSpecV3,
    created: u64,
    updated: Option<u64>,
    canceled: Option<u64>,
}
#[derive(Serialize, Deserialize)]
struct Control {
    sequence: u64,
    actor: Option<[u8; 16]>,
    scope: u8,
    tenant: Option<[u8; 16]>,
    context: ControlV1,
}
#[derive(Serialize, Deserialize)]
struct Run {
    id: [u8; 16],
    task: u32,
    state: u8,
    active: Option<[u8; 16]>,
    starts: u32,
    failures: u32,
    created: u64,
    scheduled: u64,
    priority: i32,
    changed: u64,
    #[serde(deserialize_with = "bounded_records")]
    history: Vec<(Option<u8>, u8, u64)>,
    // JSON subcodec v9 handles tagged dispositions/assignments; empty is zero bytes.
    attempts_v9: Vec<u8>,
    lease: Option<(u64, String, u64, u64, u64)>,
}
// Conversion adapter only: it is not the durable schema. Core's deserializer owns
// identifier and state/attempt consistency validation.
#[derive(Serialize)]
struct RestoreRun {
    id: RunId,
    task_id: TaskId,
    state: RunState,
    current_attempt_id: Option<AttemptId>,
    attempt_count: u32,
    failure_attempt_count: u32,
    created_at: u64,
    scheduled_at: u64,
    effective_priority: i32,
    last_state_change_at: u64,
}
fn tag(s: RunState) -> u8 {
    match s {
        RunState::Scheduled => 0,
        RunState::Ready => 1,
        RunState::Leased => 2,
        RunState::Running => 3,
        RunState::RetryWait => 4,
        RunState::Completed => 5,
        RunState::Failed => 6,
        RunState::Canceled => 7,
        RunState::Suspended => 8,
        RunState::Awaiting => 9,
    }
}
fn state(t: u8) -> Result<RunState, String> {
    Ok(match t {
        0 => RunState::Scheduled,
        1 => RunState::Ready,
        2 => RunState::Leased,
        3 => RunState::Running,
        4 => RunState::RetryWait,
        5 => RunState::Completed,
        6 => RunState::Failed,
        7 => RunState::Canceled,
        8 => RunState::Suspended,
        9 => RunState::Awaiting,
        _ => return Err("invalid run state tag".into()),
    })
}
fn bounded_records<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Vec<T>, D::Error> {
    struct Items<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Items<T> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("bounded checkpoint records")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut a: A) -> Result<Vec<T>, A::Error> {
            if a.size_hint().is_some_and(|n| n > MAX_RECORDS) {
                return Err(serde::de::Error::custom("checkpoint collection bound exceeded"));
            }
            let mut out = Vec::with_capacity(a.size_hint().unwrap_or(0).min(4096));
            while let Some(v) = a.next_element()? {
                if out.len() == MAX_RECORDS {
                    return Err(serde::de::Error::custom("checkpoint collection bound exceeded"));
                }
                out.push(v);
            }
            Ok(out)
        }
    }
    d.deserialize_seq(Items(std::marker::PhantomData))
}
#[derive(Serialize, Deserialize)]
struct ControlAssociation {
    tag: u8,
    id: [u8; 16],
    signal: u64,
    #[serde(deserialize_with = "bounded_records")]
    sequences: Vec<u64>,
}
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn record<T: Serialize>(out: &mut Vec<u8>, value: &T) -> Result<(), String> {
    let bytes = postcard::to_allocvec(value).map_err(err)?;
    if bytes.len() > super::envelope::MAX_SNAPSHOT_BYTES {
        return Err("checkpoint record size bound exceeded".into());
    }
    if out
        .len()
        .checked_add(4 + bytes.len())
        .is_none_or(|n| n > super::envelope::MAX_SNAPSHOT_BYTES - 52)
    {
        return Err("complete checkpoint exceeds maximum size".into());
    }
    out.extend_from_slice(&u32::try_from(bytes.len()).map_err(err)?.to_le_bytes());
    out.extend(bytes);
    Ok(())
}
fn count(out: &mut Vec<u8>, n: usize) -> Result<(), String> {
    check_count(n)?;
    if out.len() > super::envelope::MAX_SNAPSHOT_BYTES - 56 {
        return Err("complete checkpoint exceeds maximum size".into());
    }
    out.extend_from_slice(&u32::try_from(n).map_err(err)?.to_le_bytes());
    Ok(())
}
struct Input<'a>(&'a [u8]);
impl<'a> Input<'a> {
    fn number(&mut self) -> Result<usize, String> {
        let b = self.0.get(..4).ok_or("truncated checkpoint length")?;
        let n = u32::from_le_bytes(b.try_into().unwrap()) as usize;
        self.0 = &self.0[4..];
        Ok(n)
    }
    fn count(&mut self) -> Result<usize, String> {
        let n = self.number()?;
        if n > MAX_RECORDS || n > self.0.len() / 4 {
            return Err("checkpoint collection bound exceeded".into());
        }
        Ok(n)
    }
    fn slice(&mut self) -> Result<&'a [u8], String> {
        let n = self.number()?;
        let bytes = self.0.get(..n).ok_or("checkpoint record length exceeds remaining bytes")?;
        self.0 = &self.0[n..];
        Ok(bytes)
    }
    fn record<T: DeserializeOwned>(&mut self) -> Result<T, String> {
        decode_record(self.slice()?)
    }
    fn records<T: DeserializeOwned + Send, U: Send>(
        &mut self,
        map: impl Fn(T) -> Result<U, String> + Sync,
    ) -> Result<Vec<U>, String> {
        let n = self.count()?;
        // Minimum grain avoids worker overhead on small sections; concurrency
        // stays bounded regardless of snapshot size. All offsets are checked first.
        let workers = n
            .div_ceil(32_768)
            .min(4)
            .min(std::thread::available_parallelism().map_or(1, usize::from))
            .max(1);
        if workers == 1 {
            return (0..n).map(|_| map(self.record()?)).collect();
        }
        let mut records = Vec::with_capacity(n);
        for _ in 0..n {
            records.push(self.slice()?);
        }
        std::thread::scope(|scope| {
            let map = &map;
            let jobs: Vec<_> = records
                .chunks(n.div_ceil(workers))
                .map(|chunk| {
                    scope.spawn(move || {
                        chunk
                            .iter()
                            .map(|bytes| map(decode_record(bytes)?))
                            .collect::<Result<Vec<U>, String>>()
                    })
                })
                .collect();
            let mut out = Vec::with_capacity(n);
            for job in jobs {
                out.append(&mut job.join().map_err(|_| "checkpoint decoder worker panicked")??);
            }
            Ok(out)
        })
    }
}
fn decode_record<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let (value, rest) = postcard::take_from_bytes(bytes).map_err(err)?;
    if !rest.is_empty() {
        return Err("trailing checkpoint record bytes".into());
    }
    Ok(value)
}
pub(crate) type EncodedCheckpoint = (Vec<u8>, Vec<(String, u64)>);
pub(crate) fn encode_measured(c: &Checkpoint) -> Result<EncodedCheckpoint, String> {
    let s = &c.snapshot;
    let mut out = Vec::new();
    let mut sections = Vec::new();
    let mut cut = 0;
    record(&mut out, &Header { binding: c.binding.clone(), timestamp: s.timestamp })?;
    sections.push(("header".into(), (out.len() - cut) as u64));
    cut = out.len();
    count(&mut out, s.tasks.len())?;
    let mut tasks = std::collections::HashMap::new();
    for (i, t) in s.tasks.iter().enumerate() {
        tasks.insert(t.task_spec.id(), i as u32);
        record(
            &mut out,
            &Task {
                spec: (&t.task_spec).into(),
                created: t.created_at,
                updated: t.updated_at,
                canceled: t.canceled_at,
            },
        )?;
    }
    sections.push(("tasks".into(), (out.len() - cut) as u64));
    cut = out.len();
    count(&mut out, s.admissions.len())?;
    for a in &s.admissions {
        record(&mut out, &AdmissionRecordV3::from(a.clone()))?;
    }
    sections.push(("admissions".into(), (out.len() - cut) as u64));
    cut = out.len();
    count(&mut out, s.runs.len())?;
    for r in &s.runs {
        check_count(r.state_history.len())?;
        let v = &r.run_instance;
        record(
            &mut out,
            &Run {
                id: *v.id().as_uuid().as_bytes(),
                task: *tasks.get(&v.task_id()).ok_or("orphan run")?,
                state: tag(v.state()),
                active: v.current_attempt_id().map(|a| *a.as_uuid().as_bytes()),
                starts: v.attempt_count(),
                failures: v.failure_attempt_count(),
                created: v.created_at(),
                scheduled: v.scheduled_at(),
                priority: v.effective_priority(),
                changed: v.last_state_change_at(),
                history: r
                    .state_history
                    .iter()
                    .map(|h| (h.from.map(tag), tag(h.to), h.timestamp))
                    .collect(),
                attempts_v9: if r.attempts.is_empty() {
                    vec![]
                } else {
                    serde_json::to_vec(&r.attempts).map_err(err)?
                },
                lease: r.lease.as_ref().map(|l| {
                    (l.granted_at_sequence, l.owner.clone(), l.expiry, l.acquired_at, l.updated_at)
                }),
            },
        )?;
    }
    sections.push(("runs".into(), (out.len() - cut) as u64));
    cut = out.len();
    count(&mut out, s.control_history.len())?;
    for (seq, c) in &s.control_history {
        let (scope, tenant) = match c.scope {
            ControlScope::Store => (0, None),
            ControlScope::SingleTenant => (1, None),
            ControlScope::Tenant(t) => (2, Some(*t.as_uuid().as_bytes())),
            ControlScope::ProvisionTenant(t) => (3, Some(*t.as_uuid().as_bytes())),
        };
        record(
            &mut out,
            &Control {
                sequence: *seq,
                actor: c.actor_id.map(|a| *a.as_uuid().as_bytes()),
                scope,
                tenant,
                context: (&c.context).into(),
            },
        )?;
    }
    sections.push(("controls".into(), (out.len() - cut) as u64));
    cut = out.len();
    count(&mut out, s.cancellations.len())?;
    for c in &s.cancellations {
        record(&mut out, &CancelV1::from(c.clone()))?;
    }
    sections.push(("cancellations".into(), (out.len() - cut) as u64));
    cut = out.len();
    record(
        &mut out,
        &(s.engine.paused, s.engine.paused_at, s.engine.resumed_at, s.last_signal_sequence),
    )?;
    count(&mut out, s.dispatch_sequences.len())?;
    for v in &s.dispatch_sequences {
        record(&mut out, &serde_json::to_vec(v).map_err(err)?)?;
    }
    count(&mut out, s.administrative_wakes.len())?;
    for v in &s.administrative_wakes {
        record(&mut out, &serde_json::to_vec(v).map_err(err)?)?;
    }
    count(&mut out, s.administrative_pending.len())?;
    for v in &s.administrative_pending {
        record(&mut out, &serde_json::to_vec(v).map_err(err)?)?;
    }
    count(&mut out, s.waits.len())?;
    for v in &s.waits {
        record(&mut out, &serde_json::to_vec(v).map_err(err)?)?;
    }
    count(&mut out, s.pending_resumes.len())?;
    for v in &s.pending_resumes {
        record(&mut out, &serde_json::to_vec(v).map_err(err)?)?;
    }
    count(&mut out, s.key_reservations.len())?;
    for v in &s.key_reservations {
        record(&mut out, &serde_json::to_vec(v).map_err(err)?)?;
    }
    count(&mut out, s.signals.len())?;
    for v in &s.signals {
        record(&mut out, &serde_json::to_vec(v).map_err(err)?)?;
    }
    count(&mut out, s.dependency_declarations.len())?;
    for v in &s.dependency_declarations {
        record(&mut out, &serde_json::to_vec(v).map_err(err)?)?;
    }
    count(&mut out, s.budgets.len())?;
    for v in &s.budgets {
        record(&mut out, &serde_json::to_vec(v).map_err(err)?)?;
    }
    count(&mut out, s.subscriptions.len())?;
    for v in &s.subscriptions {
        record(&mut out, &serde_json::to_vec(v).map_err(err)?)?;
    }
    count(&mut out, s.actors.len())?;
    for v in &s.actors {
        record(&mut out, &serde_json::to_vec(v).map_err(err)?)?;
    }
    count(&mut out, s.tenants.len())?;
    for v in &s.tenants {
        record(&mut out, &serde_json::to_vec(v).map_err(err)?)?;
    }
    count(&mut out, s.role_assignments.len())?;
    for v in &s.role_assignments {
        record(&mut out, &serde_json::to_vec(v).map_err(err)?)?;
    }
    count(&mut out, s.capability_grants.len())?;
    for v in &s.capability_grants {
        record(&mut out, &serde_json::to_vec(v).map_err(err)?)?;
    }
    count(&mut out, s.ledger_entries.len())?;
    for v in &s.ledger_entries {
        record(&mut out, &serde_json::to_vec(v).map_err(err)?)?;
    }
    sections.push(("projection_sections".into(), (out.len() - cut) as u64));
    cut = out.len();
    count(&mut out, c.inspection.controls.len())?;
    for v in &c.inspection.controls {
        check_count(v.3.len())?;
        record(&mut out, v)?;
    }
    sections.push(("control_targets".into(), (out.len() - cut) as u64));
    cut = out.len();
    count(&mut out, c.inspection.attempts.len())?;
    for v in &c.inspection.attempts {
        record(&mut out, v)?;
    }
    sections.push(("attempt_owners".into(), (out.len() - cut) as u64));
    Ok((out, sections))
}
pub(crate) fn decode(bytes: &[u8]) -> Result<Checkpoint, String> {
    let mut input = Input(bytes);
    let h: Header = input.record()?;
    if h.binding.commitment_version != 1 {
        return Err("unsupported checkpoint commitment version".into());
    }
    let tasks = input.records(|t: Task| {
        Ok(SnapshotTask {
            task_spec: t.spec.try_into().map_err(err)?,
            created_at: t.created,
            updated_at: t.updated,
            canceled_at: t.canceled,
        })
    })?;
    let admissions = input.records(|a: AdmissionRecordV3| a.try_into().map_err(err))?;
    let runs = input.records(|r: Run| {
        let task = tasks.get(r.task as usize).ok_or("invalid task ordinal")?.task_spec.id();
        let restore = RestoreRun {
            id: RunId::from_uuid(uuid::Uuid::from_bytes(r.id)),
            task_id: task,
            state: state(r.state)?,
            current_attempt_id: r.active.map(|a| AttemptId::from_uuid(uuid::Uuid::from_bytes(a))),
            attempt_count: r.starts,
            failure_attempt_count: r.failures,
            created_at: r.created,
            scheduled_at: r.scheduled,
            effective_priority: r.priority,
            last_state_change_at: r.changed,
        };
        let instance: RunInstance =
            postcard::from_bytes(&postcard::to_allocvec(&restore).map_err(err)?).map_err(err)?;
        let history = r
            .history
            .into_iter()
            .map(|(from, to, timestamp)| {
                Ok(SnapshotRunStateHistoryEntry {
                    from: from.map(state).transpose()?,
                    to: state(to)?,
                    timestamp,
                })
            })
            .collect::<Result<_, String>>()?;
        Ok(SnapshotRun {
            run_instance: instance,
            state_history: history,
            attempts: if r.attempts_v9.is_empty() {
                vec![]
            } else {
                serde_json::from_slice(&r.attempts_v9).map_err(err)?
            },
            lease: r.lease.map(|(granted_at_sequence, owner, expiry, acquired_at, updated_at)| {
                SnapshotLeaseMetadata {
                    granted_at_sequence,
                    owner,
                    expiry,
                    acquired_at,
                    updated_at,
                }
            }),
        })
    })?;
    let control_history = input.records(|c: Control| {
        let tenant = c.tenant.map(|t| TenantId::from_uuid(uuid::Uuid::from_bytes(t)));
        let scope = match (c.scope, tenant) {
            (0, None) => ControlScope::Store,
            (1, None) => ControlScope::SingleTenant,
            (2, Some(t)) if !t.as_uuid().is_nil() => ControlScope::Tenant(t),
            (3, Some(t)) if !t.as_uuid().is_nil() => ControlScope::ProvisionTenant(t),
            _ => return Err("invalid control scope tag".into()),
        };
        Ok((
            c.sequence,
            ControlAttribution {
                actor_id: c.actor.map(|a| ActorId::from_uuid(uuid::Uuid::from_bytes(a))),
                scope,
                context: c.context.try_into().map_err(err)?,
            },
        ))
    })?;
    let cancellations = input.records(|c: CancelV1| c.try_into().map_err(err))?;
    let (paused, paused_at, resumed_at, last_signal_sequence) = input.record()?;
    let mut dispatch_sequences = Vec::new();
    for _ in 0..input.count()? {
        let bytes: Vec<u8> = input.record()?;
        dispatch_sequences.push(serde_json::from_slice(&bytes).map_err(err)?);
    }
    let mut administrative_wakes = Vec::new();
    for _ in 0..input.count()? {
        let bytes: Vec<u8> = input.record()?;
        administrative_wakes.push(serde_json::from_slice(&bytes).map_err(err)?);
    }
    let mut administrative_pending = Vec::new();
    for _ in 0..input.count()? {
        let bytes: Vec<u8> = input.record()?;
        administrative_pending.push(serde_json::from_slice(&bytes).map_err(err)?);
    }
    let mut waits = Vec::new();
    for _ in 0..input.count()? {
        let bytes: Vec<u8> = input.record()?;
        waits.push(serde_json::from_slice(&bytes).map_err(err)?);
    }
    let mut pending_resumes = Vec::new();
    for _ in 0..input.count()? {
        let bytes: Vec<u8> = input.record()?;
        pending_resumes.push(serde_json::from_slice(&bytes).map_err(err)?);
    }
    let mut key_reservations = Vec::new();
    for _ in 0..input.count()? {
        let bytes: Vec<u8> = input.record()?;
        key_reservations.push(serde_json::from_slice(&bytes).map_err(err)?);
    }
    let mut signals = Vec::new();
    for _ in 0..input.count()? {
        let bytes: Vec<u8> = input.record()?;
        signals.push(serde_json::from_slice(&bytes).map_err(err)?);
    }
    let mut dependency_declarations = Vec::new();
    for _ in 0..input.count()? {
        let bytes: Vec<u8> = input.record()?;
        dependency_declarations.push(serde_json::from_slice(&bytes).map_err(err)?);
    }
    let mut budgets = Vec::new();
    for _ in 0..input.count()? {
        let bytes: Vec<u8> = input.record()?;
        budgets.push(serde_json::from_slice(&bytes).map_err(err)?);
    }
    let mut subscriptions = Vec::new();
    for _ in 0..input.count()? {
        let bytes: Vec<u8> = input.record()?;
        subscriptions.push(serde_json::from_slice(&bytes).map_err(err)?);
    }
    let mut actors = Vec::new();
    for _ in 0..input.count()? {
        let bytes: Vec<u8> = input.record()?;
        actors.push(serde_json::from_slice(&bytes).map_err(err)?);
    }
    let mut tenants = Vec::new();
    for _ in 0..input.count()? {
        let bytes: Vec<u8> = input.record()?;
        tenants.push(serde_json::from_slice(&bytes).map_err(err)?);
    }
    let mut role_assignments = Vec::new();
    for _ in 0..input.count()? {
        let bytes: Vec<u8> = input.record()?;
        role_assignments.push(serde_json::from_slice(&bytes).map_err(err)?);
    }
    let mut capability_grants = Vec::new();
    for _ in 0..input.count()? {
        let bytes: Vec<u8> = input.record()?;
        capability_grants.push(serde_json::from_slice(&bytes).map_err(err)?);
    }
    let mut ledger_entries = Vec::new();
    for _ in 0..input.count()? {
        let bytes: Vec<u8> = input.record()?;
        ledger_entries.push(serde_json::from_slice(&bytes).map_err(err)?);
    }
    let mut controls = Vec::new();
    for _ in 0..input.count()? {
        let c: ControlAssociation = input.record()?;
        controls.push((c.tag, c.id, c.signal, c.sequences));
    }
    let mut attempts = Vec::new();
    for _ in 0..input.count()? {
        attempts.push(input.record()?);
    }
    if !input.0.is_empty() {
        return Err("trailing checkpoint bytes".into());
    }
    let metadata = SnapshotMetadata {
        schema_version: 9,
        wal_sequence: h.binding.cursor.sequence,
        task_count: tasks.len() as u64,
        run_count: runs.len() as u64,
    };
    let snapshot = Snapshot {
        version: 9,
        timestamp: h.timestamp,
        metadata,
        tasks,
        runs,
        admissions,
        control_history,
        cancellations,
        engine: SnapshotEngineControl { paused, paused_at, resumed_at },
        last_signal_sequence,
        dispatch_sequences,
        administrative_wakes,
        administrative_pending,
        waits,
        pending_resumes,
        key_reservations,
        signals,
        dependency_declarations,
        budgets,
        subscriptions,
        actors,
        tenants,
        role_assignments,
        capability_grants,
        ledger_entries,
    };
    Ok(Checkpoint {
        binding: h.binding,
        snapshot,
        inspection: InspectionImage { controls, attempts },
    })
}
