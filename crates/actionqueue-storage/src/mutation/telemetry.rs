//! Process-lifetime observations at the mutation authority boundary. Never replayed as counters.
use crate::wal::event::{WalEvent, WalEventType as E};
use actionqueue_core::{continuation::*, ids::WaitId};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, Mutex},
};
#[derive(Debug, Clone, Default)]
pub struct QueueTelemetry(Arc<Mutex<Observations>>);
#[derive(Debug, Clone, Default)]
pub struct Observations {
    pub admissions_created: u64,
    pub admission_duplicates: u64,
    pub admission_conflicts: u64,
    pub signal_duplicates: u64,
    pub signals: BTreeMap<(String, String), u64>,
    pub waits_satisfied: BTreeMap<&'static str, u64>,
    pub wait_latency_count: u64,
    pub wait_latency_sum: u64,
    pub disposition_rejected: u64,
    pub recovery_reconciliations: u64,
    pub compound_bytes_count: u64,
    pub compound_bytes_sum: u64,
    pub projection_mismatches: u64,
    allowlist: BTreeSet<(String, String)>,
    waits: HashMap<WaitId, u64>,
}
impl QueueTelemetry {
    pub fn snapshot(&self) -> Observations {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    /// Finite host configuration; unknown pairs share one bucket. Maximum 64 pairs.
    pub fn set_signal_allowlist(
        &self,
        pairs: BTreeSet<(String, String)>,
    ) -> Result<(), &'static str> {
        if pairs.len() > 64
            || pairs
                .iter()
                .any(|(ns, k)| SignalNamespace::new(ns).is_err() || SignalKind::new(k).is_err())
        {
            return Err("invalid metric allowlist");
        }
        let mut o = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if !o.signals.is_empty() {
            return Err("telemetry already active");
        }
        o.allowlist = pairs;
        Ok(())
    }
    pub fn admission_lookup(&self, duplicate: bool, conflict: bool) {
        let mut o = self.0.lock().unwrap_or_else(|e| e.into_inner());
        o.admission_duplicates += u64::from(duplicate);
        o.admission_conflicts += u64::from(conflict);
    }
    pub fn signal_duplicate(&self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).signal_duplicates += 1;
    }
    pub fn disposition_rejected(&self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).disposition_rejected += 1;
    }
    pub fn projection_mismatch(&self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).projection_mismatches += 1;
    }
    pub fn committed(&self, e: &WalEvent, bytes: usize) {
        let mut o = self.0.lock().unwrap_or_else(|e| e.into_inner());
        match e.event() {
            E::AdmissionCommitted { .. } => o.admissions_created += 1,
            E::SignalAdmitted { record } => signal(&mut o, record),
            E::AttemptDispositionCommitted { record } => {
                o.admissions_created += record.children.len() as u64;
                for s in &record.signals {
                    signal(&mut o, s);
                }
                if let Some(w) = record.wait_record() {
                    o.waits.insert(w.spec.wait_id(), w.timestamp);
                }
                o.compound_bytes_count += 1;
                o.compound_bytes_sum += bytes as u64;
            }
            E::WaitEstablished { record } => {
                o.waits.insert(record.spec.wait_id(), record.timestamp);
            }
            E::WaitSatisfied { record }
            | E::WaitTimedOut { record }
            | E::WaitCanceled { record } => {
                use crate::mutation::wait::WaitResolutionKind as K;
                let reason = match record.kind {
                    K::Signal(_) => "signal",
                    K::Children(_) => "children",
                    K::Deadline => "deadline",
                    K::Control(_) => "control",
                    K::Canceled(_) => "canceled",
                };
                *o.waits_satisfied.entry(reason).or_default() += 1;
                if let Some(start) = o.waits.remove(&record.wait_id) {
                    o.wait_latency_count += 1;
                    o.wait_latency_sum += record.timestamp.saturating_sub(start);
                }
            }
            E::AttemptClosed { record } if record.origin == AttemptFinishOrigin::Recovery => {
                o.recovery_reconciliations += 1
            }
            _ => {}
        }
    }
}
fn signal(o: &mut Observations, r: &crate::mutation::signal::SignalRecord) {
    let e = r.envelope();
    let pair = (e.namespace.as_str().to_owned(), e.kind.as_str().to_owned());
    let key =
        if o.allowlist.contains(&pair) { pair } else { ("overflow".into(), "overflow".into()) };
    *o.signals.entry(key).or_default() += 1;
}
