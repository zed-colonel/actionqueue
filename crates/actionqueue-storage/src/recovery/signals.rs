//! Derived signal indexes. Durable records remain resident after logical retirement.
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound::{Excluded, Unbounded};

use actionqueue_core::{bounded::OpaqueRef, continuation::*, ids::*, limits::*};

use crate::mutation::signal::*;
type Identity = (Option<TenantId>, SignalId);
type MatchKey =
    (Option<TenantId>, SignalNamespace, SignalKind, Option<CorrelationId>, Option<OpaqueRef>);
/// Resident counters. Capacity rejections are live authority telemetry, not durable facts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SignalStatistics {
    pub retained: usize,
    pub retired: usize,
    pub pinned: usize,
    pub pins: usize,
    pub bytes: usize,
    pub capacity_rejections: u64,
}
/// Private secondary indexes rebuilt from durable records during hydration.
#[derive(Debug, Clone, Default)]
pub struct SignalIndex {
    records: BTreeMap<SignalSequence, SignalRecord>,
    identities: HashMap<Identity, SignalSequence>,
    tenants: HashMap<Option<TenantId>, BTreeSet<SignalSequence>>,
    matching: HashMap<MatchKey, BTreeSet<SignalSequence>>,
    receipts: BTreeSet<(u64, SignalSequence)>,
    last_sequence: u64,
    stats: SignalStatistics,
}
fn keys(e: &SignalEnvelope) -> HashSet<MatchKey> {
    let mut keys = HashSet::new();
    for correlation in [None, e.correlation_id.clone()] {
        for source in [None, e.source_ref.clone()] {
            keys.insert((
                e.tenant_id,
                e.namespace.clone(),
                e.kind.clone(),
                correlation.clone(),
                source,
            ));
        }
    }
    keys
}
impl SignalIndex {
    /// Signal high-water mark, including retired records.
    pub fn last_sequence(&self) -> SignalSequence {
        SignalSequence::new(self.last_sequence)
    }
    /// Scoped immutable identity lookup.
    pub fn get_signal(&self, tenant: Option<TenantId>, id: &SignalId) -> Option<&SignalRecord> {
        self.identities.get(&(tenant, id.clone())).and_then(|s| self.records.get(s))
    }
    /// Global sequence lookup for structural retention planning.
    pub fn by_sequence(&self, sequence: SignalSequence) -> Option<&SignalRecord> {
        self.records.get(&sequence)
    }
    /// Bounded tenant-scoped inspection; cursor is exclusive. Includes retired records.
    pub fn list_signals(
        &self,
        tenant: Option<TenantId>,
        after: SignalSequence,
        limit: usize,
    ) -> Vec<&SignalRecord> {
        self.tenants
            .get(&tenant)
            .into_iter()
            .flat_map(|set| set.range((Excluded(after), Unbounded)))
            .take(limit.min(MAX_SIGNAL_BATCH))
            .map(|s| &self.records[s])
            .collect()
    }
    /// Retained, non-consuming candidates in ascending sequence order. Optional filter
    /// fields select their own index, avoiding scans through unrelated correlations/sources.
    pub fn retained_candidates(
        &self,
        filter: &SignalFilter,
        after: SignalSequence,
        limit: usize,
    ) -> Vec<&SignalRecord> {
        let key = (
            filter.tenant_id,
            filter.namespace.clone(),
            filter.kind.clone(),
            filter.correlation_id.clone(),
            filter.source_ref.clone(),
        );
        self.matching
            .get(&key)
            .into_iter()
            .flat_map(|set| set.range((Excluded(after), Unbounded)))
            .take(limit.min(MAX_SIGNAL_BATCH))
            .map(|s| &self.records[s])
            .collect()
    }
    /// Explicit retirement planning is read-only. Commit must repeat protection checks.
    pub fn retirement_candidates(
        &self,
        tenant: Option<TenantId>,
        policy: SignalRetentionPolicy,
        now: u64,
        limit: usize,
    ) -> Vec<SignalSequence> {
        let mut result: Vec<_> = self
            .receipts
            .iter()
            .take_while(|(at, _)| *at < now)
            .filter_map(|(_, s)| {
                let r = &self.records[s];
                (r.envelope.tenant_id == tenant
                    && policy.permits(
                        SignalRetentionCandidate {
                            received_at: r.envelope.received_at,
                            sequence: s.get(),
                            protected: self.is_protected(r),
                        },
                        self.last_sequence,
                        now,
                    ))
                .then_some(*s)
            })
            .take(limit.min(MAX_SIGNAL_BATCH))
            .collect();
        result.sort_unstable();
        result
    }
    /// Protection seam for AQ-06. Durable wait/resume/history protections must be
    /// added here before waits are enabled; they must not be removable by unpin.
    pub(crate) fn is_protected(&self, record: &SignalRecord) -> bool {
        !record.pins.is_empty()
    }
    /// Counts include retired identities and immutable encoded bytes.
    pub fn statistics(&self) -> SignalStatistics {
        self.stats
    }
    pub(crate) fn records(&self) -> impl Iterator<Item = &SignalRecord> {
        self.records.values()
    }
    pub(crate) fn insert(&mut self, r: SignalRecord) -> Result<(), SignalRejection> {
        if r.sequence.get()
            != self.last_sequence.checked_add(1).ok_or(SignalRejection::SequenceExhausted)?
            || self.identities.contains_key(&(r.envelope.tenant_id, r.envelope.signal_id.clone()))
        {
            return Err(SignalRejection::InvalidEnvelope);
        }
        let bytes = r.encoded_bytes()?;
        if bytes > MAX_SIGNAL_RECORD_BYTES {
            return Err(SignalRejection::TooLarge);
        }
        self.stats.bytes = self.stats.bytes.checked_add(bytes).ok_or(SignalRejection::Capacity)?;
        self.last_sequence = r.sequence.get();
        self.identities.insert((r.envelope.tenant_id, r.envelope.signal_id.clone()), r.sequence);
        self.tenants.entry(r.envelope.tenant_id).or_default().insert(r.sequence);
        self.stats.pins += r.pins.len();
        self.stats.pinned += usize::from(!r.pins.is_empty());
        if r.is_retained() {
            self.stats.retained += 1;
            self.receipts.insert((r.envelope.received_at, r.sequence));
            for key in keys(&r.envelope) {
                self.matching.entry(key).or_default().insert(r.sequence);
            }
        } else {
            self.stats.retired += 1;
        }
        self.records.insert(r.sequence, r);
        Ok(())
    }
    pub(crate) fn apply_pin(
        &mut self,
        record: &SignalPinRecord,
        acquire: bool,
    ) -> Result<(), SignalRejection> {
        let r = self
            .get_signal(record.tenant_id, &record.signal_id)
            .ok_or(SignalRejection::NotFound)?;
        if !r.is_retained() {
            return Err(SignalRejection::Retired);
        }
        if record.control.wal_sequence <= r.wal_sequence
            || r.pins.contains_key(&record.pin_id) == acquire
        {
            return Err(SignalRejection::InvalidEnvelope);
        }
        if acquire && r.pins.len() >= MAX_SIGNAL_PINS {
            return Err(SignalRejection::Capacity);
        }
        if !acquire && record.control.wal_sequence <= r.pins[&record.pin_id].wal_sequence {
            return Err(SignalRejection::InvalidEnvelope);
        }
        let sequence = r.sequence;
        let r = self.records.get_mut(&sequence).expect("indexed");
        if acquire {
            self.stats.pinned += usize::from(r.pins.is_empty());
            self.stats.pins += 1;
            r.pins.insert(record.pin_id.clone(), record.control.clone());
        } else {
            r.pins.remove(&record.pin_id);
            self.stats.pins -= 1;
            self.stats.pinned -= usize::from(r.pins.is_empty());
        }
        Ok(())
    }
    pub(crate) fn apply_retirement(
        &mut self,
        record: &SignalsRetiredRecord,
    ) -> Result<(), SignalRejection> {
        if record.sequences.is_empty()
            || record.sequences.len() > MAX_SIGNAL_BATCH
            || record.sequences.windows(2).any(|w| w[0] >= w[1])
        {
            return Err(SignalRejection::InvalidEnvelope);
        }
        // Validate the whole batch before changing any index. Recovery trusts durable
        // selection, never today's clock or policy, but still checks protection/causality.
        for s in &record.sequences {
            let r = self.records.get(s).ok_or(SignalRejection::NotFound)?;
            if r.envelope.tenant_id != record.tenant_id {
                return Err(SignalRejection::TenantMismatch);
            }
            if !r.is_retained() || self.is_protected(r) {
                return Err(SignalRejection::Protected);
            }
            if record.control.wal_sequence <= r.wal_sequence
                || record.control.timestamp <= r.envelope.received_at
            {
                return Err(SignalRejection::InvalidEnvelope);
            }
        }
        for s in &record.sequences {
            let r = self.records.get_mut(s).expect("validated");
            for key in keys(&r.envelope) {
                if let Some(set) = self.matching.get_mut(&key) {
                    set.remove(s);
                    if set.is_empty() {
                        self.matching.remove(&key);
                    }
                }
            }
            self.receipts.remove(&(r.envelope.received_at, *s));
            r.retirement = Some(record.control.clone());
            self.stats.retained -= 1;
            self.stats.retired += 1;
        }
        Ok(())
    }
    pub(crate) fn hydrate(
        records: &[SignalRecord],
        last: u64,
        wal: u64,
    ) -> Result<Self, SignalRejection> {
        let mut index = Self::default();
        let mut previous_wal = 0;
        for r in records {
            if r.wal_sequence < previous_wal
                || r.wal_sequence > wal
                || CanonicalSignalV1::new(&r.envelope)?.digest() != r.digest
            {
                return Err(SignalRejection::InvalidEnvelope);
            }
            previous_wal = r.wal_sequence;
            if r.pins.len() > MAX_SIGNAL_PINS || (r.retirement.is_some() && !r.pins.is_empty()) {
                return Err(SignalRejection::InvalidEnvelope);
            }
            for c in r.pins.values().chain(r.retirement.iter()) {
                if c.wal_sequence <= r.wal_sequence || c.wal_sequence > wal {
                    return Err(SignalRejection::InvalidEnvelope);
                }
            }
            if r.retirement.as_ref().is_some_and(|c| c.timestamp <= r.envelope.received_at) {
                return Err(SignalRejection::InvalidEnvelope);
            }
            index.insert(r.clone())?;
        }
        if index.last_sequence != last {
            return Err(SignalRejection::InvalidEnvelope);
        }
        Ok(index)
    }
}

#[cfg(test)]
mod tests {
    use actionqueue_core::mutation::*;

    use super::*;
    use crate::{
        mutation::StorageMutationAuthority,
        recovery::reducer::ReplayReducer,
        wal::{
            event::WalEvent,
            writer::{WalWriter, WalWriterError},
        },
    };
    #[derive(Debug)]
    struct NoWrites;
    impl WalWriter for NoWrites {
        fn append(&mut self, _: &WalEvent) -> Result<(), WalWriterError> {
            panic!("must reject before append")
        }
        fn flush(&mut self) -> Result<(), WalWriterError> {
            panic!("must reject before sync")
        }
        fn close(self) -> Result<(), WalWriterError> {
            Ok(())
        }
    }
    #[test]
    fn exhaustion_rejects_new_identity_but_preserves_exact_retry() {
        let e = SignalEnvelope {
            signal_id: SignalId::new("one").unwrap(),
            tenant_id: None,
            namespace: SignalNamespace::new("ns").unwrap(),
            kind: SignalKind::new("kind").unwrap(),
            correlation_id: None,
            causation: None,
            source_ref: None,
            payload: None,
            payload_hash: None,
            occurred_at: None,
            received_at: 0,
            control_context: None,
        };
        for (last_signal, latest_wal, expected_wal) in [(u64::MAX, 1, 2), (1, u64::MAX, 0)] {
            let mut p = ReplayReducer::new();
            p.signals
                .insert(SignalRecord::new(e.clone(), SignalSequence::new(1), 1).unwrap())
                .unwrap();
            p.signals.last_sequence = last_signal;
            p.latest_sequence = latest_wal;
            let mut a = StorageMutationAuthority::new(NoWrites, p);
            let c = SignalAdmitCommand::new(0, e.clone());
            assert!(matches!(
                a.submit_command(MutationCommand::SignalAdmit(c), DurabilityPolicy::Immediate)
                    .unwrap()
                    .applied(),
                AppliedMutation::Signal(AdmitSignalOutcome::AlreadyExists { .. })
            ));
            let mut new = e.clone();
            new.signal_id = SignalId::new("two").unwrap();
            assert!(matches!(
                a.submit_command(
                    MutationCommand::SignalAdmit(SignalAdmitCommand::new(expected_wal, new)),
                    DurabilityPolicy::Immediate
                ),
                Err(crate::mutation::MutationAuthorityError::Signal(
                    SignalRejection::SequenceExhausted
                ))
            ));
        }
    }
}
