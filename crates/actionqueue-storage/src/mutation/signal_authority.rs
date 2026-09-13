//! Signal preparation under the exclusive mutation owner, followed by the common WAL lane.
use actionqueue_core::{
    continuation::*, data_ref::DataRef, ids::SignalSequence, limits::*, mutation::*,
};

use super::{authority::*, signal::*};
use crate::{
    recovery::signals::SignalStatistics,
    wal::{event::*, writer::WalWriter},
};
pub(super) enum SignalPreparation {
    Noop(MutationOutcome),
    Event(Box<WalEvent>, AppliedMutation),
}
use SignalRejection as R;
impl<W: WalWriter, P: MutationProjection> StorageMutationAuthority<W, P> {
    /// Operational creation quotas; retries resolve before applying these limits.
    pub fn set_signal_limits(&mut self, limits: SignalLimits) {
        self.signal_limits = limits;
    }
    /// Explicit retirement thresholds; recovery does not consult them.
    pub fn set_signal_retention_policy(&mut self, policy: SignalRetentionPolicy) {
        self.signal_retention = policy;
    }
    /// Operational signal statistics; rejection telemetry resets on reopen.
    pub fn signal_statistics(&self) -> SignalStatistics {
        let mut stats =
            self.projection().signal_index().map(|i| i.statistics()).unwrap_or_default();
        stats.capacity_rejections = self.signal_capacity_rejections;
        stats
    }
    /// Resolve before sampling the clock or allocating sequences. Fenced authorities fail closed.
    pub fn lookup_signal(
        &self,
        envelope: &SignalEnvelope,
    ) -> Result<Option<AdmitSignalOutcome>, MutationAuthorityError<P::Error>> {
        if self.recovery_required() {
            return Err(MutationAuthorityError::RecoveryRequired);
        }
        self.authorize_control(
            actionqueue_core::control::QueueAction::AdmitSignal,
            envelope.tenant_id,
        )?;
        if envelope
            .control_context
            .as_ref()
            .is_some_and(|c| Some(c) != self.control_context().map(|h| &h.attribution))
        {
            return Err(MutationAuthorityError::Control(
                actionqueue_core::control::ControlError::Unauthorized,
            ));
        }
        let digest =
            CanonicalSignalV1::new(envelope).map_err(MutationAuthorityError::Signal)?.digest();
        let result = self
            .projection()
            .signal_index()
            .ok_or(MutationAuthorityError::Signal(R::UnsupportedFeature))?
            .get_signal(envelope.tenant_id, &envelope.signal_id)
            .map(|r| r.resolve(&digest))
            .transpose()
            .map_err(MutationAuthorityError::Signal);
        if matches!(result, Ok(Some(_))) {
            self.telemetry().signal_duplicate();
        }
        result
    }
    pub(super) fn prepare_signal(
        &self,
        command: &MutationCommand,
        durability: DurabilityPolicy,
    ) -> Result<Option<SignalPreparation>, R> {
        if !matches!(
            command,
            MutationCommand::SignalAdmit(_)
                | MutationCommand::SignalPin(_)
                | MutationCommand::SignalUnpin(_)
                | MutationCommand::RetireSignals(_)
        ) {
            return Ok(None);
        }
        if durability != DurabilityPolicy::Immediate {
            return Err(R::ImmediateDurabilityRequired);
        }
        let index = self.projection().signal_index().ok_or(R::UnsupportedFeature)?;
        let stats = index.statistics();
        let noop = |sequence, applied| {
            Ok(Some(SignalPreparation::Noop(MutationOutcome::new(sequence, applied))))
        };
        let (expected, event, applied) = match command {
            MutationCommand::SignalAdmit(c) => {
                let digest = CanonicalSignalV1::new(c.envelope())?.digest();
                if let Some(r) = index.get_signal(c.envelope().tenant_id, &c.envelope().signal_id) {
                    return noop(r.wal_sequence(), AppliedMutation::Signal(r.resolve(&digest)?));
                }
                self.projection().validate_signal_references(c.envelope())?;
                if let Some(DataRef::Inline(d)) = &c.envelope().payload {
                    if d.bytes().len() > self.signal_limits.inline_bytes.min(MAX_INLINE_DATA_BYTES)
                    {
                        return Err(R::TooLarge);
                    }
                }
                if stats.retained + stats.retired >= self.signal_limits.identities {
                    return Err(R::Capacity);
                }
                if self.projection().latest_sequence().checked_add(1).ok_or(R::SequenceExhausted)?
                    != c.expected_sequence()
                {
                    return Err(R::InvalidEnvelope);
                }
                let sequence =
                    index.last_sequence().get().checked_add(1).ok_or(R::SequenceExhausted)?;
                let record = SignalRecord::new(
                    c.envelope().clone(),
                    SignalSequence::new(sequence),
                    c.expected_sequence(),
                )?;
                let bytes = record.encoded_bytes()?;
                if bytes > self.signal_limits.record_bytes.min(MAX_SIGNAL_RECORD_BYTES) {
                    return Err(R::TooLarge);
                }
                if stats.bytes.checked_add(bytes).is_none_or(|v| v > self.signal_limits.bytes) {
                    return Err(R::Capacity);
                }
                let applied = AppliedMutation::Signal(record.outcome(true));
                (c.expected_sequence(), WalEventType::SignalAdmitted { record }, applied)
            }
            MutationCommand::SignalPin(c) | MutationCommand::SignalUnpin(c) => {
                let acquire = matches!(command, MutationCommand::SignalPin(_));
                let r = index.get_signal(c.tenant_id, &c.signal_id).ok_or(R::NotFound)?;
                if acquire && !r.is_retained() {
                    return Err(R::Retired);
                }
                if r.pins().contains_key(&c.pin_id) == acquire {
                    return noop(
                        self.projection().latest_sequence(),
                        AppliedMutation::SignalRetention { changed: 0 },
                    );
                }
                if acquire
                    && (r.pins().len() >= self.signal_limits.pins_per_signal.min(MAX_SIGNAL_PINS)
                        || stats.pins >= self.signal_limits.pins)
                {
                    return Err(R::Capacity);
                }
                let record = SignalPinRecord {
                    tenant_id: c.tenant_id,
                    signal_id: c.signal_id.clone(),
                    pin_id: c.pin_id.clone(),
                    control: SignalControlRecord {
                        wal_sequence: c.expected_sequence,
                        timestamp: c.timestamp,
                        control_context: c.control_context.clone(),
                    },
                };
                (
                    c.expected_sequence,
                    if acquire {
                        WalEventType::SignalPinned { record }
                    } else {
                        WalEventType::SignalUnpinned { record }
                    },
                    AppliedMutation::SignalRetention { changed: 1 },
                )
            }
            MutationCommand::RetireSignals(c) => {
                if c.sequences.len() > self.signal_limits.retirement_batch.min(MAX_SIGNAL_BATCH) {
                    return Err(R::TooLarge);
                }
                if c.sequences.windows(2).any(|w| w[0] >= w[1]) {
                    return Err(R::InvalidEnvelope);
                }
                if c.sequences.is_empty() {
                    return noop(
                        self.projection().latest_sequence(),
                        AppliedMutation::SignalRetention { changed: 0 },
                    );
                }
                for s in &c.sequences {
                    let r = index.by_sequence(*s).ok_or(R::NotFound)?;
                    if r.envelope().tenant_id != c.tenant_id {
                        return Err(R::TenantMismatch);
                    }
                    if !r.is_retained()
                        || !self.signal_retention.permits(
                            SignalRetentionCandidate {
                                received_at: r.envelope().received_at,
                                sequence: s.get(),
                                protected: self.projection().signal_is_protected(r.sequence()),
                            },
                            index.last_sequence().get(),
                            c.timestamp,
                        )
                    {
                        return Err(R::Protected);
                    }
                }
                let record = SignalsRetiredRecord {
                    tenant_id: c.tenant_id,
                    sequences: c.sequences.clone(),
                    control: SignalControlRecord {
                        wal_sequence: c.expected_sequence,
                        timestamp: c.timestamp,
                        control_context: c.control_context.clone(),
                    },
                };
                (
                    c.expected_sequence,
                    WalEventType::SignalsRetired { record },
                    AppliedMutation::SignalRetention { changed: c.sequences.len() },
                )
            }
            _ => unreachable!(),
        };
        if self.projection().latest_sequence().checked_add(1).ok_or(R::SequenceExhausted)?
            != expected
        {
            return Err(R::InvalidEnvelope);
        }
        let profile = self
            .store_session()
            .map(|s| s.manifest().features.clone())
            .unwrap_or_else(crate::store::capabilities);
        crate::store::check_event_profile(&event, &profile).map_err(|_| R::UnsupportedFeature)?;
        let event = WalEvent::new(expected, event);
        if crate::wal::codec::encode(&event).map_err(|_| R::TooLarge)?.len()
            > MAX_SIGNAL_RECORD_BYTES
        {
            return Err(R::TooLarge);
        }
        Ok(Some(SignalPreparation::Event(Box::new(event), applied)))
    }
}
