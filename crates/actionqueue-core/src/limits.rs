//! Hard ceilings; runtime configuration may lower these limits, never raise them.

/// Maximum opaque ref bytes.
pub const MAX_OPAQUE_REF_BYTES: usize = 512;
/// Maximum admission key bytes.
pub const MAX_ADMISSION_KEY_BYTES: usize = 256;
/// Maximum signal id bytes.
pub const MAX_SIGNAL_ID_BYTES: usize = 256;
/// Maximum correlation id bytes.
pub const MAX_CORRELATION_ID_BYTES: usize = 256;
/// Maximum trace id bytes.
pub const MAX_TRACE_ID_BYTES: usize = 128;
/// Maximum signal namespace bytes.
pub const MAX_SIGNAL_NAMESPACE_BYTES: usize = 64;
/// Maximum signal kind bytes.
pub const MAX_SIGNAL_KIND_BYTES: usize = 64;
/// Maximum code bytes.
pub const MAX_CODE_BYTES: usize = 64;
/// Maximum opaque data resolver scheme bytes.
pub const MAX_DATA_SCHEME_BYTES: usize = 64;
/// Maximum error message bytes.
pub const MAX_ERROR_MESSAGE_BYTES: usize = 2048;
/// Maximum executor trait bytes.
pub const MAX_EXECUTOR_TRAIT_BYTES: usize = 128;
/// Maximum executor traits.
pub const MAX_EXECUTOR_TRAITS: usize = 64;
/// Maximum inline data bytes.
pub const MAX_INLINE_DATA_BYTES: usize = 65536;
/// Maximum dependencies per task.
pub const MAX_DEPENDENCIES_PER_TASK: usize = 64;
/// Maximum child admissions per disposition.
pub const MAX_CHILD_ADMISSIONS_PER_DISPOSITION: usize = 64;
/// Maximum signals per disposition.
pub const MAX_SIGNALS_PER_DISPOSITION: usize = 32;
/// Maximum runs per admission.
pub const MAX_RUNS_PER_ADMISSION: usize = 64;
/// Maximum content type bytes.
pub const MAX_CONTENT_TYPE_BYTES: usize = 128;
/// Maximum budget consumption entries per disposition.
pub const MAX_CONSUMPTION_ENTRIES_PER_DISPOSITION: usize = 64;

/// Hard admission record ceiling including the v1 WAL frame header.
pub const MAX_ADMISSION_RECORD_BYTES: usize = 16 * 1024 * 1024 + 52;
/// Creation limits. Each field is clamped to its hard ceiling when used.
/// Existing admissions are resolved before applying lowered creation limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionLimits {
    /// Maximum dependencies.
    pub dependencies: usize,
    /// Maximum initial runs.
    pub initial_runs: usize,
    /// Maximum payload bytes.
    pub payload_bytes: usize,
    /// Maximum content-type bytes.
    pub content_type_bytes: usize,
    /// Maximum encoded record bytes, including framing.
    pub record_bytes: usize,
}
impl Default for AdmissionLimits {
    fn default() -> Self {
        Self {
            dependencies: MAX_DEPENDENCIES_PER_TASK,
            initial_runs: MAX_RUNS_PER_ADMISSION,
            payload_bytes: MAX_INLINE_DATA_BYTES,
            content_type_bytes: MAX_CONTENT_TYPE_BYTES,
            record_bytes: MAX_ADMISSION_RECORD_BYTES,
        }
    }
}
impl AdmissionLimits {
    /// Checks borrowed data before normalization, cloning, hashing, or run allocation.
    pub fn validate_spec(
        &self,
        spec: &crate::task::task_spec::TaskSpec,
        dependencies: usize,
    ) -> Result<(), crate::admission::AdmissionRejection> {
        use crate::admission::AdmissionRejection::TooLarge;
        if dependencies > self.dependencies.min(MAX_DEPENDENCIES_PER_TASK)
            || spec.task_payload().bytes().len() > self.payload_bytes.min(MAX_INLINE_DATA_BYTES)
            || spec.task_payload().content_type().map_or(0, str::len)
                > self.content_type_bytes.min(MAX_CONTENT_TYPE_BYTES)
        {
            return Err(TooLarge);
        }
        let count = match spec.run_policy() {
            crate::task::run_policy::RunPolicy::Once => 1,
            crate::task::run_policy::RunPolicy::Repeat(p) => p.count() as usize,
            #[cfg(feature = "workflow")]
            crate::task::run_policy::RunPolicy::Cron(p) => {
                p.max_occurrences().map_or(5, |n| (n as usize).min(5))
            }
        };
        if count > self.initial_runs.min(MAX_RUNS_PER_ADMISSION) {
            return Err(TooLarge);
        }
        // Include per-entry framing even for empty strings. Reserve fixed space for
        // IDs, bounded causal/control fields, and the maximum initial run set.
        let mut size = 16_384usize;
        let mut add = |n: usize| -> Result<(), crate::admission::AdmissionRejection> {
            size = size.checked_add(n).and_then(|v| v.checked_add(8)).ok_or(TooLarge)?;
            if size > MAX_ADMISSION_RECORD_BYTES {
                return Err(TooLarge);
            }
            Ok(())
        };
        add(spec.task_payload().bytes().len())?;
        add(spec.task_payload().content_type().map_or(0, str::len))?;
        add(spec.constraints().concurrency_key().map_or(0, str::len))?;
        add(spec.metadata().description().map_or(0, str::len))?;
        for tag in spec.metadata().tags() {
            add(tag.len())?;
        }
        #[cfg(feature = "workflow")]
        if let crate::task::run_policy::RunPolicy::Cron(p) = spec.run_policy() {
            add(p.expression().len())?;
        }
        Ok(())
    }
}

/// Total framed signal record ceiling, including the 52-byte WAL header.
pub const MAX_SIGNAL_RECORD_BYTES: usize = 128 * 1024;
/// Hard bound on one retirement proposal and query page.
pub const MAX_SIGNAL_BATCH: usize = 1024;
/// Hard bound on independent pins on one signal.
pub const MAX_SIGNAL_PINS: usize = 64;
/// Operational signal limits. Hard format limits always apply, including during replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalLimits {
    /// Includes retired identities, which remain resident.
    pub identities: usize,
    /// Sum of framed immutable admission record sizes, including retired records.
    pub bytes: usize,
    /// Per-record creation limit, clamped to the hard ceiling.
    pub record_bytes: usize,
    /// Inline creation limit, clamped to the hard ceiling.
    pub inline_bytes: usize,
    /// Independent pins per signal, clamped to the hard ceiling.
    pub pins_per_signal: usize,
    /// Total pins in the store.
    pub pins: usize,
    /// Retirement batch size, clamped to the hard ceiling.
    pub retirement_batch: usize,
}
impl Default for SignalLimits {
    fn default() -> Self {
        Self {
            identities: 100_000,
            bytes: 16 * 1024 * 1024,
            record_bytes: MAX_SIGNAL_RECORD_BYTES,
            inline_bytes: MAX_INLINE_DATA_BYTES,
            pins_per_signal: MAX_SIGNAL_PINS,
            pins: 100_000,
            retirement_batch: MAX_SIGNAL_BATCH,
        }
    }
}
/// Both thresholds must be exceeded; retirement is explicit, never automatic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalRetentionPolicy {
    /// Minimum elapsed receipt age in seconds.
    pub minimum_age_secs: u64,
    /// Protect the newest sequence window.
    pub minimum_sequence_window: u64,
}
impl Default for SignalRetentionPolicy {
    fn default() -> Self {
        Self { minimum_age_secs: 7 * 24 * 60 * 60, minimum_sequence_window: 10_000 }
    }
}
/// Receipt and protection facts for one signal, independent of routing fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalRetentionCandidate {
    /// Store-assigned receipt time in seconds.
    pub received_at: u64,
    /// Store-assigned signal sequence.
    pub sequence: u64,
    /// Whether pins or durable references prevent retirement.
    pub protected: bool,
}
impl SignalRetentionPolicy {
    /// Conservative receipt-age/sequence arithmetic; clock rollback is ineligible.
    pub fn permits(
        &self,
        candidate: SignalRetentionCandidate,
        last_sequence: u64,
        now: u64,
    ) -> bool {
        !candidate.protected
            && now.checked_sub(candidate.received_at).is_some_and(|age| age > self.minimum_age_secs)
            && last_sequence
                .checked_sub(candidate.sequence)
                .is_some_and(|distance| distance > self.minimum_sequence_window)
    }
}
