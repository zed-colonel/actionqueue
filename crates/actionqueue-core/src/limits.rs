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
