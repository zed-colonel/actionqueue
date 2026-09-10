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
