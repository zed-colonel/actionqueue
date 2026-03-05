//! Command execution primitives for the ActionQueue CLI control plane.

use serde::Serialize;

pub mod daemon;
pub mod stats;
pub mod submit;

/// Stable classification for CLI failure lanes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    /// Invalid invocation shape or argument syntax.
    Usage,
    /// Contract-level input validation failure.
    Validation,
    /// Local runtime failure while executing command flow.
    Runtime,
    /// Connectivity failure class (reserved for remote control-plane paths).
    Connectivity,
}

impl ErrorKind {
    /// Maps error kind to deterministic process exit code.
    pub const fn exit_code(self) -> i32 {
        match self {
            ErrorKind::Usage => 2,
            ErrorKind::Validation => 3,
            ErrorKind::Runtime => 4,
            ErrorKind::Connectivity => 5,
        }
    }
}

/// Typed CLI command failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CliError {
    kind: ErrorKind,
    code: &'static str,
    message: String,
}

impl CliError {
    /// Builds a usage-class error.
    pub fn usage(code: &'static str, message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Usage, code, message: message.into() }
    }

    /// Builds a validation-class error.
    pub fn validation(code: &'static str, message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Validation, code, message: message.into() }
    }

    /// Builds a runtime-class error.
    pub fn runtime(code: &'static str, message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Runtime, code, message: message.into() }
    }

    /// Builds a connectivity-class error.
    pub fn connectivity(code: &'static str, message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Connectivity, code, message: message.into() }
    }

    /// Returns failure classification.
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns stable machine-readable error code.
    pub const fn code(&self) -> &'static str {
        self.code
    }

    /// Returns human-readable error message.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for CliError {}

/// Structured stderr payload for deterministic automation-safe failures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ErrorPayload<'a> {
    /// Stable error class taxonomy.
    pub error_kind: ErrorKind,
    /// Stable machine-readable failure code.
    pub error_code: &'a str,
    /// Human-readable detail string.
    pub message: &'a str,
}

/// Command success output payload lane.
#[derive(Debug, Clone, PartialEq)]
#[must_use]
pub enum CommandOutput {
    /// Human-readable deterministic text output.
    Text(String),
    /// Machine-readable deterministic JSON output.
    Json(serde_json::Value),
}

/// Returns current Unix timestamp seconds.
pub fn now_unix_seconds() -> Result<u64, CliError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|err| CliError::runtime("runtime_clock_error", err.to_string()))
        .map(|duration| duration.as_secs())
}

/// Resolves CLI data-root with deterministic default behavior.
///
/// When no override is provided, defaults to `$HOME/.actionqueue/data`. If the
/// `HOME` environment variable is not set, falls back to `.actionqueue/data`
/// relative to the current working directory.
pub fn resolve_data_dir(override_dir: Option<&std::path::Path>) -> std::path::PathBuf {
    override_dir.map(ToOwned::to_owned).unwrap_or_else(|| {
        std::env::var("HOME")
            .map(|h| std::path::PathBuf::from(h).join(".actionqueue/data"))
            .unwrap_or_else(|_| std::path::PathBuf::from(".actionqueue/data"))
    })
}
