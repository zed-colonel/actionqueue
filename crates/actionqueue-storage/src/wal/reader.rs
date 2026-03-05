//! WAL reader interface.

use crate::wal::event::WalEvent;
use crate::wal::tail_validation::WalCorruption;

/// A reader that can read events from the WAL.
pub trait WalReader {
    /// Read the next event from the WAL.
    fn read_next(&mut self) -> Result<Option<WalEvent>, WalReaderError>;

    /// Seek to a specific sequence number.
    fn seek_to_sequence(&mut self, sequence: u64) -> Result<(), WalReaderError>;

    /// Returns true if the reader is at the end of the WAL.
    fn is_end(&self) -> bool;
}

/// Errors that can occur during WAL reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalReaderError {
    /// The reader reached the end of the WAL.
    EndOfWal,
    /// I/O error during read.
    IoError(String),
    /// Strict WAL corruption details with deterministic record-boundary offset.
    Corruption(WalCorruption),
    /// The event could not be decoded outside strict framed-record corruption paths.
    DecodeError(String),
    /// An error occurred during replay reduction.
    ReducerError(String),
}

impl std::fmt::Display for WalReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalReaderError::EndOfWal => write!(f, "End of WAL reached"),
            WalReaderError::IoError(e) => write!(f, "I/O error: {e}"),
            WalReaderError::Corruption(details) => write!(f, "{details}"),
            WalReaderError::DecodeError(e) => write!(f, "Decode error: {e}"),
            WalReaderError::ReducerError(e) => write!(f, "Reducer error: {e}"),
        }
    }
}

impl std::error::Error for WalReaderError {}

impl std::convert::From<crate::recovery::reducer::ReplayReducerError> for WalReaderError {
    fn from(err: crate::recovery::reducer::ReplayReducerError) -> Self {
        match err {
            crate::recovery::reducer::ReplayReducerError::InvalidTransition => {
                WalReaderError::ReducerError("Invalid state transition during replay".to_string())
            }
            crate::recovery::reducer::ReplayReducerError::DuplicateEvent => {
                WalReaderError::ReducerError("Duplicate event detected".to_string())
            }
            crate::recovery::reducer::ReplayReducerError::CorruptedData => {
                WalReaderError::ReducerError("Corrupted event data".to_string())
            }
            crate::recovery::reducer::ReplayReducerError::LeaseCausality(details) => {
                WalReaderError::ReducerError(format!(
                    "Lease causality violation during replay: {details}"
                ))
            }
            crate::recovery::reducer::ReplayReducerError::TaskCausality(details) => {
                WalReaderError::ReducerError(format!(
                    "Task causality violation during replay: {details}"
                ))
            }
            crate::recovery::reducer::ReplayReducerError::EngineControlCausality(details) => {
                WalReaderError::ReducerError(format!(
                    "Engine control causality violation during replay: {details}"
                ))
            }
        }
    }
}
