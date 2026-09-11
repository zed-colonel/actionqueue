//! Streaming target WAL reader, using the shared bounded frame parser.
use std::{
    fs::File,
    io::{Seek, SeekFrom},
};

use super::{
    event::WalEvent,
    reader::{WalReader, WalReaderError},
    tail_validation::read_record,
};
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalFsReaderError {
    IoError(String),
}
impl std::fmt::Display for WalFsReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WAL open: {self:?}")
    }
}
impl std::error::Error for WalFsReaderError {}
impl From<std::io::Error> for WalFsReaderError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e.to_string())
    }
}
pub struct WalFsReader {
    file: File,
    current_sequence: u64,
    is_end: bool,
    pending_event: Option<WalEvent>,
    store_id: Option<uuid::Uuid>,
}
impl WalFsReader {
    /// Low-level frame inspection; production store readers use `for_session`.
    pub fn new(path: std::path::PathBuf) -> Result<Self, WalFsReaderError> {
        Ok(Self {
            file: File::open(path)?,
            current_sequence: 0,
            is_end: false,
            pending_event: None,
            store_id: None,
        })
    }
    pub fn for_session(session: &crate::store::StoreSession) -> Result<Self, WalFsReaderError> {
        let mut r = Self::new(session.wal_path())?;
        r.store_id = Some(session.manifest().store_id);
        Ok(r)
    }
    pub fn current_sequence(&self) -> u64 {
        self.current_sequence
    }
    pub fn is_end(&self) -> bool {
        self.is_end
    }
    pub fn reset_eof(&mut self) {
        self.is_end = false;
    }
    pub fn position(&mut self) -> Result<u64, WalReaderError> {
        self.file.stream_position().map_err(|e| WalReaderError::IoError(e.to_string()))
    }
}
impl WalReader for WalFsReader {
    fn read_next(&mut self) -> Result<Option<WalEvent>, WalReaderError> {
        if let Some(e) = self.pending_event.take() {
            return Ok(Some(e));
        }
        if self.is_end {
            return Ok(None);
        }
        let expected = if self.store_id.is_some() {
            Some(
                self.current_sequence
                    .checked_add(1)
                    .ok_or_else(|| WalReaderError::IoError("sequence exhausted".into()))?,
            )
        } else {
            None
        };
        match read_record(&mut self.file, self.store_id, expected) {
            Ok(Some(r)) => {
                self.current_sequence = r.event.sequence();
                Ok(Some(r.event))
            }
            Ok(None) => {
                self.is_end = true;
                Ok(None)
            }
            Err(e) => {
                self.is_end = true;
                Err(e)
            }
        }
    }
    fn seek_to_sequence(&mut self, sequence: u64) -> Result<(), WalReaderError> {
        self.file.seek(SeekFrom::Start(0)).map_err(|e| WalReaderError::IoError(e.to_string()))?;
        self.current_sequence = 0;
        self.is_end = false;
        self.pending_event = None;
        // Validate the entire file before exposing a seek result, including the suffix.
        let mut found = None;
        while let Some(event) = self.read_next()? {
            if event.sequence() == sequence {
                found = Some((event, self.position()?));
            }
        }
        let (event, position) = found.ok_or(WalReaderError::EndOfWal)?;
        self.file
            .seek(SeekFrom::Start(position))
            .map_err(|e| WalReaderError::IoError(e.to_string()))?;
        self.current_sequence = event.sequence();
        self.pending_event = Some(event);
        self.is_end = false;
        Ok(())
    }
    fn is_end(&self) -> bool {
        self.is_end
    }
}
