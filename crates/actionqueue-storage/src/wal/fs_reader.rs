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
use crate::store::StoreError;
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
    cursor: super::checkpoint::WalCursor,
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
            cursor: Default::default(),
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
    pub fn cursor(&self) -> super::checkpoint::WalCursor {
        self.cursor
    }
    /// Validates the two bounded probes, then starts at the checkpoint end.
    ///
    /// A checkpoint that disagrees with the WAL's manifest binding or covered
    /// boundary is an invalid store, not an I/O failure, and halts startup.
    pub(crate) fn from_checkpoint(
        session: &crate::store::StoreSession,
        cursor: super::checkpoint::WalCursor,
    ) -> Result<Self, StoreError> {
        let bad = || {
            StoreError::InvalidStore(format!(
                "snapshot checkpoint at sequence {} does not match the WAL boundary; run \
                 actionqueue store verify",
                cursor.sequence
            ))
        };
        let invalid = |e: WalReaderError| StoreError::InvalidStore(e.to_string());
        let io = |e: std::io::Error| StoreError::Io(format!("reading WAL boundary: {e}"));
        let mut reader = Self::for_session(session)
            .map_err(|WalFsReaderError::IoError(e)| StoreError::Io(format!("opening WAL: {e}")))?;
        let first = reader.read_next().map_err(invalid)?.ok_or_else(bad)?;
        if !matches!(first.event(), super::event::WalEventType::StoreInitialized { manifest_digest } if *manifest_digest == session.manifest().digest())
        {
            return Err(bad());
        }
        if cursor.sequence == 0
            || cursor.end > reader.file.metadata().map_err(io)?.len()
            || cursor.last_start >= cursor.end
            || cursor.last_start < if cursor.sequence == 1 { 0 } else { reader.cursor.end }
        {
            return Err(bad());
        }
        reader.file.seek(SeekFrom::Start(cursor.last_start)).map_err(io)?;
        let last =
            read_record(&mut reader.file, Some(session.manifest().store_id), Some(cursor.sequence))
                .map_err(invalid)?
                .ok_or_else(bad)?;
        if cursor.last_start + last.record_len as u64 != cursor.end
            || last.frame_hash != cursor.last_hash
        {
            return Err(bad());
        }
        if cursor.sequence == 1 && (cursor.last_start != 0 || reader.cursor != cursor) {
            return Err(bad());
        }
        reader.cursor = cursor;
        reader.current_sequence = cursor.sequence;
        reader.is_end = false;
        Ok(reader)
    }
    pub fn current_sequence(&self) -> u64 {
        self.current_sequence
    }
    pub fn is_end(&self) -> bool {
        self.is_end
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
                self.cursor.advance_hash(
                    r.event.sequence(),
                    self.cursor.end,
                    r.record_len as u64,
                    r.frame_hash,
                );
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
        self.cursor = Default::default();
        self.is_end = false;
        self.pending_event = None;
        // Validate the entire file before exposing a seek result, including the suffix.
        let mut found = None;
        while let Some(event) = self.read_next()? {
            if event.sequence() == sequence {
                found = Some((event, self.position()?, self.cursor));
            }
        }
        let (event, position, cursor) = found.ok_or(WalReaderError::EndOfWal)?;
        self.file
            .seek(SeekFrom::Start(position))
            .map_err(|e| WalReaderError::IoError(e.to_string()))?;
        self.cursor = cursor;
        self.current_sequence = event.sequence();
        self.pending_event = Some(event);
        self.is_end = false;
        Ok(())
    }
    fn is_end(&self) -> bool {
        self.is_end
    }
}
