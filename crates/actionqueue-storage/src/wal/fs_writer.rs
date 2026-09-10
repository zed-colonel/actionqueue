//! Session-bound filesystem WAL writer. All recovery validation precedes writable access.
use super::{
    event::WalEvent,
    repair::RepairPolicy,
    tail_validation::WalCorruption,
    writer::{WalWriter, WalWriterError},
};
use crate::{
    recovery::reducer::ReplayReducer,
    store::{StoreError, StoreSession},
};
use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
};
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalFsWriterInitError {
    IoError(String),
    Corruption(WalCorruption),
    Store(StoreError),
}
impl std::fmt::Display for WalFsWriterInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WAL initialization: {self:?}")
    }
}
impl std::error::Error for WalFsWriterInitError {}
impl From<std::io::Error> for WalFsWriterInitError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e.to_string())
    }
}
impl From<StoreError> for WalFsWriterInitError {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}
#[derive(Debug)]
pub struct WalFsWriter {
    file: File,
    current_sequence: u64,
    poisoned: bool,
    session: Option<StoreSession>,
    projection: Option<ReplayReducer>,
}
impl WalFsWriter {
    pub fn new(session: StoreSession) -> Result<Self, WalFsWriterInitError> {
        Self::new_with_repair(session, RepairPolicy::Strict)
    }
    pub fn new_with_repair(
        session: StoreSession,
        policy: RepairPolicy,
    ) -> Result<Self, WalFsWriterInitError> {
        session.require_write()?;
        let recovered = crate::recovery::bootstrap::recover_read_only(&session, policy)?;
        session.claim_writer()?;
        let mut file = OpenOptions::new().read(true).write(true).open(session.wal_path())?;
        if let Some(tail) = recovered.incomplete_tail {
            file.set_len(tail.offset)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            current_sequence: recovered.projection.latest_sequence(),
            poisoned: false,
            session: Some(session),
            projection: Some(recovered.projection),
        })
    }
    pub fn session(&self) -> Option<&StoreSession> {
        self.session.as_ref()
    }
    pub fn current_sequence(&self) -> u64 {
        self.current_sequence
    }
    /// Raw framing fixtures only: unavailable in production builds.
    #[cfg(any(test, feature = "testing"))]
    pub fn new_raw_for_test(path: std::path::PathBuf) -> Result<Self, WalFsWriterInitError> {
        use super::reader::{WalReader, WalReaderError};
        let mut sequence = 0;
        if path.exists() {
            let mut reader = super::fs_reader::WalFsReader::new(path.clone())
                .map_err(|e| WalFsWriterInitError::IoError(e.to_string()))?;
            loop {
                match reader.read_next() {
                    Ok(Some(e)) => sequence = e.sequence(),
                    Ok(None) => break,
                    Err(WalReaderError::Corruption(c)) => {
                        return Err(WalFsWriterInitError::Corruption(c))
                    }
                    Err(e) => return Err(WalFsWriterInitError::IoError(e.to_string())),
                }
            }
        }
        let mut file =
            OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            current_sequence: sequence,
            poisoned: false,
            session: None,
            projection: None,
        })
    }
}
impl WalWriter for WalFsWriter {
    fn fence(&mut self) {
        self.poisoned = true;
    }
    fn recovery_required(&self) -> bool {
        self.poisoned
    }
    fn append(&mut self, event: &WalEvent) -> Result<(), WalWriterError> {
        if self.poisoned {
            return Err(WalWriterError::Poisoned);
        }
        let expected = self.current_sequence.checked_add(1).ok_or(WalWriterError::Poisoned)?;
        if event.sequence() < expected || (self.session.is_some() && event.sequence() != expected) {
            return Err(WalWriterError::SequenceViolation { expected, provided: event.sequence() });
        }
        let mut prepared = self.projection.clone();
        if let Some(session) = &self.session {
            crate::store::check_event_profile(event.event(), &session.manifest().features)
                .map_err(|e| WalWriterError::EncodeError(e.to_string()))?;
            if matches!(event.event(), super::event::WalEventType::StoreInitialized { .. }) {
                return Err(WalWriterError::EncodeError(
                    "StoreInitialized is only valid at sequence one".into(),
                ));
            }
        }
        if let Some(p) = &mut prepared {
            p.validate_target_event(event.event())
                .map_err(|e| WalWriterError::EncodeError(e.to_string()))?;
            p.apply(event).map_err(|e| WalWriterError::EncodeError(e.to_string()))?;
            // An accepted durable prefix must also be inspectable and snapshotable.
            p.projection_image().map_err(|e| WalWriterError::EncodeError(e.to_string()))?;
        }
        let bytes = super::codec::encode_for_store(
            event,
            self.session.as_ref().map(|s| s.manifest().store_id).unwrap_or_default(),
        )
        .map_err(|e| WalWriterError::EncodeError(e.to_string()))?;
        let start =
            self.file.stream_position().map_err(|e| WalWriterError::IoError(e.to_string()))?;
        crate::store::fault::checkpoint("wal_before_append")
            .map_err(|e| WalWriterError::IoError(e.to_string()))?;
        // Partial-frame injection is compiled only into conformance/test support builds.
        #[cfg(feature = "testing")]
        if crate::store::fault::armed("wal_partial_frame") {
            self.file.write_all(&bytes[..super::codec::HEADER_LEN]).map_err(|e| {
                self.poisoned = true;
                WalWriterError::IoError(e.to_string())
            })?;
            crate::store::fault::checkpoint("wal_partial_frame").map_err(|e| {
                self.poisoned = true;
                WalWriterError::IoError(e.to_string())
            })?;
        }
        if let Err(e) = self.file.write_all(&bytes) {
            // A failed append is uncertain; fence this writer even if rollback succeeds.
            self.poisoned = true;
            self.file
                .set_len(start)
                .and_then(|_| self.file.sync_all())
                .map_err(|_| WalWriterError::Poisoned)?;
            return Err(WalWriterError::IoError(e.to_string()));
        }
        self.current_sequence = event.sequence();
        self.projection = prepared;
        Ok(())
    }
    fn flush(&mut self) -> Result<(), WalWriterError> {
        if self.poisoned {
            return Err(WalWriterError::Poisoned);
        }
        crate::store::fault::checkpoint("wal_before_sync")
            .and_then(|_| self.file.sync_all())
            .map_err(|e| {
                self.poisoned = true;
                WalWriterError::IoError(e.to_string())
            })
    }
    fn close(mut self) -> Result<(), WalWriterError> {
        self.flush()
    }
    fn store_session(&self) -> Option<&StoreSession> {
        self.session.as_ref()
    }
}

impl Drop for WalFsWriter {
    fn drop(&mut self) {
        // Drop releases ownership without implicitly acknowledging unsynced work.
        if let Some(session) = &self.session {
            session.release_writer();
        }
    }
}
