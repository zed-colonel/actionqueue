//! Version-one rolling commitment over exact encoded frames, never re-encoded events.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WalCursor {
    pub sequence: u64,
    pub end: u64,
    pub last_start: u64,
    pub last_hash: [u8; 32],
    pub commitment: [u8; 32],
}
impl WalCursor {
    pub(crate) fn advance(&mut self, sequence: u64, start: u64, bytes: &[u8]) {
        let hash: [u8; 32] = Sha256::digest(bytes).into();
        self.advance_hash(sequence, start, bytes.len() as u64, hash);
    }
    pub(crate) fn advance_hash(&mut self, sequence: u64, start: u64, len: u64, hash: [u8; 32]) {
        let mut h = Sha256::new();
        h.update(b"AQ-CONT-1\0wal-prefix\0v1\0");
        h.update(self.commitment);
        h.update(sequence.to_le_bytes());
        h.update(start.to_le_bytes());
        h.update(len.to_le_bytes());
        h.update(hash);
        *self = Self {
            sequence,
            end: start + len,
            last_start: start,
            last_hash: hash,
            commitment: h.finalize().into(),
        };
    }
}

/// Opaque evidence produced only by session recovery and the native append path.
/// Custom writers have no constructor; transparent adapters may forward a borrow.
#[derive(Debug)]
#[doc(hidden)]
pub struct WalProvenance {
    pub(crate) session: uuid::Uuid,
    pub(crate) origin: uuid::Uuid,
    pub(crate) cursor: WalCursor,
}
