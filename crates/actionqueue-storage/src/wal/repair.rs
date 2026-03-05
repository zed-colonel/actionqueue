//! WAL file repair utilities.
//!
//! This module provides controlled truncation of trailing partial records
//! in a WAL file, enabling recovery from crashes that left incomplete writes.

use std::fs::OpenOptions;
use std::io;

/// Truncates a WAL file to the given `valid_end_offset`.
///
/// This removes any trailing bytes after the last complete, valid record.
/// The file is synced after truncation to ensure durability.
///
/// # Safety
///
/// The caller must ensure that `valid_end_offset` actually points to a valid
/// record boundary. Using an incorrect offset will corrupt the WAL.
pub fn truncate_to_last_valid(path: &std::path::Path, valid_end_offset: u64) -> io::Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(valid_end_offset)?;
    file.sync_all()?;
    Ok(())
}

/// Policy controlling how the WAL writer handles trailing corruption on bootstrap.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RepairPolicy {
    /// Current behavior: hard-fail on any trailing corruption.
    #[default]
    Strict,
    /// Truncate trailing partial record and continue.
    /// Only the incomplete trailing record is removed; mid-stream corruption
    /// still hard-fails.
    TruncatePartial,
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_path() -> std::path::PathBuf {
        let dir = std::env::temp_dir();
        let count = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let path =
            dir.join(format!("actionqueue_wal_repair_test_{}_{}.tmp", std::process::id(), count));
        let _ = fs::remove_file(&path);
        path
    }

    #[test]
    fn truncate_removes_trailing_bytes() {
        let path = temp_path();
        {
            let mut file = fs::File::create(&path).unwrap();
            file.write_all(b"valid_record_data_here_extra_junk").unwrap();
        }

        truncate_to_last_valid(&path, 22).unwrap();

        let contents = fs::read(&path).unwrap();
        assert_eq!(&contents, b"valid_record_data_here");

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn truncate_to_zero_empties_file() {
        let path = temp_path();
        {
            let mut file = fs::File::create(&path).unwrap();
            file.write_all(b"some data").unwrap();
        }

        truncate_to_last_valid(&path, 0).unwrap();

        let contents = fs::read(&path).unwrap();
        assert!(contents.is_empty());

        let _ = fs::remove_file(&path);
    }
}
