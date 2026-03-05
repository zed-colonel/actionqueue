//! WAL writer interface.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::wal::event::WalEvent;

/// Snapshot of authoritative WAL append telemetry totals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct WalAppendTelemetrySnapshot {
    /// Total successful WAL append operations.
    pub append_success_total: u64,
    /// Total failed WAL append operations.
    pub append_failure_total: u64,
}

/// Shared authoritative telemetry for WAL append outcomes.
///
/// This type stores monotonic append-success and append-failure totals and is
/// safe to share across threads. Counter updates use saturating semantics at
/// `u64::MAX` and never wrap or panic.
#[derive(Debug, Clone)]
pub struct WalAppendTelemetry {
    inner: Arc<WalAppendTelemetryInner>,
}

#[derive(Debug, Default)]
struct WalAppendTelemetryInner {
    append_success_total: AtomicU64,
    append_failure_total: AtomicU64,
}

impl Default for WalAppendTelemetry {
    fn default() -> Self {
        Self::new()
    }
}

impl WalAppendTelemetry {
    /// Creates a new zero-initialized WAL append telemetry handle.
    pub fn new() -> Self {
        Self { inner: Arc::new(WalAppendTelemetryInner::default()) }
    }

    /// Returns a point-in-time snapshot of append outcome totals.
    pub fn snapshot(&self) -> WalAppendTelemetrySnapshot {
        WalAppendTelemetrySnapshot {
            append_success_total: self.inner.append_success_total.load(Ordering::Relaxed),
            append_failure_total: self.inner.append_failure_total.load(Ordering::Relaxed),
        }
    }

    fn record_append_success(&self) {
        Self::saturating_increment(&self.inner.append_success_total);
    }

    fn record_append_failure(&self) {
        Self::saturating_increment(&self.inner.append_failure_total);
    }

    fn saturating_increment(counter: &AtomicU64) {
        let mut current = counter.load(Ordering::Relaxed);
        loop {
            if current == u64::MAX {
                return;
            }

            let next = current.saturating_add(1);
            match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

/// WAL writer wrapper that records authoritative append outcomes.
///
/// This wrapper preserves the underlying writer behavior and error propagation.
/// It only records append success/failure totals after each `append` result is
/// known. `flush` and `close` are transparent pass-through operations.
#[derive(Debug)]
pub struct InstrumentedWalWriter<W: WalWriter> {
    inner: W,
    telemetry: WalAppendTelemetry,
}

impl<W: WalWriter> InstrumentedWalWriter<W> {
    /// Wraps an inner writer with authoritative append telemetry.
    pub fn new(inner: W, telemetry: WalAppendTelemetry) -> Self {
        Self { inner, telemetry }
    }

    /// Returns a shared telemetry handle.
    pub fn telemetry(&self) -> &WalAppendTelemetry {
        &self.telemetry
    }

    /// Returns a reference to the inner writer.
    pub fn inner(&self) -> &W {
        &self.inner
    }
}

impl<W: WalWriter> WalWriter for InstrumentedWalWriter<W> {
    fn append(&mut self, event: &WalEvent) -> Result<(), WalWriterError> {
        match self.inner.append(event) {
            Ok(()) => {
                self.telemetry.record_append_success();
                Ok(())
            }
            Err(error) => {
                self.telemetry.record_append_failure();
                Err(error)
            }
        }
    }

    fn flush(&mut self) -> Result<(), WalWriterError> {
        self.inner.flush()
    }

    fn close(self) -> Result<(), WalWriterError> {
        self.inner.close()
    }
}

/// A writer that can append events to the WAL.
pub trait WalWriter {
    /// Append an event to the WAL.
    fn append(&mut self, event: &WalEvent) -> Result<(), WalWriterError>;

    /// Flush pending writes to durable storage.
    fn flush(&mut self) -> Result<(), WalWriterError>;

    /// Close the writer, releasing any resources.
    fn close(self) -> Result<(), WalWriterError>;
}

/// Errors that can occur during WAL writing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalWriterError {
    /// The writer was closed.
    Closed,
    /// I/O error during write.
    IoError(String),
    /// The event could not be encoded.
    EncodeError(String),
    /// Sequence number violation (non-increasing or duplicate).
    SequenceViolation {
        /// The expected next sequence number.
        expected: u64,
        /// The sequence number that was provided.
        provided: u64,
    },
    /// Writer is permanently poisoned after a truncation-recovery failure.
    /// Callers must restart the process.
    Poisoned,
}

impl std::fmt::Display for WalWriterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalWriterError::Closed => write!(f, "WAL writer is closed"),
            WalWriterError::IoError(e) => write!(f, "I/O error: {e}"),
            WalWriterError::EncodeError(e) => write!(f, "Encode error: {e}"),
            WalWriterError::SequenceViolation { expected, provided } => {
                write!(f, "Sequence violation: expected {expected}, got {provided}")
            }
            WalWriterError::Poisoned => {
                write!(f, "WAL writer is permanently poisoned after truncation failure")
            }
        }
    }
}

impl std::error::Error for WalWriterError {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{InstrumentedWalWriter, WalAppendTelemetry, WalWriter, WalWriterError};
    use crate::wal::event::WalEventType;

    #[derive(Debug)]
    struct SuccessWriter;

    impl WalWriter for SuccessWriter {
        fn append(&mut self, _event: &crate::wal::event::WalEvent) -> Result<(), WalWriterError> {
            Ok(())
        }

        fn flush(&mut self) -> Result<(), WalWriterError> {
            Ok(())
        }

        fn close(self) -> Result<(), WalWriterError> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FailureWriter {
        error: WalWriterError,
    }

    impl WalWriter for FailureWriter {
        fn append(&mut self, _event: &crate::wal::event::WalEvent) -> Result<(), WalWriterError> {
            Err(self.error.clone())
        }

        fn flush(&mut self) -> Result<(), WalWriterError> {
            Ok(())
        }

        fn close(self) -> Result<(), WalWriterError> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FlushErrorWriter {
        flush_error: WalWriterError,
    }

    impl WalWriter for FlushErrorWriter {
        fn append(&mut self, _event: &crate::wal::event::WalEvent) -> Result<(), WalWriterError> {
            Ok(())
        }

        fn flush(&mut self) -> Result<(), WalWriterError> {
            Err(self.flush_error.clone())
        }

        fn close(self) -> Result<(), WalWriterError> {
            Ok(())
        }
    }

    fn sample_event(sequence: u64) -> crate::wal::event::WalEvent {
        crate::wal::event::WalEvent::new(
            sequence,
            WalEventType::EnginePaused { timestamp: sequence },
        )
    }

    #[test]
    fn instrumented_writer_increments_success_on_append_success() {
        let telemetry = WalAppendTelemetry::new();
        let mut writer = InstrumentedWalWriter::new(SuccessWriter, telemetry.clone());

        writer.append(&sample_event(1)).expect("append should succeed");

        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.append_success_total, 1);
        assert_eq!(snapshot.append_failure_total, 0);
    }

    #[test]
    fn instrumented_writer_increments_failure_and_preserves_error_identity() {
        let telemetry = WalAppendTelemetry::new();
        let expected_error = WalWriterError::IoError("append failed".to_string());
        let mut writer = InstrumentedWalWriter::new(
            FailureWriter { error: expected_error.clone() },
            telemetry.clone(),
        );

        let observed_error =
            writer.append(&sample_event(1)).expect_err("append should return the underlying error");

        assert_eq!(observed_error, expected_error);
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.append_success_total, 0);
        assert_eq!(snapshot.append_failure_total, 1);
    }

    #[test]
    fn flush_errors_do_not_alter_append_counters() {
        let telemetry = WalAppendTelemetry::new();
        let mut writer = InstrumentedWalWriter::new(
            FlushErrorWriter { flush_error: WalWriterError::IoError("flush failed".to_string()) },
            telemetry.clone(),
        );

        writer.append(&sample_event(1)).expect("append should succeed");
        let _ = writer.flush();

        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.append_success_total, 1);
        assert_eq!(snapshot.append_failure_total, 0);
    }

    #[test]
    fn counter_updates_saturate_at_u64_max_without_panic() {
        let telemetry = WalAppendTelemetry::new();
        telemetry.inner.append_success_total.store(u64::MAX, std::sync::atomic::Ordering::Relaxed);
        telemetry.inner.append_failure_total.store(u64::MAX, std::sync::atomic::Ordering::Relaxed);

        let mut success_writer = InstrumentedWalWriter::new(SuccessWriter, telemetry.clone());
        let mut failure_writer = InstrumentedWalWriter::new(
            FailureWriter { error: WalWriterError::IoError("append failed".to_string()) },
            telemetry.clone(),
        );

        success_writer.append(&sample_event(1)).expect("append should still return success");
        let _ = failure_writer.append(&sample_event(2));

        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.append_success_total, u64::MAX);
        assert_eq!(snapshot.append_failure_total, u64::MAX);
    }

    #[test]
    fn concurrent_append_success_paths_preserve_monotonic_totals() {
        let telemetry = Arc::new(WalAppendTelemetry::new());
        let threads = 8u64;
        let appends_per_thread = 2_000u64;

        let mut handles = Vec::new();
        for _ in 0..threads {
            let telemetry = Arc::clone(&telemetry);
            handles.push(std::thread::spawn(move || {
                let mut writer =
                    InstrumentedWalWriter::new(SuccessWriter, telemetry.as_ref().clone());
                for sequence in 1..=appends_per_thread {
                    writer
                        .append(&sample_event(sequence))
                        .expect("append should succeed on every iteration");
                }
            }));
        }

        for handle in handles {
            handle.join().expect("concurrent append worker should not panic");
        }

        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.append_success_total, threads * appends_per_thread);
        assert_eq!(snapshot.append_failure_total, 0);
    }
}
