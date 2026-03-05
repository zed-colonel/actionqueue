//! Recovery replay driver - orchestrates replay from WAL.

use crate::recovery::reducer::ReplayReducer;
use crate::wal::reader::{WalReader, WalReaderError};

/// A replay driver that loads events from a reader and feeds them to a reducer.
pub struct ReplayDriver<R: WalReader> {
    reader: R,
    reducer: ReplayReducer,
}

impl<R: WalReader> ReplayDriver<R> {
    /// Creates a new replay driver with the given reader and reducer.
    pub fn new(reader: R, reducer: ReplayReducer) -> Self {
        ReplayDriver { reader, reducer }
    }

    /// Runs the replay, feeding events from the reader to the reducer.
    ///
    /// Reads events sequentially from the reader and applies them to the reducer.
    /// If strict WAL corruption is encountered, returns `WalReaderError::Corruption`.
    /// If all events are processed successfully, returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns `WalReaderError::Corruption` if strict corruption is detected
    /// in the event stream.
    ///
    /// Returns `WalReaderError::IoError` if an I/O error occurs during reading.
    ///
    /// Returns `WalReaderError::ReducerError` if the reducer encounters an error applying an event
    /// (e.g., invalid transition, duplicate event).
    pub fn run(&mut self) -> Result<(), WalReaderError> {
        self.run_with_applied_count().map(|_| ())
    }

    /// Runs replay and returns the authoritative count of applied replay events.
    ///
    /// This is the counting seam used by recovery bootstrap metrics population.
    pub fn run_with_applied_count(&mut self) -> Result<u64, WalReaderError> {
        let mut applied_events = 0u64;
        loop {
            match self.reader.read_next() {
                Ok(Some(event)) => {
                    // Apply the event to the reducer
                    self.reducer.apply(&event)?;
                    applied_events = applied_events.saturating_add(1);
                }
                Ok(None) => {
                    // End of WAL reached successfully.
                    break;
                }
                Err(e) => {
                    // Propagate strict corruption or other reader/reducer errors.
                    return Err(e);
                }
            }
        }
        Ok(applied_events)
    }

    /// Returns the reducer after replay has completed.
    pub fn into_reducer(self) -> ReplayReducer {
        self.reducer
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use crate::recovery::reducer::ReplayReducer;
    use crate::recovery::replay::ReplayDriver;
    use crate::wal::event::{WalEvent, WalEventType};
    use crate::wal::reader::{WalReader, WalReaderError};

    /// Test WAL reader with deterministic event and error sequencing.
    #[derive(Debug)]
    struct TestWalReader {
        events: VecDeque<WalEvent>,
        terminal_error: Option<WalReaderError>,
        is_end: bool,
    }

    impl TestWalReader {
        fn new(events: Vec<WalEvent>, terminal_error: Option<WalReaderError>) -> Self {
            Self { events: VecDeque::from(events), terminal_error, is_end: false }
        }
    }

    impl WalReader for TestWalReader {
        fn read_next(&mut self) -> Result<Option<WalEvent>, WalReaderError> {
            if let Some(event) = self.events.pop_front() {
                return Ok(Some(event));
            }

            if let Some(error) = self.terminal_error.take() {
                return Err(error);
            }

            self.is_end = true;
            Ok(None)
        }

        fn seek_to_sequence(&mut self, _sequence: u64) -> Result<(), WalReaderError> {
            Ok(())
        }

        fn is_end(&self) -> bool {
            self.is_end
        }
    }

    #[test]
    fn run_returns_exact_applied_event_count_for_successful_replay() {
        let reader = TestWalReader::new(
            vec![
                WalEvent::new(1, WalEventType::EnginePaused { timestamp: 10 }),
                WalEvent::new(2, WalEventType::EngineResumed { timestamp: 11 }),
            ],
            None,
        );
        let mut driver = ReplayDriver::new(reader, ReplayReducer::new());

        let applied_events =
            driver.run_with_applied_count().expect("replay should succeed with count");

        assert_eq!(applied_events, 2);
    }

    #[test]
    fn run_preserves_typed_error_semantics_when_reader_errors() {
        let reader = TestWalReader::new(
            vec![WalEvent::new(1, WalEventType::EnginePaused { timestamp: 10 })],
            Some(WalReaderError::IoError("boom".to_string())),
        );
        let mut driver = ReplayDriver::new(reader, ReplayReducer::new());

        let error =
            driver.run_with_applied_count().expect_err("replay should fail with reader io error");

        assert!(matches!(error, WalReaderError::IoError(message) if message == "boom"));
    }
}
