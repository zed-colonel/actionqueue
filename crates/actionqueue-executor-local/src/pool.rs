//! Bounded, single-threaded dispatch queue for attempt coordination.
//! This type is NOT thread-safe. For concurrent access, wrap in a Mutex.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

/// Typed errors emitted by [`DispatchQueue`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchQueueError {
    /// Intake was rejected because the queue reached configured capacity.
    Backpressure {
        /// Maximum number of queued work items allowed.
        capacity: usize,
    },
    /// Operation was rejected because the dispatch queue has been shut down.
    Shutdown,
}

impl fmt::Display for DispatchQueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backpressure { capacity } => {
                write!(f, "dispatch queue capacity reached ({capacity})")
            }
            Self::Shutdown => write!(f, "dispatch queue is shut down"),
        }
    }
}

impl Error for DispatchQueueError {}

/// Deterministic FIFO dispatch queue for attempt coordination.
///
/// # Invariants
///
/// - Intake is bounded to `capacity`.
/// - Dequeue order is deterministic FIFO.
/// - Shutdown state is explicit and inspectable.
/// - This type does not apply execution or retry policy.
#[derive(Debug)]
pub struct DispatchQueue<T> {
    capacity: usize,
    queue: VecDeque<T>,
    shutdown: bool,
}

impl<T> DispatchQueue<T> {
    /// Creates a dispatch queue with bounded intake capacity.
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity: capacity.get(),
            queue: VecDeque::with_capacity(capacity.get()),
            shutdown: false,
        }
    }

    /// Enqueues one work item.
    ///
    /// Returns:
    /// - [`DispatchQueueError::Shutdown`] if the dispatch queue has been closed.
    /// - [`DispatchQueueError::Backpressure`] if queue capacity is already reached.
    pub fn enqueue(&mut self, item: T) -> Result<(), DispatchQueueError> {
        if self.shutdown {
            return Err(DispatchQueueError::Shutdown);
        }

        if self.queue.len() >= self.capacity {
            return Err(DispatchQueueError::Backpressure { capacity: self.capacity });
        }

        self.queue.push_back(item);
        Ok(())
    }

    /// Dequeues one work item in deterministic FIFO order.
    ///
    /// Returns [`DispatchQueueError::Shutdown`] only when the dispatch queue has been shut down
    /// and no queued work remains.
    pub fn dequeue(&mut self) -> Result<Option<T>, DispatchQueueError> {
        if let Some(item) = self.queue.pop_front() {
            return Ok(Some(item));
        }

        if self.shutdown {
            return Err(DispatchQueueError::Shutdown);
        }

        Ok(None)
    }

    /// Marks the dispatch queue as shut down.
    ///
    /// After shutdown:
    /// - New intake is rejected.
    /// - Existing queued items remain available to be drained in FIFO order.
    pub fn shutdown(&mut self) {
        self.shutdown = true;
    }

    /// Returns queue capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns queued item count.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns true when no items are queued.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Returns true when dispatch queue has been marked shut down.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::{DispatchQueue, DispatchQueueError};

    #[test]
    fn dequeue_is_fifo() {
        let mut queue = DispatchQueue::new(NonZeroUsize::new(3).expect("non-zero"));
        queue.enqueue(10).expect("first enqueue should succeed");
        queue.enqueue(20).expect("second enqueue should succeed");
        queue.enqueue(30).expect("third enqueue should succeed");

        assert_eq!(queue.dequeue().expect("dequeue should succeed"), Some(10));
        assert_eq!(queue.dequeue().expect("dequeue should succeed"), Some(20));
        assert_eq!(queue.dequeue().expect("dequeue should succeed"), Some(30));
        assert_eq!(queue.dequeue().expect("dequeue should succeed"), None);
    }

    #[test]
    fn enqueue_returns_backpressure_at_capacity() {
        let mut queue = DispatchQueue::new(NonZeroUsize::new(1).expect("non-zero"));

        queue.enqueue(7).expect("enqueue should succeed");

        assert_eq!(queue.enqueue(8), Err(DispatchQueueError::Backpressure { capacity: 1 }));
    }

    #[test]
    fn shutdown_rejects_intake_but_allows_drain() {
        let mut queue = DispatchQueue::new(NonZeroUsize::new(2).expect("non-zero"));
        queue.enqueue(1).expect("enqueue should succeed");
        queue.shutdown();

        assert_eq!(queue.enqueue(2), Err(DispatchQueueError::Shutdown));
        assert_eq!(queue.dequeue().expect("drain should succeed"), Some(1));
        assert_eq!(queue.dequeue(), Err(DispatchQueueError::Shutdown));
    }
}
