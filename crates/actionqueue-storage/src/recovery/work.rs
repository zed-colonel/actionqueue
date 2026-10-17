//! Iterator-work instrumentation. Production measurements are scoped to live authority preparation.
//! Counters are thread-local and external to projections, snapshots and cloning.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Visit {
    WaitHistory,
    SignalHistory,
    MatchCandidate,
    SignalCandidate,
    WaitBucket,
    Waiter,
    CandidateRemoval,
}
#[cfg(feature = "testing")]
thread_local! {
    static COUNTS: std::cell::Cell<[usize; 7]> = const { std::cell::Cell::new([0; 7]) };
}
#[inline]
pub(crate) fn visit(kind: Visit) {
    if matches!(kind, Visit::Waiter | Visit::SignalCandidate) {
        MATCHING.with(|c| {
            if let Some(mut work) = c.get() {
                match kind {
                    Visit::Waiter => work.waits += 1,
                    Visit::SignalCandidate => work.signals += 1,
                    _ => {}
                }
                c.set(Some(work));
            }
        });
    }
    #[cfg(feature = "testing")]
    COUNTS.with(|c| {
        let mut counts = c.get();
        counts[kind as usize] += 1;
        c.set(counts);
    });
}
/// Reset the calling thread's measured work, excluding fixture setup/replay costs.
#[cfg(feature = "testing")]
pub fn reset() {
    COUNTS.with(|c| c.set([0; 7]));
}
/// Wait history, signal history, match candidates, signal candidates, bucket probes,
/// waiter visits, and candidate removals, in that order.
#[cfg(feature = "testing")]
pub fn counts() -> [usize; 7] {
    COUNTS.with(std::cell::Cell::get)
}

/// Complete reducer copies and apply entries on the calling thread, including replay.
/// Snapshot materialization, digest traversal and collection scans are excluded.
#[cfg(feature = "testing")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProjectionWork {
    pub full_projection_clones: usize,
    /// Sum of source run counts across complete reducer copies.
    pub cloned_run_entries: usize,
    /// Entries into `ReplayReducer::apply`, including nested and rejected applications.
    pub reducer_event_visits: usize,
}
#[cfg(feature = "testing")]
thread_local! {
    static PROJECTION_WORK: std::cell::Cell<ProjectionWork> = const {
        std::cell::Cell::new(ProjectionWork {
            full_projection_clones: 0,
            cloned_run_entries: 0,
            reducer_event_visits: 0,
        })
    };
}
/// Reset only the calling thread's projection work; the seven iterator counters are separate.
#[cfg(feature = "testing")]
pub fn reset_projection_work() {
    PROJECTION_WORK.with(|c| c.set(ProjectionWork::default()));
}
/// Read without resetting. Use synchronous regions that remain on the calling thread.
#[cfg(feature = "testing")]
pub fn projection_work() -> ProjectionWork {
    PROJECTION_WORK.with(std::cell::Cell::get)
}
#[cfg(feature = "testing")]
pub(crate) fn projection_clone(runs: usize) {
    PROJECTION_WORK.with(|c| {
        let mut work = c.get();
        work.full_projection_clones += 1;
        work.cloned_run_entries += runs;
        c.set(work);
    });
}
#[cfg(feature = "testing")]
pub(crate) fn reducer_event() {
    PROJECTION_WORK.with(|c| {
        let mut work = c.get();
        work.reducer_event_visits += 1;
        c.set(work);
    });
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;

    #[test]
    fn projection_counters_are_thread_local_and_reset_independently() {
        reset();
        reset_projection_work();
        visit(Visit::WaitHistory);
        projection_clone(7);
        reducer_event();
        let expected = ProjectionWork {
            full_projection_clones: 1,
            cloned_run_entries: 7,
            reducer_event_visits: 1,
        };
        assert_eq!(projection_work(), expected);
        std::thread::spawn(|| {
            assert_eq!(projection_work(), ProjectionWork::default());
            projection_clone(3);
            reducer_event();
            reset_projection_work();
            assert_eq!(projection_work(), ProjectionWork::default());
        })
        .join()
        .unwrap();
        assert_eq!(projection_work(), expected);
        reset();
        assert_eq!(projection_work(), expected);
        visit(Visit::WaitHistory);
        reset_projection_work();
        assert_eq!(projection_work(), ProjectionWork::default());
        assert_eq!(counts(), [1, 0, 0, 0, 0, 0, 0]);
        reset();
    }
}

/// Actual candidate visits while preparing a live mutation, including nested reducer work.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MatchingWork {
    pub waits: u64,
    pub signals: u64,
}
thread_local! {
    static MATCHING: std::cell::Cell<Option<MatchingWork>> = const { std::cell::Cell::new(None) };
}
/// Never installed by replay or inspection. Restore the previous scope even on panic.
pub(crate) fn measure<T>(operation: impl FnOnce() -> T) -> (T, MatchingWork) {
    struct Reset(Option<MatchingWork>);
    impl Drop for Reset {
        fn drop(&mut self) {
            MATCHING.with(|c| c.set(self.0));
        }
    }
    let _reset = Reset(MATCHING.with(|c| c.replace(Some(MatchingWork::default()))));
    let result = operation();
    let work = MATCHING.with(|c| c.get().unwrap_or_default());
    (result, work)
}
