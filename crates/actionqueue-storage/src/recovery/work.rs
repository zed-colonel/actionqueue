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
