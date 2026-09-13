//! Deterministic iterator-work instrumentation, enabled only by the testing feature.
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
    #[cfg(feature = "testing")]
    COUNTS.with(|c| {
        let mut counts = c.get();
        counts[kind as usize] += 1;
        c.set(counts);
    });
    #[cfg(not(feature = "testing"))]
    let _ = kind;
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
