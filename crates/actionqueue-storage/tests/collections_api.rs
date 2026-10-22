//! Exercise the collection API from outside the storage crate.
use actionqueue_core::{
    continuation::SignalEnvelope,
    ids::{RunId, TaskId, WaitId},
};
use actionqueue_storage::recovery::{
    collections::{
        BTreeSet, BTreeSetIntoIter, BTreeSetIter, HashSet, HashSetIntoIter, HashSetIter, History,
        HistoryIter,
    },
    reducer::ReplayReducer,
    waits::WaitIndex,
    ProjectionChange,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct NoDefault(u32);

#[test]
fn sets_preserve_membership_replacement_and_structural_independence() {
    let mut hash = HashSet::<NoDefault>::default();
    let mut ordered = BTreeSet::<NoDefault>::default();
    assert!(hash.is_empty());
    assert!(ordered.is_empty());
    for n in [3, 1, 2] {
        assert_eq!(hash.insert(NoDefault(n)), None);
        assert_eq!(ordered.insert(NoDefault(n)), None);
    }
    assert_eq!(hash.insert(NoDefault(2)), Some(NoDefault(2)));
    assert_eq!(ordered.insert(NoDefault(2)), Some(NoDefault(2)));
    let hash_before = hash.clone();
    let ordered_before = ordered.clone();
    assert_eq!(hash.remove(&NoDefault(2)), Some(NoDefault(2)));
    assert_eq!(ordered.remove(&NoDefault(2)), Some(NoDefault(2)));
    assert_eq!(hash.remove(&NoDefault(2)), None);
    assert_eq!(ordered.remove(&NoDefault(2)), None);
    hash.extend([NoDefault(4), NoDefault(1)]);
    ordered.extend([NoDefault(4), NoDefault(1)]);
    assert_eq!(hash.len(), 3);
    assert_eq!(ordered.len(), 3);
    assert!(hash_before.contains(&NoDefault(2)));
    assert!(ordered_before.contains(&NoDefault(2)));
    assert_eq!(hash, [1, 3, 4].map(NoDefault).into_iter().collect());
    assert_eq!(ordered, [1, 3, 4].map(NoDefault).into_iter().collect());
    hash.clear();
    ordered.clear();
    assert!(hash.is_empty());
    assert!(ordered.is_empty());
    assert_eq!(hash_before.len(), 3);
    assert_eq!(ordered_before.len(), 3);
}

#[test]
fn named_set_iterators_forward_values_and_remaining_lengths() {
    let hash: HashSet<_> = (0..100).collect();
    let mut borrowed: HashSetIter<'_, _> = hash.iter();
    assert_eq!(borrowed.len(), 100);
    let first = *borrowed.next().unwrap();
    assert_eq!(borrowed.size_hint(), (99, Some(99)));
    let mut values = borrowed.copied().collect::<std::collections::BTreeSet<_>>();
    assert!(values.insert(first));
    assert_eq!(values, (0..100).collect());
    let _: HashSetIter<'_, _> = (&hash).into_iter();
    let mut owned: HashSetIntoIter<_> = hash.into_iter();
    assert_eq!(owned.len(), 100);
    owned.next().unwrap();
    assert_eq!(owned.size_hint(), (99, Some(99)));
    assert_eq!(owned.count(), 99);

    let ordered: BTreeSet<_> = (0..100).rev().collect();
    let mut borrowed: BTreeSetIter<'_, _> = (&ordered).into_iter();
    assert_eq!(borrowed.len(), 100);
    assert_eq!(borrowed.next(), Some(&0));
    assert_eq!(borrowed.next_back(), Some(&99));
    assert_eq!(borrowed.size_hint(), (98, Some(98)));
    assert_eq!(borrowed.copied().collect::<Vec<_>>(), (1..99).collect::<Vec<_>>());
    let mut range = ordered.range(20..=80);
    assert_eq!(range.next(), Some(&20));
    assert_eq!(range.next_back(), Some(&80));
    assert_eq!(range.rev().copied().collect::<Vec<_>>(), (21..80).rev().collect::<Vec<_>>());
    let mut owned: BTreeSetIntoIter<_> = ordered.into_iter();
    assert_eq!(owned.next(), Some(0));
    assert_eq!(owned.next_back(), Some(99));
    assert_eq!(owned.len(), 98);
    assert_eq!(owned.size_hint(), (98, Some(98)));
    assert_eq!(owned.rev().collect::<Vec<_>>(), (1..99).rev().collect::<Vec<_>>());
}

#[test]
fn history_iterator_borrows_records_across_both_ends() {
    // Cross persistent vector chunk boundaries, and exercise the empty case.
    for len in [0, 1, 1000] {
        let history: History<_> = (0..len).collect();
        let _: HistoryIter<'_, _> = history.iter();
        let mut iter: HistoryIter<'_, _> = (&history).into_iter();
        let mut expected = (0..len).collect::<Vec<_>>().into_iter();
        for step in 0..len {
            assert_eq!(iter.len(), len - step);
            assert_eq!(iter.size_hint(), (len - step, Some(len - step)));
            let (actual, value) = if step % 2 == 0 {
                (iter.next(), expected.next())
            } else {
                (iter.next_back(), expected.next_back())
            };
            assert_eq!(actual.copied(), value);
            assert!(std::ptr::eq(actual.unwrap(), &history[value.unwrap()]));
        }
        assert_eq!(iter.len(), 0);
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_back(), None);
    }
}

#[test]
fn projection_surfaces_name_storage_owned_sets() {
    let change = ProjectionChange::default();
    let _: BTreeSet<TaskId> = change.created_tasks;
    let _: BTreeSet<TaskId> = change.tasks;
    let _: BTreeSet<RunId> = change.runs;
    let _: BTreeSet<WaitId> = change.resolved_waits;
    let _: fn(&WaitIndex, &SignalEnvelope) -> BTreeSet<WaitId> = WaitIndex::signal_waiters;
    let projection = ReplayReducer::new();
    for (task, prerequisites) in projection.dependency_declarations() {
        let _: TaskId = task;
        let _: &HashSet<TaskId> = prerequisites;
    }
}
