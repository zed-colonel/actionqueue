//! Storage-owned persistent collections. Nodes share records, and writers copy only
//! the addressed record. Materialization is explicit at persistence/API boundaries.
use std::{
    hash::Hash,
    ops::{Index, IndexMut},
    sync::Arc,
};

macro_rules! set {
    ($name:ident, $iter:ident, $into_iter:ident, $inner:ident, $module:ident, $($bound:tt)+) => {
        /// A storage-owned, structurally shared set.
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name<T: Clone + $($bound)+>(imbl::$inner<T>);

        impl<T: Clone + $($bound)+> Default for $name<T> {
            fn default() -> Self { Self(imbl::$inner::new()) }
        }
        impl<T: Clone + $($bound)+> $name<T> {
            pub fn new() -> Self { Self::default() }
            pub fn len(&self) -> usize { self.0.len() }
            pub fn is_empty(&self) -> bool { self.0.is_empty() }
            pub fn contains(&self, value: &T) -> bool { self.0.contains(value) }
            pub fn iter(&self) -> $iter<'_, T> { $iter(self.0.iter()) }
            pub fn insert(&mut self, value: T) -> Option<T> { self.0.insert(value) }
            pub fn remove(&mut self, value: &T) -> Option<T> { self.0.remove(value) }
            pub fn clear(&mut self) { self.0.clear(); }
        }
        impl<T: Clone + $($bound)+> FromIterator<T> for $name<T> {
            fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
                Self(iter.into_iter().collect())
            }
        }
        impl<T: Clone + $($bound)+> Extend<T> for $name<T> {
            fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
                self.0.extend(iter);
            }
        }

        /// A borrowed iterator over a storage-owned set.
        #[derive(Clone)]
        pub struct $iter<'a, T: Clone + $($bound)+>(
            imbl::$module::Iter<'a, T, imbl::shared_ptr::DefaultSharedPtr>,
        );
        impl<'a, T: Clone + $($bound)+> Iterator for $iter<'a, T> {
            type Item = &'a T;
            fn next(&mut self) -> Option<Self::Item> { self.0.next() }
            fn size_hint(&self) -> (usize, Option<usize>) { self.0.size_hint() }
        }
        impl<T: Clone + $($bound)+> ExactSizeIterator for $iter<'_, T> {}

        /// A consuming iterator over a storage-owned set.
        #[derive(Clone)]
        pub struct $into_iter<T: Clone + $($bound)+>(
            imbl::$module::ConsumingIter<T, imbl::shared_ptr::DefaultSharedPtr>,
        );
        impl<T: Clone + $($bound)+> Iterator for $into_iter<T> {
            type Item = T;
            fn next(&mut self) -> Option<Self::Item> { self.0.next() }
            fn size_hint(&self) -> (usize, Option<usize>) { self.0.size_hint() }
        }
        impl<T: Clone + $($bound)+> ExactSizeIterator for $into_iter<T> {}

        impl<'a, T: Clone + $($bound)+> IntoIterator for &'a $name<T> {
            type Item = &'a T;
            type IntoIter = $iter<'a, T>;
            fn into_iter(self) -> Self::IntoIter { self.iter() }
        }
        impl<T: Clone + $($bound)+> IntoIterator for $name<T> {
            type Item = T;
            type IntoIter = $into_iter<T>;
            fn into_iter(self) -> Self::IntoIter { $into_iter(self.0.into_iter()) }
        }
    }
}
set!(HashSet, HashSetIter, HashSetIntoIter, HashSet, hashset, Eq + Hash);
set!(BTreeSet, BTreeSetIter, BTreeSetIntoIter, OrdSet, ordset, Ord);

impl<T: Clone + Ord> BTreeSet<T> {
    pub fn range<R: std::ops::RangeBounds<T>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = &T> {
        self.0.range(range)
    }
}
impl<T: Clone + Ord> DoubleEndedIterator for BTreeSetIter<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back()
    }
}
impl<T: Clone + Ord> DoubleEndedIterator for BTreeSetIntoIter<T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back()
    }
}

fn writable<T: Clone>(value: &mut Arc<T>) -> &mut T {
    #[cfg(feature = "testing")]
    if Arc::strong_count(value) > 1 {
        super::work::record_copy();
    }
    Arc::make_mut(value)
}

fn owned<T: Clone>(value: Arc<T>) -> T {
    #[cfg(feature = "testing")]
    if Arc::strong_count(&value) > 1 {
        super::work::record_copy();
    }
    Arc::unwrap_or_clone(value)
}

macro_rules! map {
    ($name:ident, $entry:ident, $inner:ident, $($bound:tt)+) => {
        #[derive(Debug, Clone)]
        pub struct $name<K: Clone + $($bound)+, V: Clone>(imbl::$inner<K, Arc<V>>);
        impl<K: Clone + $($bound)+, V: Clone> Default for $name<K, V> {
            fn default() -> Self { Self(imbl::$inner::new()) }
        }
        impl<K: Clone + $($bound)+, V: Clone> $name<K, V> {
            pub fn new() -> Self { Self::default() }
            pub fn len(&self) -> usize { self.0.len() }
            pub fn is_empty(&self) -> bool { self.0.is_empty() }
            pub fn clear(&mut self) { self.0.clear(); }
            pub fn contains_key(&self, key: &K) -> bool { self.0.contains_key(key) }
            pub fn get(&self, key: &K) -> Option<&V> { self.0.get(key).map(Arc::as_ref) }
            pub fn get_mut(&mut self, key: &K) -> Option<&mut V> { self.0.get_mut(key).map(writable) }
            pub fn insert(&mut self, key: K, value: V) -> Option<V> {
                self.0.insert(key, Arc::new(value)).map(owned)
            }
            pub fn remove(&mut self, key: &K) -> Option<V> {
                self.0.remove(key).map(owned)
            }
            pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> { self.0.iter().map(|(k,v)| (k,v.as_ref())) }
            pub fn values(&self) -> impl Iterator<Item = &V> { self.0.values().map(Arc::as_ref) }
            pub fn keys(&self) -> impl Iterator<Item = &K> { self.0.keys() }
            pub fn entry(&mut self, key: K) -> $entry<'_, K, V> { $entry { map: self, key } }
        }
        impl<K: Clone + $($bound)+, V: Clone> FromIterator<(K,V)> for $name<K,V> {
            fn from_iter<I: IntoIterator<Item=(K,V)>>(iter: I) -> Self {
                Self(iter.into_iter().map(|(k,v)| (k,Arc::new(v))).collect())
            }
        }
        impl<K: Clone + $($bound)+, V: Clone> Index<&K> for $name<K,V> {
            type Output=V;
            fn index(&self, key: &K) -> &V { self.get(key).expect("projection key") }
        }
        pub struct $entry<'a, K: Clone + $($bound)+, V: Clone> { map: &'a mut $name<K,V>, key: K }
        impl<'a, K: Clone + $($bound)+, V: Clone> $entry<'a,K,V> {
            pub fn or_insert_with(self, create: impl FnOnce() -> V) -> &'a mut V {
                if !self.map.contains_key(&self.key) { self.map.insert(self.key.clone(), create()); }
                self.map.get_mut(&self.key).expect("inserted key")
            }
            pub fn or_insert(self, value: V) -> &'a mut V { self.or_insert_with(|| value) }
            pub fn or_default(self) -> &'a mut V where V: Default { self.or_insert_with(V::default) }
        }
    }
}
/// Hydration builds an immutable native hash table in linear time. A persistent
/// delta records subsequent changes without ever copying that retained table.
/// Delta keys shadow base keys, including removals; space is bounded by identities.
#[derive(Debug, Clone)]
pub struct HashMap<K: Clone + Eq + Hash, V: Clone> {
    base: Arc<std::collections::HashMap<K, Arc<V>>>,
    delta: imbl::HashMap<K, Option<Arc<V>>>,
    len: usize,
}
impl<K: Clone + Eq + Hash, V: Clone> Default for HashMap<K, V> {
    fn default() -> Self {
        Self {
            base: Arc::new(std::collections::HashMap::new()),
            delta: imbl::HashMap::new(),
            len: 0,
        }
    }
}
impl<K: Clone + Eq + Hash, V: Clone> HashMap<K, V> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn clear(&mut self) {
        *self = Self::new();
    }
    pub fn get(&self, key: &K) -> Option<&V> {
        match self.delta.get(key) {
            Some(v) => v.as_deref(),
            None => self.base.get(key).map(Arc::as_ref),
        }
    }
    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }
    fn get_arc(&self, key: &K) -> Option<Arc<V>> {
        match self.delta.get(key) {
            Some(v) => v.clone(),
            None => self.base.get(key).cloned(),
        }
    }
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if !self.delta.contains_key(key) {
            self.delta.insert(key.clone(), Some(self.base.get(key)?.clone()));
        }
        self.delta.get_mut(key).and_then(Option::as_mut).map(writable)
    }
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let previous = self.get_arc(&key);
        if previous.is_none() {
            self.len += 1;
        }
        self.delta.insert(key, Some(Arc::new(value)));
        previous.map(owned)
    }
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let previous = self.get_arc(key)?;
        if self.base.contains_key(key) {
            self.delta.insert(key.clone(), None);
        } else {
            self.delta.remove(key);
        }
        self.len -= 1;
        Some(owned(previous))
    }
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.base
            .iter()
            .filter(|(k, _)| !self.delta.contains_key(*k))
            .map(|(k, v)| (k, v.as_ref()))
            .chain(self.delta.iter().filter_map(|(k, v)| v.as_deref().map(|v| (k, v))))
    }
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.iter().map(|(k, _)| k)
    }
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_, v)| v)
    }
    pub fn entry(&mut self, key: K) -> HashEntry<'_, K, V> {
        HashEntry { map: self, key }
    }
}
impl<K: Clone + Eq + Hash, V: Clone> FromIterator<(K, V)> for HashMap<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        let mut base = std::collections::HashMap::with_capacity(upper.unwrap_or(lower));
        base.extend(iter.map(|(k, v)| (k, Arc::new(v))));
        let len = base.len();
        Self { base: Arc::new(base), delta: imbl::HashMap::new(), len }
    }
}
impl<K: Clone + Eq + Hash, V: Clone> Index<&K> for HashMap<K, V> {
    type Output = V;
    fn index(&self, k: &K) -> &V {
        self.get(k).expect("projection key")
    }
}
pub struct HashEntry<'a, K: Clone + Eq + Hash, V: Clone> {
    map: &'a mut HashMap<K, V>,
    key: K,
}
impl<'a, K: Clone + Eq + Hash, V: Clone> HashEntry<'a, K, V> {
    pub fn or_insert_with(self, create: impl FnOnce() -> V) -> &'a mut V {
        if !self.map.contains_key(&self.key) {
            self.map.insert(self.key.clone(), create());
        }
        self.map.get_mut(&self.key).expect("inserted key")
    }
    pub fn or_insert(self, value: V) -> &'a mut V {
        self.or_insert_with(|| value)
    }
    pub fn or_default(self) -> &'a mut V
    where
        V: Default,
    {
        self.or_insert_with(V::default)
    }
}
map!(BTreeMap, OrdEntry, OrdMap, Ord);

/// Chronological, structurally shared history with borrowed records. No contiguous
/// backing store is promised; use `to_vec` when a consumer needs owned contiguous data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct History<T: Clone>(imbl::Vector<Arc<T>>);
impl<T: Clone> Default for History<T> {
    fn default() -> Self {
        Self(imbl::Vector::new())
    }
}
impl<T: Clone> History<T> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn push(&mut self, value: T) {
        self.0.push_back(Arc::new(value));
    }
    pub fn iter(&self) -> HistoryIter<'_, T> {
        HistoryIter(self.0.iter())
    }
    pub fn iter_mut(&mut self) -> impl DoubleEndedIterator<Item = &mut T> {
        self.0.iter_mut().map(writable)
    }
    pub fn get(&self, index: usize) -> Option<&T> {
        self.0.get(index).map(Arc::as_ref)
    }
    pub fn last(&self) -> Option<&T> {
        self.0.back().map(Arc::as_ref)
    }
    pub fn last_mut(&mut self) -> Option<&mut T> {
        self.0.back_mut().map(writable)
    }
    pub fn to_vec(&self) -> Vec<T> {
        self.iter().cloned().collect()
    }
}
impl<T: Clone> Index<usize> for History<T> {
    type Output = T;
    fn index(&self, i: usize) -> &T {
        self.get(i).expect("history index")
    }
}
impl<T: Clone> IndexMut<usize> for History<T> {
    fn index_mut(&mut self, i: usize) -> &mut T {
        writable(self.0.get_mut(i).expect("history index"))
    }
}
impl<T: Clone> FromIterator<T> for History<T> {
    fn from_iter<I: IntoIterator<Item = T>>(i: I) -> Self {
        Self(i.into_iter().map(Arc::new).collect())
    }
}
impl<T: Clone> From<Vec<T>> for History<T> {
    fn from(v: Vec<T>) -> Self {
        v.into_iter().collect()
    }
}
impl<'a, T: Clone> IntoIterator for &'a History<T> {
    type Item = &'a T;
    type IntoIter = HistoryIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A borrowed, chronological iterator over a structurally shared history.
#[derive(Clone)]
pub struct HistoryIter<'a, T: Clone>(
    imbl::vector::Iter<'a, Arc<T>, imbl::shared_ptr::DefaultSharedPtr>,
);
impl<'a, T: Clone> Iterator for HistoryIter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Arc::as_ref)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}
impl<T: Clone> DoubleEndedIterator for HistoryIter<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(Arc::as_ref)
    }
}
impl<T: Clone> ExactSizeIterator for HistoryIter<'_, T> {}

impl<K: Clone + Ord, V: Clone> BTreeMap<K, V> {
    pub fn range<R: std::ops::RangeBounds<K>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = (&K, &V)> {
        self.0.range(range).map(|(k, v)| (k, v.as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Debug)]
    struct Record(Arc<AtomicUsize>, usize);
    impl Clone for Record {
        fn clone(&self) -> Self {
            self.0.fetch_add(1, Ordering::Relaxed);
            Self(self.0.clone(), self.1)
        }
    }
    impl PartialEq for Record {
        fn eq(&self, other: &Self) -> bool {
            self.1 == other.1
        }
    }
    impl Eq for Record {}
    impl PartialOrd for Record {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for Record {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.1.cmp(&other.1)
        }
    }
    impl Hash for Record {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.1.hash(state);
        }
    }
    #[test]
    fn wrapper_reads_and_structural_clones_do_not_copy_elements() {
        let copies = Arc::new(AtomicUsize::new(0));
        let mut hash: HashSet<_> = (0..1000).map(|i| Record(copies.clone(), i)).collect();
        let mut ordered: BTreeSet<_> = (0..1000).map(|i| Record(copies.clone(), i)).collect();
        let history: History<_> = (0..1000).map(|i| Record(copies.clone(), i)).collect();
        // Construction's backend copies precede the wrapper operations under test.
        copies.store(0, Ordering::Relaxed);
        let hash_clone = hash.clone();
        let ordered_clone = ordered.clone();
        let history_clone = history.clone();
        let key = Record(copies.clone(), 500);
        assert!(hash.contains(&key));
        assert!(ordered.contains(&key));
        assert_eq!(hash.iter().count(), 1000);
        assert_eq!((&hash_clone).into_iter().count(), 1000);
        assert_eq!(ordered.iter().rev().count(), 1000);
        assert_eq!((&ordered_clone).into_iter().count(), 1000);
        assert_eq!(ordered.range(Record(copies.clone(), 400)..key).count(), 100);
        assert_eq!(history.iter().rev().count(), 1000);
        assert_eq!((&history_clone).into_iter().count(), 1000);
        hash.clear();
        ordered.clear();
        assert_eq!(hash_clone.len(), 1000);
        assert_eq!(ordered_clone.len(), 1000);
        assert_eq!(copies.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn consuming_wrappers_preserve_backend_copy_counts() {
        // Ownership and shared nodes affect backend copying. Compare both cases
        // directly so an adapter cannot add a clone per yielded element.
        for shared in [false, true] {
            let copies = Arc::new(AtomicUsize::new(0));
            let records = || (0..1000).map(|i| Record(copies.clone(), i));
            let hash: HashSet<_> = records().collect();
            let ordered: BTreeSet<_> = records().collect();
            let raw_hash: imbl::HashSet<Record> = records().collect();
            let raw_ordered: imbl::OrdSet<Record> = records().collect();
            let owners = shared
                .then(|| (hash.clone(), ordered.clone(), raw_hash.clone(), raw_ordered.clone()));
            copies.store(0, Ordering::Relaxed);
            assert_eq!(hash.into_iter().count(), 1000);
            assert_eq!(ordered.into_iter().rev().count(), 1000);
            let wrapped_copies = copies.swap(0, Ordering::Relaxed);
            assert_eq!(raw_hash.into_iter().count(), 1000);
            assert_eq!(raw_ordered.into_iter().rev().count(), 1000);
            assert_eq!(copies.load(Ordering::Relaxed), wrapped_copies);
            drop(owners);
        }
    }
    #[test]
    fn long_histories_and_shared_buckets_copy_only_edited_records() {
        for n in [1000, 100_000] {
            let copies = Arc::new(AtomicUsize::new(0));
            let history: History<_> = (0..n).map(|i| Record(copies.clone(), i)).collect();
            let mut histories = HashMap::new();
            histories.insert(1, history);
            #[cfg(feature = "testing")]
            crate::recovery::work::reset_projection_work();
            let mut next = histories.clone();
            #[cfg(feature = "testing")]
            assert_eq!(crate::recovery::work::projection_work().record_copies, 0);
            next.get_mut(&1).unwrap().last_mut().unwrap().1 += 1;
            assert_eq!(copies.load(Ordering::Relaxed), 1);
            // One history root and one actual record, at either retained size.
            #[cfg(feature = "testing")]
            assert_eq!(crate::recovery::work::projection_work().record_copies, 2);
            assert_eq!(histories[&1].last().unwrap().1, n - 1);
            assert_eq!(next[&1].last().unwrap().1, n);
            next.get_mut(&1).unwrap().push(Record(copies.clone(), n + 1));
            assert_eq!(histories[&1].len(), n);
            assert_eq!(copies.load(Ordering::Relaxed), 1);
            let mut buckets: HashMap<u64, BTreeSet<usize>> = HashMap::new();
            buckets.insert(1, (0..n).collect());
            let original = buckets.clone();
            buckets.get_mut(&1).unwrap().remove(&(n - 1));
            assert!(original[&1].contains(&(n - 1)));
            assert_eq!(buckets[&1].len(), n - 1);
        }
    }
}

#[cfg(test)]
mod hydration_tests {
    use super::*;
    #[test]
    fn bulk_roots_and_persistent_deltas_preserve_map_semantics_across_clones() {
        for bulk in [false, true] {
            let initial = (0..1000u32).map(|k| (k, vec![k]));
            let mut map: HashMap<_, _> = if bulk {
                initial.collect()
            } else {
                let mut m = HashMap::new();
                for (k, v) in initial {
                    m.insert(k, v);
                }
                m
            };
            let original = map.clone();
            crate::recovery::work::reset_projection_work();
            let mut expected: std::collections::BTreeMap<_, _> =
                map.iter().map(|(k, v)| (*k, v.clone())).collect();
            for n in 0..200u32 {
                let key = (n * 37) % 1100;
                match n % 4 {
                    0 => assert_eq!(map.remove(&key), expected.remove(&key)),
                    1 => assert_eq!(map.insert(key, vec![n]), expected.insert(key, vec![n])),
                    2 => {
                        if let Some(v) = map.get_mut(&key) {
                            v.push(n);
                        }
                        if let Some(v) = expected.get_mut(&key) {
                            v.push(n);
                        }
                    }
                    _ => {
                        let v = map.entry(key).or_default();
                        v.push(n);
                        expected.entry(key).or_default().push(n);
                    }
                }
                assert_eq!(map.len(), expected.len());
                assert_eq!(
                    map.iter()
                        .map(|(k, v)| (*k, v.clone()))
                        .collect::<std::collections::BTreeMap<_, _>>(),
                    expected
                );
            }
            assert!(crate::recovery::work::projection_work().record_copies <= 400);
            assert_eq!(original.len(), 1000);
            assert_eq!(original[&37], vec![37]);
            let cloned = map.clone();
            map.clear();
            assert!(map.is_empty());
            assert_eq!(cloned.len(), expected.len());
        }
    }
}
