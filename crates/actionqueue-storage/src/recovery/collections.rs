//! Storage-owned persistent collections. Nodes share records, and writers copy only
//! the addressed record. Materialization is explicit at persistence/API boundaries.
use std::{
    hash::Hash,
    ops::{Index, IndexMut},
    sync::Arc,
};

pub use imbl::{HashSet, OrdSet as BTreeSet};

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
map!(HashMap, HashEntry, HashMap, Eq + Hash);
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
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> + ExactSizeIterator {
        self.0.iter().map(Arc::as_ref)
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
    type IntoIter = std::iter::Map<
        imbl::vector::Iter<'a, Arc<T>, imbl::shared_ptr::DefaultSharedPtr>,
        fn(&'a Arc<T>) -> &'a T,
    >;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter().map(Arc::as_ref)
    }
}

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
    #[test]
    fn long_histories_and_shared_buckets_copy_only_edited_records() {
        for n in [1000, 100_000] {
            let copies = Arc::new(AtomicUsize::new(0));
            let history: History<_> = (0..n).map(|i| Record(copies.clone(), i)).collect();
            let mut histories = HashMap::new();
            histories.insert(1, history);
            let mut next = histories.clone();
            next.get_mut(&1).unwrap().last_mut().unwrap().1 += 1;
            assert_eq!(copies.load(Ordering::Relaxed), 1);
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
