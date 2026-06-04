// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{BTreeMap, HashMap, btree_map};
use std::fmt;
use std::ops::Index;
use std::sync::Arc;

/// A cheaply clonable map of key-value metadata, used by
/// [`Field`](crate::Field) and [`Schema`](crate::Schema).
///
/// Cloning a `Metadata` is always cheap, as the underlying map is
/// reference-counted. Mutating a shared `Metadata` (e.g. via
/// [`Metadata::insert`]) will clone the underlying map if (and only if)
/// it is shared (copy-on-write).
///
/// The entries are stored in a [`BTreeMap`], so iteration order is
/// deterministic (sorted by key).
///
/// # Example
/// ```
/// # use arrow_schema::Metadata;
/// let mut metadata = Metadata::new();
/// metadata.insert("key", "value");
///
/// let clone = metadata.clone(); // cheap
/// assert_eq!(clone.get("key"), Some(&"value".to_string()));
///
/// // Mutating one does not affect the other:
/// metadata.insert("key2", "value2");
/// assert_eq!(metadata.len(), 2);
/// assert_eq!(clone.len(), 1);
/// ```
#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Metadata(
    // Invariant: the inner map is never empty (`None` encodes the empty map).
    // This ensures the derived implementations of `PartialEq`, `Ord`, `Hash`, …
    // treat an empty `Metadata` consistently, and the empty case never allocates.
    Option<Arc<BTreeMap<String, String>>>,
);

impl Metadata {
    /// Creates an empty [`Metadata`].
    ///
    /// This does not allocate.
    pub const fn new() -> Self {
        Self(None)
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.0.as_ref().map_or(0, |map| map.len())
    }

    /// Returns `true` if there are no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    /// Returns a reference to the value corresponding to the key.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.0.as_ref()?.get(key)
    }

    /// Returns `true` if the map contains a value for the specified key.
    pub fn contains_key(&self, key: &str) -> bool {
        self.0.as_ref().is_some_and(|map| map.contains_key(key))
    }

    /// Inserts a key-value pair, returning the old value of the key, if any.
    ///
    /// Clones the underlying map if (and only if) it is shared.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) -> Option<String> {
        Arc::make_mut(self.0.get_or_insert_default()).insert(key.into(), value.into())
    }

    /// Removes a key from the map, returning the value at the key
    /// if the key was previously in the map.
    ///
    /// Clones the underlying map if (and only if) it is shared and contains the key.
    pub fn remove(&mut self, key: &str) -> Option<String> {
        let map = self.0.as_mut()?;
        if !map.contains_key(key) {
            return None;
        }
        let removed = Arc::make_mut(map).remove(key);
        if map.is_empty() {
            self.0 = None;
        }
        removed
    }

    /// Removes all entries.
    pub fn clear(&mut self) {
        self.0 = None;
    }

    /// Returns an iterator over the entries, sorted by key.
    pub fn iter(&self) -> MetadataIter<'_> {
        self.0
            .as_deref()
            .map(|map| map.iter())
            .into_iter()
            .flatten()
    }

    /// Returns an iterator over the keys, in sorted order.
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.iter().map(|(key, _)| key)
    }

    /// Returns an iterator over the values, sorted by key.
    pub fn values(&self) -> impl Iterator<Item = &String> {
        self.iter().map(|(_, value)| value)
    }
}

/// Iterator over the entries of a [`Metadata`], sorted by key.
pub type MetadataIter<'a> =
    std::iter::Flatten<std::option::IntoIter<btree_map::Iter<'a, String, String>>>;

impl fmt::Debug for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl Index<&str> for Metadata {
    type Output = String;

    fn index(&self, key: &str) -> &String {
        self.get(key)
            .unwrap_or_else(|| panic!("no entry found for key {key:?}"))
    }
}

impl From<BTreeMap<String, String>> for Metadata {
    fn from(map: BTreeMap<String, String>) -> Self {
        if map.is_empty() {
            Self(None)
        } else {
            Self(Some(Arc::new(map)))
        }
    }
}

impl From<HashMap<String, String>> for Metadata {
    fn from(map: HashMap<String, String>) -> Self {
        map.into_iter().collect::<BTreeMap<_, _>>().into()
    }
}

impl From<Arc<BTreeMap<String, String>>> for Metadata {
    fn from(map: Arc<BTreeMap<String, String>>) -> Self {
        if map.is_empty() {
            Self(None)
        } else {
            Self(Some(map))
        }
    }
}

impl<K: Into<String>, V: Into<String>, const N: usize> From<[(K, V); N]> for Metadata {
    fn from(entries: [(K, V); N]) -> Self {
        entries.into_iter().collect()
    }
}

impl From<Metadata> for BTreeMap<String, String> {
    fn from(metadata: Metadata) -> Self {
        match metadata.0 {
            None => BTreeMap::new(),
            Some(map) => Arc::try_unwrap(map).unwrap_or_else(|map| (*map).clone()),
        }
    }
}

impl From<Metadata> for HashMap<String, String> {
    fn from(metadata: Metadata) -> Self {
        metadata.into_iter().collect()
    }
}

impl From<&Metadata> for HashMap<String, String> {
    fn from(metadata: &Metadata) -> Self {
        metadata
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl<K: Into<String>, V: Into<String>> FromIterator<(K, V)> for Metadata {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        iter.into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<BTreeMap<_, _>>()
            .into()
    }
}

impl<K: Into<String>, V: Into<String>> Extend<(K, V)> for Metadata {
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        let mut iter = iter
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .peekable();
        if iter.peek().is_none() {
            return; // Avoid cloning a shared map for an empty iterator
        }
        Arc::make_mut(self.0.get_or_insert_default()).extend(iter);
    }
}

impl<'a> IntoIterator for &'a Metadata {
    type Item = (&'a String, &'a String);
    type IntoIter = MetadataIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for Metadata {
    type Item = (String, String);
    type IntoIter = btree_map::IntoIter<String, String>;

    fn into_iter(self) -> Self::IntoIter {
        BTreeMap::from(self).into_iter()
    }
}

impl PartialEq<HashMap<String, String>> for Metadata {
    fn eq(&self, other: &HashMap<String, String>) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .all(|(k, v)| other.get(k).is_some_and(|other_v| v == other_v))
    }
}

impl PartialEq<BTreeMap<String, String>> for Metadata {
    fn eq(&self, other: &BTreeMap<String, String>) -> bool {
        self.iter().eq(other.iter())
    }
}

#[cfg(feature = "serde")]
mod serde_impl {
    use super::Metadata;
    use std::collections::BTreeMap;

    impl serde_core::Serialize for Metadata {
        fn serialize<S: serde_core::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            use serde_core::ser::SerializeMap as _;
            let mut map = serializer.serialize_map(Some(self.len()))?;
            for (key, value) in self {
                map.serialize_entry(key, value)?;
            }
            map.end()
        }
    }

    impl<'de> serde_core::Deserialize<'de> for Metadata {
        fn deserialize<D: serde_core::Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Self, D::Error> {
            Ok(BTreeMap::<String, String>::deserialize(deserializer)?.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let metadata = Metadata::new();
        assert!(metadata.is_empty());
        assert_eq!(metadata.len(), 0);
        assert_eq!(metadata.get("key"), None);
        assert!(!metadata.contains_key("key"));
        assert_eq!(metadata.iter().count(), 0);
        assert_eq!(metadata, Metadata::default());

        // Empty maps don't allocate:
        assert!(Metadata::from(HashMap::new()).0.is_none());
        assert!(Metadata::from(BTreeMap::new()).0.is_none());
        assert!(
            std::iter::empty::<(String, String)>()
                .collect::<Metadata>()
                .0
                .is_none()
        );
    }

    #[test]
    fn test_insert_get_remove() {
        let mut metadata = Metadata::new();
        assert_eq!(metadata.insert("a", "1"), None);
        assert_eq!(metadata.insert("a", "2"), Some("1".to_string()));
        assert_eq!(metadata.insert("b", "3"), None);

        assert_eq!(metadata.len(), 2);
        assert_eq!(metadata.get("a"), Some(&"2".to_string()));
        assert_eq!(metadata["b"], "3");
        assert!(metadata.contains_key("a"));
        assert!(!metadata.contains_key("c"));

        assert_eq!(metadata.remove("c"), None);
        assert_eq!(metadata.remove("a"), Some("2".to_string()));
        assert_eq!(metadata.remove("b"), Some("3".to_string()));

        // Invariant: removing the last entry restores the unallocated state
        assert!(metadata.is_empty());
        assert!(metadata.0.is_none());
    }

    #[test]
    fn test_clear() {
        let mut metadata = Metadata::from([("a", "1")]);
        metadata.clear();
        assert!(metadata.is_empty());
        assert!(metadata.0.is_none());
    }

    #[test]
    #[should_panic(expected = "no entry found for key \"missing\"")]
    fn test_index_panics() {
        let metadata = Metadata::from([("a", "1")]);
        let _ = &metadata["missing"];
    }

    #[test]
    fn test_copy_on_write() {
        let mut metadata = Metadata::from([("a", "1")]);
        let clone = metadata.clone();

        // Cloning is shallow:
        let arc = metadata.0.as_ref().expect("non-empty");
        assert!(Arc::ptr_eq(arc, clone.0.as_ref().expect("non-empty")));

        // Mutation clones the shared map, leaving the clone untouched:
        metadata.insert("b", "2");
        assert_eq!(metadata.len(), 2);
        assert_eq!(clone.len(), 1);

        // Mutating an unshared map does not clone it:
        let arc = metadata.0.as_ref().expect("non-empty").clone();
        metadata.insert("c", "3");
        assert!(
            Arc::ptr_eq(&arc, metadata.0.as_ref().expect("non-empty"))
                || Arc::strong_count(&arc) == 1
        );

        // Removing a missing key from a shared map does not clone it:
        let clone = metadata.clone();
        let arc = metadata.0.as_ref().expect("non-empty");
        assert!(Arc::ptr_eq(arc, clone.0.as_ref().expect("non-empty")));
        let mut metadata2 = metadata.clone();
        assert_eq!(metadata2.remove("missing"), None);
        assert!(Arc::ptr_eq(
            metadata.0.as_ref().expect("non-empty"),
            metadata2.0.as_ref().expect("non-empty")
        ));
    }

    #[test]
    fn test_deterministic_iteration_order() {
        let metadata: Metadata = [("b", "2"), ("a", "1"), ("c", "3")].into_iter().collect();
        let keys: Vec<&String> = metadata.keys().collect();
        assert_eq!(keys, ["a", "b", "c"]);
        let values: Vec<&String> = metadata.values().collect();
        assert_eq!(values, ["1", "2", "3"]);
    }

    #[test]
    fn test_eq_with_std_maps() {
        let metadata = Metadata::from([("a", "1"), ("b", "2")]);

        let hash_map: HashMap<String, String> = metadata.clone().into();
        assert_eq!(metadata, hash_map);

        let btree_map: BTreeMap<String, String> = metadata.clone().into();
        assert_eq!(metadata, btree_map);

        let mut different = hash_map.clone();
        different.insert("c".to_string(), "3".to_string());
        assert_ne!(metadata, different);

        let mut different = hash_map;
        different.insert("a".to_string(), "other".to_string());
        assert_ne!(metadata, different);
    }

    #[test]
    fn test_extend() {
        let mut metadata = Metadata::new();
        let clone = metadata.clone();
        metadata.extend(std::iter::empty::<(String, String)>());
        assert!(metadata.0.is_none()); // No allocation for an empty extend

        metadata.extend([("a", "1"), ("b", "2")]);
        assert_eq!(metadata.len(), 2);
        assert!(clone.is_empty());
    }

    #[test]
    fn test_debug() {
        let metadata = Metadata::from([("b", "2"), ("a", "1")]);
        assert_eq!(format!("{metadata:?}"), r#"{"a": "1", "b": "2"}"#);
        assert_eq!(format!("{:?}", Metadata::new()), "{}");
    }

    #[test]
    #[cfg(feature = "serde")]
    fn test_serde_round_trip() {
        for metadata in [Metadata::new(), Metadata::from([("a", "1"), ("b", "2")])] {
            let serialized = postcard::to_stdvec(&metadata).expect("serialize");
            let deserialized: Metadata = postcard::from_bytes(&serialized).expect("deserialize");
            assert_eq!(metadata, deserialized);
        }
    }

    #[test]
    #[cfg(feature = "serde")]
    fn test_serde_matches_std_map_format() {
        // `Metadata` must serialize exactly like the maps it replaced
        let metadata = Metadata::from([("a", "1"), ("b", "2")]);
        let as_btree_map: BTreeMap<String, String> = metadata.clone().into();

        let serialized = postcard::to_stdvec(&metadata).expect("serialize");
        let map_serialized = postcard::to_stdvec(&as_btree_map).expect("serialize");
        assert_eq!(serialized, map_serialized);

        let deserialized: Metadata = postcard::from_bytes(&map_serialized).expect("deserialize");
        assert_eq!(metadata, deserialized);
    }
}
