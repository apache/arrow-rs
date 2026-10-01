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

//! [`PageStore`]: pages that the push decoder shares with its column readers.

use bytes::Bytes;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::ops::Range;
use std::sync::Mutex;

/// The pages (or full column chunks) of one row group, keyed by file offset.
///
/// The batch-granular push decoder adds and removes pages while its column
/// readers use the store. The readers read it through [`ColumnChunkData::Shared`].
/// Offsets are unique in a file, so one store holds all column chunks of a
/// row group.
///
/// With an offset index, [`SerializedPageReader`] reads one page at a time
/// at the page start, and the store has one entry per page. Without one, it
/// reads from the column chunk start, and the store has one entry per column
/// chunk.
///
/// [`ColumnChunkData::Shared`]: crate::arrow::in_memory_row_group::ColumnChunkData::Shared
/// [`SerializedPageReader`]: crate::file::serialized_reader::SerializedPageReader
#[derive(Debug, Default)]
pub(crate) struct PageStore {
    /// file offset of the first byte -> bytes
    pages: Mutex<BTreeMap<u64, Bytes>>,
}

impl PageStore {
    /// Add the bytes of `range`. If an entry starts at `range.start`, keep
    /// the longer of the two entries. Thus, the store holds a page that is
    /// pushed two times one time only, and a longer push at the same offset
    /// does not lose bytes.
    pub(crate) fn insert(&self, range: Range<u64>, data: Bytes) {
        debug_assert_eq!(range.end - range.start, data.len() as u64);
        let mut pages = self.pages.lock().unwrap();
        match pages.entry(range.start) {
            Entry::Vacant(entry) => {
                entry.insert(data);
            }
            Entry::Occupied(mut entry) => {
                if entry.get().len() < data.len() {
                    entry.insert(data);
                }
            }
        }
    }

    /// Returns `true` if one entry contains all bytes of `range`.
    pub(crate) fn contains(&self, range: &Range<u64>) -> bool {
        let pages = self.pages.lock().unwrap();
        pages
            .range(..=range.start)
            .next_back()
            .is_some_and(|(start, data)| start + data.len() as u64 >= range.end)
    }

    /// The bytes from `start` to the end of the entry that contains `start`,
    /// if any.
    pub(crate) fn get(&self, start: u64) -> Option<Bytes> {
        let pages = self.pages.lock().unwrap();
        let (entry_start, data) = pages.range(..=start).next_back()?;
        let offset = usize::try_from(start - entry_start).ok()?;
        (offset < data.len()).then(|| data.slice(offset..))
    }

    /// Remove the entry that starts at `start`, if any.
    pub(crate) fn remove(&self, start: u64) {
        self.pages.lock().unwrap().remove(&start);
    }

    /// Remove all entries.
    pub(crate) fn clear(&self) {
        self.pages.lock().unwrap().clear();
    }

    /// The total number of bytes in the store.
    pub(crate) fn buffered_bytes(&self) -> u64 {
        self.pages
            .lock()
            .unwrap()
            .values()
            .map(|data| data.len() as u64)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_contains_get_remove() {
        let store = PageStore::default();
        assert!(!store.contains(&(10..20)));
        assert!(store.get(10).is_none());

        store.insert(10..20, Bytes::from_static(b"0123456789"));
        assert!(store.contains(&(10..20)));
        assert!(store.contains(&(12..15)));
        assert!(!store.contains(&(10..21)));
        assert!(!store.contains(&(5..12)));
        assert_eq!(store.get(10).unwrap(), Bytes::from_static(b"0123456789"));
        assert_eq!(store.get(17).unwrap(), Bytes::from_static(b"789"));
        assert!(store.get(20).is_none());
        assert!(store.get(9).is_none());

        store.insert(30..32, Bytes::from_static(b"xy"));
        assert_eq!(store.buffered_bytes(), 12);
        store.remove(10);
        assert!(store.get(12).is_none());
        assert_eq!(store.buffered_bytes(), 2);
        store.clear();
        assert_eq!(store.buffered_bytes(), 0);
    }

    #[test]
    fn test_insert_at_same_offset_keeps_longer_entry() {
        let store = PageStore::default();
        store.insert(10..20, Bytes::from_static(b"0123456789"));

        // Same length: keep the first entry.
        store.insert(10..20, Bytes::from_static(b"abcdefghij"));
        assert_eq!(store.buffered_bytes(), 10);
        assert_eq!(store.get(10).unwrap(), Bytes::from_static(b"0123456789"));

        // Shorter: keep the first entry.
        store.insert(10..15, Bytes::from_static(b"ABCDE"));
        assert_eq!(store.buffered_bytes(), 10);
        assert!(store.contains(&(10..20)));

        // Longer: replace the entry, so that no bytes are lost.
        store.insert(10..25, Bytes::from_static(b"0123456789KLMNO"));
        assert_eq!(store.buffered_bytes(), 15);
        assert!(store.contains(&(10..25)));
        assert_eq!(store.get(20).unwrap(), Bytes::from_static(b"KLMNO"));
    }
}
