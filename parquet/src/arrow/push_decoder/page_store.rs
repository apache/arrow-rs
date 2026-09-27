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

//! [`PageStore`]: the pages of one row group that the push decoder and its
//! column readers share.
//!
//! The decoder adds and removes pages while the readers use the store. The
//! readers read it through [`ColumnChunkData::Shared`]. See *Page flow* in
//! the `reader_builder::incremental` module.
//!
//! # Lookup
//!
//! [`PageStore::get`] returns the bytes from `start` to the end of the entry
//! that contains `start`. Thus, it supports the two read patterns of
//! [`SerializedPageReader`]:
//!
//! | Offset index | [`SerializedPageReader`] reads | Entries in the store |
//! |---|---|---|
//! | yes | one page at a time, at the page start. It skips the other pages without a read. | one per page |
//! | no | from the column chunk start, one page header at a time | one per column chunk |
//!
//! [`ColumnChunkData::Shared`]: crate::arrow::in_memory_row_group::ColumnChunkData::Shared
//! [`SerializedPageReader`]: crate::file::serialized_reader::SerializedPageReader

use bytes::Bytes;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Mutex;

/// The pages (or full column chunks) of one row group, keyed by file offset.
/// See the module documentation.
///
/// Offsets are unique in a file. Thus, one store holds all column chunks of a
/// row group.
#[derive(Debug, Default)]
pub(crate) struct PageStore {
    /// file offset of the first byte -> bytes
    pages: Mutex<BTreeMap<u64, Bytes>>,
}

impl PageStore {
    /// Add the bytes of `range`. If an entry starts at `range.start`, keep
    /// that entry. Thus, the store holds a page that is pushed two times one
    /// time only.
    pub(crate) fn insert(&self, range: Range<u64>, data: Bytes) {
        debug_assert_eq!(range.end - range.start, data.len() as u64);
        self.pages
            .lock()
            .unwrap()
            .entry(range.start)
            .or_insert(data);
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
    fn insert_contains_get_remove() {
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

        // A second insert at the same offset keeps the first entry.
        store.insert(10..20, Bytes::from_static(b"abcdefghij"));
        assert_eq!(store.buffered_bytes(), 10);
        assert_eq!(store.get(10).unwrap(), Bytes::from_static(b"0123456789"));

        store.insert(30..32, Bytes::from_static(b"xy"));
        assert_eq!(store.buffered_bytes(), 12);
        store.remove(10);
        assert!(store.get(12).is_none());
        assert_eq!(store.buffered_bytes(), 2);
        store.clear();
        assert_eq!(store.buffered_bytes(), 0);
    }
}
