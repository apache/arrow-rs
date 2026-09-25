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

//! [`PageStore`]: the pages of one row group that a live reader may read,
//! shared between the push decoder and that reader.
//!
//! A [`ColumnChunkData::Dense`] or [`ColumnChunkData::Sparse`] chunk is
//! immutable, so a reader over it can only be built once every byte it reads
//! is present. A [`ColumnChunkData::Shared`] chunk reads from a `PageStore`
//! instead. The decoder adds pages to the store before a batch needs them and
//! removes them after the reader has passed them, so one reader, with its
//! decoded dictionaries, can decode a whole row group while only a few pages
//! of it are resident.
//!
//! # Lookup
//!
//! With an offset index,
//! [`SerializedPageReader`](crate::file::serialized_reader::SerializedPageReader)
//! reads one page at a time, at the exact page start, and skips pages it does
//! not need without reading them. Without an offset index it reads the column
//! chunk from its start, one page header at a time. [`PageStore::get`]
//! supports both: it returns the resident bytes that contain `start`, from
//! `start` to the end of the entry.
//!
//! [`ColumnChunkData::Dense`]: crate::arrow::in_memory_row_group::ColumnChunkData::Dense
//! [`ColumnChunkData::Sparse`]: crate::arrow::in_memory_row_group::ColumnChunkData::Sparse
//! [`ColumnChunkData::Shared`]: crate::arrow::in_memory_row_group::ColumnChunkData::Shared

use bytes::Bytes;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Mutex;

/// Resident pages (or whole column chunks) of one row group, keyed by file
/// offset.
///
/// Offsets are unique in a file, so one store serves every column chunk of a
/// row group. The store is shared by `Arc` and is interior mutable, so the
/// decoder can add and remove pages while readers hold it.
#[derive(Debug, Default)]
pub(crate) struct PageStore {
    /// file offset of the first byte -> bytes
    pages: Mutex<BTreeMap<u64, Bytes>>,
}

impl PageStore {
    /// Add the bytes for `range`. If an entry already starts at
    /// `range.start`, the store keeps it, so a page pushed twice is held once.
    pub(crate) fn insert(&self, range: Range<u64>, data: Bytes) {
        debug_assert_eq!(range.end - range.start, data.len() as u64);
        self.pages
            .lock()
            .unwrap()
            .entry(range.start)
            .or_insert(data);
    }

    /// Returns `true` if one entry contains every byte of `range`.
    pub(crate) fn contains(&self, range: &Range<u64>) -> bool {
        let pages = self.pages.lock().unwrap();
        pages
            .range(..=range.start)
            .next_back()
            .is_some_and(|(start, data)| start + data.len() as u64 >= range.end)
    }

    /// The resident bytes from `start` to the end of the entry that contains
    /// `start`, if any.
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

    /// Remove every entry.
    pub(crate) fn clear(&self) {
        self.pages.lock().unwrap().clear();
    }

    /// Total resident bytes.
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
