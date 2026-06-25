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

//! Pluggable storage for completed, serialized page blobs.
//!
//! While a row group is being written the [`ArrowWriter`] must buffer every
//! column's encoded pages, because Parquet requires each column chunk to be
//! contiguous in the file while record batches arrive with all columns interleaved.
//! By default that buffer lives on the heap, so the writer's peak memory grows
//! with the row group size. A [`PageStore`] lets the buffer live somewhere else
//! — a local temp file, object storage, etc. — bounding peak write memory
//! independently of the row group size.
//!
//! [`ArrowWriter`]: crate::arrow::arrow_writer::ArrowWriter

use std::fmt::Debug;

use bytes::Bytes;

use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescriptor;

/// An opaque, store-allocated handle to a blob held by a [`PageStore`].
///
/// Handles are allocated by the store — densely and sequentially — and are only
/// meaningful to the store that produced them. The caller treats them as opaque
/// tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageKey(u64);

impl PageKey {
    /// Create a handle wrapping `raw`.
    ///
    /// A [`PageStore`] implementation calls this to mint the handle it returns
    /// from [`put`](PageStore::put). The value is opaque to the caller, so a
    /// store is free to use a dense counter, a packed locator, or anything else
    /// it can later resolve in [`take`](PageStore::take).
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw value passed to [`new`](Self::new).
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A pluggable store for completed, serialized page blobs.
///
/// The store is intentionally "dumb": it only maps an opaque [`PageKey`] to a
/// blob of bytes. It knows nothing about pages, dictionaries, ordering, or
/// offsets. The caller keeps the handles it gets back from [`put`](Self::put)
/// and decides what they mean.
///
/// Each store instance is owned by a single column writer and mutated by one
/// thread at a time (both methods take `&mut self`), so it needs no internal
/// synchronization — hence only `Send`, not `Sync`.
///
/// The default ([`InMemoryPageStore`]) keeps blobs in memory on the heap.
///
/// For an example of configuring the Parquet writer to use an alternate
/// `PageStore` see the [`ArrowWriterOptions::with_page_store_factory`] API.
///
/// [`ArrowWriterOptions::with_page_store_factory`]: crate::arrow::arrow_writer::ArrowWriterOptions::with_page_store_factory
pub trait PageStore: Send {
    /// Store `value`, returning a handle that can later be passed to
    /// [`take`](Self::take).
    fn put(&mut self, value: Bytes) -> Result<PageKey>;

    /// Take back the blob previously stored under `key`.
    ///
    /// The caller takes ownership of the returned bytes and will **not** request
    /// `key` again, so the store may release any resources backing it — eagerly
    /// here, or when the store is dropped.
    fn take(&mut self, key: PageKey) -> Result<Bytes>;

    /// The number of bytes this store currently holds **in memory** (resident
    /// on the heap), used to report the writer's memory footprint.
    ///
    /// The default is `0`, which is exactly right for a backend that moves
    /// every blob off-heap (a temp file, object storage): the bytes it has been
    /// handed no longer occupy heap. The in-memory backend overrides this to
    /// report its resident blobs. A backend that keeps a partial in-memory
    /// buffer should report that buffer's size.
    fn memory_size(&self) -> usize {
        0
    }
}

/// Context for a single [`PageStoreFactory::create`] call.
///
/// Describes the leaf column chunk the store will buffer. It is held by
/// reference for the duration of the call; a backend reads only what it needs.
/// More fields may be added in future releases without breaking existing
/// implementations — the type is constructed only by the writer, so an
/// implementer only ever receives one and calls its accessors.
pub struct PageStoreArgs<'a> {
    column_index: usize,
    column_descriptor: &'a ColumnDescriptor,
}

impl<'a> PageStoreArgs<'a> {
    // Constructed only by the Arrow writer; without that feature there is no caller.
    #[cfg(feature = "arrow")]
    pub(crate) fn new(column_index: usize, column_descriptor: &'a ColumnDescriptor) -> Self {
        Self {
            column_index,
            column_descriptor,
        }
    }

    /// Index of the leaf column within the row group.
    ///
    /// A backend may use this to e.g. name spill files or shard across a bounded
    /// pool; it carries no ordering or coordination requirement.
    pub fn column_index(&self) -> usize {
        self.column_index
    }

    /// Descriptor for the leaf column: physical/logical type, path, and max
    /// definition/repetition levels.
    ///
    /// Lets a backend tailor buffering to the column — for example spilling only
    /// large `BYTE_ARRAY` columns while keeping small fixed-width ones on the
    /// heap.
    pub fn column_descriptor(&self) -> &ColumnDescriptor {
        self.column_descriptor
    }
}

/// Creates a fresh [`PageStore`] for each column chunk.
///
/// See
/// [`ArrowWriterOptions::with_page_store_factory`](crate::arrow::arrow_writer::ArrowWriterOptions::with_page_store_factory).
pub trait PageStoreFactory: Send + Sync + Debug {
    /// Create a new, empty [`PageStore`] for the leaf column described by `args`.
    fn create(&self, args: &PageStoreArgs<'_>) -> Result<Box<dyn PageStore>>;
}

/// The default [`PageStore`], holding blobs on the heap in a `Vec<Bytes>`.
///
/// Peak memory grows with the row group size; use a spilling backend to bound
/// it.
#[derive(Debug, Default)]
pub struct InMemoryPageStore {
    blobs: Vec<Bytes>,
    /// Running total of resident blob bytes, kept in step with `put`/`take`.
    resident: usize,
}

impl PageStore for InMemoryPageStore {
    fn put(&mut self, value: Bytes) -> Result<PageKey> {
        let key = PageKey(self.blobs.len() as u64);
        self.resident += value.len();
        self.blobs.push(value);
        Ok(key)
    }

    fn take(&mut self, key: PageKey) -> Result<Bytes> {
        // Replace the slot with an empty `Bytes` so the stored blob is released
        // as soon as it is taken, keeping memory bounded while the chunk is
        // streamed into the output file.
        let blob = self
            .blobs
            .get_mut(key.0 as usize)
            .map(std::mem::take)
            .ok_or_else(|| ParquetError::General(format!("invalid page key {}", key.0)))?;
        self.resident -= blob.len();
        Ok(blob)
    }

    fn memory_size(&self) -> usize {
        self.resident
    }
}

/// Factory for [`InMemoryPageStore`] — the default used by
/// [`ArrowWriter`](crate::arrow::arrow_writer::ArrowWriter).
#[derive(Debug, Default)]
pub struct InMemoryPageStoreFactory;

impl PageStoreFactory for InMemoryPageStoreFactory {
    fn create(&self, _args: &PageStoreArgs<'_>) -> Result<Box<dyn PageStore>> {
        Ok(Box::new(InMemoryPageStore::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_round_trips_blobs_in_handle_order() {
        let mut store = InMemoryPageStore::default();
        let k0 = store.put(Bytes::from_static(b"hello")).unwrap();
        let k1 = store.put(Bytes::from_static(b"world")).unwrap();
        assert_ne!(k0, k1);
        assert_eq!(&store.take(k0).unwrap()[..], b"hello");
        assert_eq!(&store.take(k1).unwrap()[..], b"world");
    }

    #[test]
    fn in_memory_take_releases_the_slot() {
        let mut store = InMemoryPageStore::default();
        let k = store.put(Bytes::from_static(b"abc")).unwrap();
        assert_eq!(&store.take(k).unwrap()[..], b"abc");
        // A second take yields the emptied placeholder rather than the blob,
        // confirming the bytes were released on the first take.
        assert!(store.take(k).unwrap().is_empty());
    }

    #[test]
    fn in_memory_invalid_key_errors() {
        let mut store = InMemoryPageStore::default();
        assert!(store.take(PageKey(99)).is_err());
    }

    #[test]
    fn in_memory_reports_resident_bytes() {
        let mut store = InMemoryPageStore::default();
        assert_eq!(store.memory_size(), 0);
        let k0 = store.put(Bytes::from_static(b"hello")).unwrap();
        let k1 = store.put(Bytes::from_static(b"!")).unwrap();
        assert_eq!(store.memory_size(), 6);
        store.take(k0).unwrap();
        assert_eq!(store.memory_size(), 1);
        store.take(k1).unwrap();
        assert_eq!(store.memory_size(), 0);
    }

    #[test]
    fn default_store_memory_size_is_zero() {
        // A spilling backend that does not override `memory_size` reports 0,
        // reflecting that its blobs no longer occupy the heap.
        struct OffHeap;
        impl PageStore for OffHeap {
            fn put(&mut self, _value: Bytes) -> Result<PageKey> {
                Ok(PageKey::new(0))
            }
            fn take(&mut self, _key: PageKey) -> Result<Bytes> {
                Ok(Bytes::new())
            }
        }
        assert_eq!(OffHeap.memory_size(), 0);
    }
}
