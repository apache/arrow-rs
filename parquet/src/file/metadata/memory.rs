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

//! Memory calculations for [`ParquetMetadata::memory_size`]
//!
//! [`ParquetMetadata::memory_size`]: crate::file::metadata::ParquetMetaData::memory_size
use crate::basic::{BoundaryOrder, ColumnOrder, Compression, Encoding, PageType};
use crate::data_type::private::ParquetValueType;
use crate::file::metadata::{
    ColumnChunkMetaData, FileMetaData, KeyValue, PageEncodingStats, ParquetPageEncodingStats,
    RowGroupMetaData, SortingColumn,
};
use crate::file::page_index::column_index::{
    ByteArrayColumnIndex, ColumnIndex, ColumnIndexMetaData, PrimitiveColumnIndex,
};
use crate::file::page_index::offset_index::{OffsetIndexMetaData, PageLocation};
use crate::file::statistics::{Statistics, ValueStatistics};
use std::collections::HashMap;
use std::sync::Arc;

/// Trait for calculating the size of various containers
pub trait HeapSize {
    /// Return the size of any bytes allocated on the heap by this object,
    /// including heap memory in those structures
    ///
    /// Note that the size of the type itself is not included in the result --
    /// instead, that size is added by the caller (e.g. container).
    fn heap_size(&self) -> usize;
}

impl<T: HeapSize> HeapSize for Vec<T> {
    fn heap_size(&self) -> usize {
        let item_size = std::mem::size_of::<T>();
        // account for the contents of the Vec
        (self.capacity() * item_size) +
        // add any heap allocations by contents
        self.iter().map(|t| t.heap_size()).sum::<usize>()
    }
}

impl<K: HeapSize, V: HeapSize> HeapSize for HashMap<K, V> {
    fn heap_size(&self) -> usize {
        let capacity = self.capacity();
        if capacity == 0 {
            return 0;
        }

        // HashMap doesn't provide a way to get its heap size, so this is an approximation based on
        // the behavior of hashbrown::HashMap as at version 0.16.0, and may become inaccurate
        // if the implementation changes.
        let key_val_size = std::mem::size_of::<(K, V)>();
        // Overhead for the control tags group, which may be smaller depending on architecture
        let group_size = 16;
        // 1 byte of metadata stored per bucket.
        let metadata_size = 1;

        // Compute the number of buckets for the capacity. Based on hashbrown's capacity_to_buckets
        let buckets = if capacity < 15 {
            let min_cap = match key_val_size {
                0..=1 => 14,
                2..=3 => 7,
                _ => 3,
            };
            let cap = min_cap.max(capacity);
            if cap < 4 {
                4
            } else if cap < 8 {
                8
            } else {
                16
            }
        } else {
            (capacity.saturating_mul(8) / 7).next_power_of_two()
        };

        group_size
            + (buckets * (key_val_size + metadata_size))
            + self.keys().map(|k| k.heap_size()).sum::<usize>()
            + self.values().map(|v| v.heap_size()).sum::<usize>()
    }
}

impl<T: HeapSize> HeapSize for Arc<T> {
    fn heap_size(&self) -> usize {
        // Arc stores weak and strong counts on the heap alongside an instance of T
        2 * std::mem::size_of::<usize>() + std::mem::size_of::<T>() + self.as_ref().heap_size()
    }
}

impl HeapSize for Arc<dyn HeapSize> {
    fn heap_size(&self) -> usize {
        2 * std::mem::size_of::<usize>()
            + std::mem::size_of_val(self.as_ref())
            + self.as_ref().heap_size()
    }
}

impl<T: HeapSize> HeapSize for Box<T> {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<T>() + self.as_ref().heap_size()
    }
}

impl<T: HeapSize> HeapSize for Option<T> {
    fn heap_size(&self) -> usize {
        self.as_ref().map(|inner| inner.heap_size()).unwrap_or(0)
    }
}

impl HeapSize for String {
    fn heap_size(&self) -> usize {
        self.capacity()
    }
}

impl HeapSize for FileMetaData {
    fn heap_size(&self) -> usize {
        #[cfg(feature = "encryption")]
        let encryption_heap_size =
            self.encryption_algorithm.heap_size() + self.footer_signing_key_metadata.heap_size();
        #[cfg(not(feature = "encryption"))]
        let encryption_heap_size = 0;

        self.created_by.heap_size()
            + self.key_value_metadata.heap_size()
            + self.schema_descr.heap_size()
            + self.column_orders.heap_size()
            + encryption_heap_size
    }
}

impl HeapSize for KeyValue {
    fn heap_size(&self) -> usize {
        self.key.heap_size() + self.value.heap_size()
    }
}

impl HeapSize for RowGroupMetaData {
    fn heap_size(&self) -> usize {
        // don't count schema_descr here because it is already
        // counted in FileMetaData
        self.columns.heap_size() + self.sorting_columns.heap_size()
    }
}

impl HeapSize for ColumnChunkMetaData {
    fn heap_size(&self) -> usize {
        #[cfg(feature = "encryption")]
        let encryption_heap_size =
            self.column_crypto_metadata.heap_size() + self.encrypted_column_metadata.heap_size();
        #[cfg(not(feature = "encryption"))]
        let encryption_heap_size = 0;

        // don't count column_descr here because it is already counted in
        // FileMetaData
        self.encodings.heap_size()
            + self.file_path.heap_size()
            + self.compression.heap_size()
            + self.statistics.heap_size()
            + self.encoding_stats.heap_size()
            + self.unencoded_byte_array_data_bytes.heap_size()
            + self.repetition_level_histogram.heap_size()
            + self.definition_level_histogram.heap_size()
            + self.geo_statistics.heap_size()
            + encryption_heap_size
    }
}

impl HeapSize for Encoding {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl HeapSize for ParquetPageEncodingStats {
    fn heap_size(&self) -> usize {
        match self {
            Self::Full(v) => v.heap_size(),
            Self::Mask(_) => 0,
        }
    }
}

impl HeapSize for PageEncodingStats {
    fn heap_size(&self) -> usize {
        self.page_type.heap_size() + self.encoding.heap_size()
    }
}

impl HeapSize for SortingColumn {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}
impl HeapSize for Compression {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl HeapSize for PageType {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl HeapSize for Statistics {
    fn heap_size(&self) -> usize {
        match self {
            Statistics::Boolean(value_statistics) => value_statistics.heap_size(),
            Statistics::Int32(value_statistics) => value_statistics.heap_size(),
            Statistics::Int64(value_statistics) => value_statistics.heap_size(),
            Statistics::Int96(value_statistics) => value_statistics.heap_size(),
            Statistics::Float(value_statistics) => value_statistics.heap_size(),
            Statistics::Double(value_statistics) => value_statistics.heap_size(),
            Statistics::ByteArray(value_statistics) => value_statistics.heap_size(),
            Statistics::FixedLenByteArray(value_statistics) => value_statistics.heap_size(),
        }
    }
}

impl HeapSize for OffsetIndexMetaData {
    fn heap_size(&self) -> usize {
        self.page_locations.heap_size() + self.unencoded_byte_array_data_bytes.heap_size()
    }
}

impl HeapSize for ColumnIndexMetaData {
    fn heap_size(&self) -> usize {
        match self {
            Self::NONE => 0,
            Self::BOOLEAN(native_index) => native_index.heap_size(),
            Self::INT32(native_index) => native_index.heap_size(),
            Self::INT64(native_index) => native_index.heap_size(),
            Self::INT96(native_index) => native_index.heap_size(),
            Self::FLOAT(native_index) => native_index.heap_size(),
            Self::DOUBLE(native_index) => native_index.heap_size(),
            Self::BYTE_ARRAY(native_index) => native_index.heap_size(),
            Self::FIXED_LEN_BYTE_ARRAY(native_index) => native_index.heap_size(),
        }
    }
}

impl HeapSize for ColumnIndex {
    fn heap_size(&self) -> usize {
        self.null_pages.heap_size()
            + self.boundary_order.heap_size()
            + self.null_counts.heap_size()
            + self.definition_level_histograms.heap_size()
            + self.repetition_level_histograms.heap_size()
    }
}

impl<T: ParquetValueType> HeapSize for PrimitiveColumnIndex<T> {
    fn heap_size(&self) -> usize {
        self.column_index.heap_size() + self.min_values.heap_size() + self.max_values.heap_size()
    }
}

impl HeapSize for ByteArrayColumnIndex {
    fn heap_size(&self) -> usize {
        self.column_index.heap_size()
            + self.min_bytes.heap_size()
            + self.min_offsets.heap_size()
            + self.max_bytes.heap_size()
            + self.max_offsets.heap_size()
    }
}

impl<T: ParquetValueType> HeapSize for ValueStatistics<T> {
    fn heap_size(&self) -> usize {
        self.min_opt().map(T::heap_size).unwrap_or(0)
            + self.max_opt().map(T::heap_size).unwrap_or(0)
    }
}
impl HeapSize for bool {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}
impl HeapSize for u8 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}
impl HeapSize for i32 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}
impl HeapSize for i64 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl HeapSize for f32 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}
impl HeapSize for f64 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl HeapSize for usize {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl HeapSize for BoundaryOrder {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl HeapSize for PageLocation {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl HeapSize for ColumnOrder {
    fn heap_size(&self) -> usize {
        0 // no heap allocations in ColumnOrder
    }
}
