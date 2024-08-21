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
use crate::basic::{ColumnOrder, Compression, Encoding, PageType};
use crate::data_type::private::ParquetValueType;
use crate::file::metadata::{ColumnChunkMetaData, FileMetaData, KeyValue, RowGroupMetaData};
use crate::file::page_encoding_stats::PageEncodingStats;
use crate::file::page_index::index::{Index, NativeIndex, PageIndex};
use crate::file::page_index::offset_index::OffsetIndexMetaData;
use crate::file::statistics::{Statistics, ValueStatistics};
use crate::format::{BoundaryOrder, PageLocation, SortingColumn};
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

impl<T: HeapSize> HeapSize for Arc<T> {
    fn heap_size(&self) -> usize {
        self.as_ref().heap_size()
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
        self.created_by.heap_size()
            + self.key_value_metadata.heap_size()
            + self.schema_descr.heap_size()
            + self.column_orders.heap_size()
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
    }
}

impl HeapSize for Encoding {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
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

impl HeapSize for Index {
    fn heap_size(&self) -> usize {
        match self {
            Index::NONE => 0,
            Index::BOOLEAN(native_index) => native_index.heap_size(),
            Index::INT32(native_index) => native_index.heap_size(),
            Index::INT64(native_index) => native_index.heap_size(),
            Index::INT96(native_index) => native_index.heap_size(),
            Index::FLOAT(native_index) => native_index.heap_size(),
            Index::DOUBLE(native_index) => native_index.heap_size(),
            Index::BYTE_ARRAY(native_index) => native_index.heap_size(),
            Index::FIXED_LEN_BYTE_ARRAY(native_index) => native_index.heap_size(),
        }
    }
}

impl<T: ParquetValueType> HeapSize for NativeIndex<T> {
    fn heap_size(&self) -> usize {
        self.indexes.heap_size() + self.boundary_order.heap_size()
    }
}

impl<T: ParquetValueType> HeapSize for PageIndex<T> {
    fn heap_size(&self) -> usize {
        self.min.heap_size() + self.max.heap_size() + self.null_count.heap_size()
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
