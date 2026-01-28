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

// Re-export HeapSize trait from arrow-memory-size for backward compatibility
pub use arrow_memory_size::HeapSize;

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

// =============================================================================
// Parquet-specific HeapSize implementations
// =============================================================================

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
