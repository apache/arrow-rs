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

//! [`OffsetIndexMetaData`] structure holding decoded [`OffsetIndex`] information
//!
//! [`OffsetIndex`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md

use crate::parquet_thrift::{FieldType, ThriftCompactInputProtocol};
use crate::{
    errors::{ParquetError, Result},
    thrift_struct,
};

thrift_struct!(
/// Page location information for [`OffsetIndexMetaData`]
pub struct PageLocation {
  /// Offset of the page in the file
  1: required i64 offset
  /// Size of the page, including header. Sum of compressed_page_size and header
  2: required i32 compressed_page_size
  /// Index within the RowGroup of the first row of the page. When an
  /// OffsetIndex is present, pages must begin on row boundaries
  /// (repetition_level = 0).
  3: required i64 first_row_index
}
);

impl From<&crate::format::PageLocation> for PageLocation {
    fn from(value: &crate::format::PageLocation) -> Self {
        Self {
            offset: value.offset,
            compressed_page_size: value.compressed_page_size,
            first_row_index: value.first_row_index,
        }
    }
}

impl From<&PageLocation> for crate::format::PageLocation {
    fn from(value: &PageLocation) -> Self {
        Self {
            offset: value.offset,
            compressed_page_size: value.compressed_page_size,
            first_row_index: value.first_row_index,
        }
    }
}

thrift_struct!(
/// [`OffsetIndex`] information for a column chunk. Contains offsets and sizes for each page
/// in the chunk. Optionally stores fully decoded page sizes for BYTE_ARRAY columns.
///
/// [`OffsetIndex`]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
pub struct OffsetIndexMetaData {
  /// Vector of [`PageLocation`] objects, one per page in the chunk.
  1: required list<PageLocation> page_locations
  /// Optional vector of unencoded page sizes, one per page in the chunk.
  /// Only defined for BYTE_ARRAY columns.
  2: optional list<i64> unencoded_byte_array_data_bytes
}
);

impl OffsetIndexMetaData {
    /// Creates a new [`OffsetIndexMetaData`] from an [`OffsetIndex`].
    ///
    /// [`OffsetIndex`]: crate::format::OffsetIndex
     #[allow(dead_code)]
    pub(crate) fn try_new(index: crate::format::OffsetIndex) -> Result<Self> {
        let page_locations = index.page_locations.iter().map(|loc| loc.into()).collect();
        Ok(Self {
            page_locations,
            unencoded_byte_array_data_bytes: index.unencoded_byte_array_data_bytes,
        })
    }

    /// Vector of [`PageLocation`] objects, one per page in the chunk.
    pub fn page_locations(&self) -> &Vec<PageLocation> {
        &self.page_locations
    }

    /// Optional vector of unencoded page sizes, one per page in the chunk. Only defined
    /// for BYTE_ARRAY columns.
    pub fn unencoded_byte_array_data_bytes(&self) -> Option<&Vec<i64>> {
        self.unencoded_byte_array_data_bytes.as_ref()
    }

    pub(crate) fn to_thrift(&self) -> crate::format::OffsetIndex {
        let page_locations = self.page_locations.iter().map(|loc| loc.into()).collect();
        crate::format::OffsetIndex::new(
            page_locations,
            self.unencoded_byte_array_data_bytes.clone(),
        )
    }
}
