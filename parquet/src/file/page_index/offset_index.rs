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

use crate::errors::ParquetError;
use crate::format::{OffsetIndex, PageLocation};

/// [`OffsetIndex`] information for a column chunk. Contains offsets and sizes for each page
/// in the chunk. Optionally stores fully decoded page sizes for BYTE_ARRAY columns.
#[derive(Debug, Clone, PartialEq)]
pub struct OffsetIndexMetaData {
    /// Vector of [`PageLocation`] objects, one per page in the chunk.
    pub page_locations: Vec<PageLocation>,
    /// Optional vector of unencoded page sizes, one per page in the chunk.
    /// Only defined for BYTE_ARRAY columns.
    pub unencoded_byte_array_data_bytes: Option<Vec<i64>>,
}

impl OffsetIndexMetaData {
    /// Creates a new [`OffsetIndexMetaData`] from an [`OffsetIndex`].
    pub(crate) fn try_new(index: OffsetIndex) -> Result<Self, ParquetError> {
        Ok(Self {
            page_locations: index.page_locations,
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

    // TODO: remove annotation after merge
    #[allow(dead_code)]
    pub(crate) fn to_thrift(&self) -> OffsetIndex {
        OffsetIndex::new(
            self.page_locations.clone(),
            self.unencoded_byte_array_data_bytes.clone(),
        )
    }
}
