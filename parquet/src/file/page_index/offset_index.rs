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

//! [`ParquetOffsetIndex`] structure holding decoded [`OffsetIndex`] information

use crate::errors::ParquetError;
use crate::format::{OffsetIndex, PageLocation};

#[derive(Debug, Clone, PartialEq)]
pub struct ParquetOffsetIndex {
    pub page_locations: Vec<PageLocation>,
    pub unencoded_byte_array_data_bytes: Option<Vec<i64>>,
}

impl ParquetOffsetIndex {
    pub(crate) fn try_new(index: OffsetIndex) -> Result<Self, ParquetError> {
        Ok(Self {
            page_locations: index.page_locations,
            unencoded_byte_array_data_bytes: index.unencoded_byte_array_data_bytes,
        })
    }

    pub fn page_locations(&self) -> &Vec<PageLocation> {
        &self.page_locations
    }

    pub fn unencoded_byte_array_data_bytes(&self) -> Option<&Vec<i64>> {
        self.unencoded_byte_array_data_bytes.as_ref()
    }
}
