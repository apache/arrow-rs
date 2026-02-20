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

//! Content-defined chunking (CDC) for Parquet data pages.
//!
//! CDC creates data page boundaries based on content rather than fixed sizes,
//! enabling efficient deduplication in content-addressable storage (CAS) systems.
//! See [`CdcOptions`](crate::file::properties::CdcOptions) for configuration.

mod cdc;
mod cdc_generated;

pub(crate) use cdc::ContentDefinedChunker;

/// A chunk of data with level and value offsets for record-shredded nested data.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Chunk {
    /// The start offset of this chunk inside the given levels.
    pub level_offset: usize,
    /// The start offset of this chunk inside the given values array.
    pub value_offset: usize,
    /// The number of levels in this chunk.
    pub num_levels: usize,
    /// The number of values (Arrow array elements) in this chunk.
    pub num_values: usize,
}
