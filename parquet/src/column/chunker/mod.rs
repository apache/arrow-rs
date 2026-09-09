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

use crate::column::writer::LevelValueWindow;

#[cfg(feature = "arrow")]
pub(crate) use cdc::CdcFramer;

/// A portion of one input batch belonging to a single content-defined chunk.
#[derive(Debug, Clone)]
pub(crate) struct CdcSpan {
    pub(crate) window: LevelValueWindow,
    /// Whether this span starts a chunk rather than continuing the chunk from
    /// the preceding input batch.
    pub(crate) starts_chunk: bool,
}
