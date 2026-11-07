// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
pub(crate) struct CdcChunk {
    /// The start offset of this chunk inside the given levels.
    pub level_offset: usize,
    /// The number of levels in this chunk.
    pub num_levels: usize,
    /// The start index into `non_null_indices` for this chunk.
    pub value_offset: usize,
    /// The number of `non_null_indices` entries in this chunk.
    pub num_values: usize,
}
