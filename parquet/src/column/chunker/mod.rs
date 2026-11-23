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
