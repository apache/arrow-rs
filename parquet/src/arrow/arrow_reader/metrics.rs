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

//! [ArrowReaderMetrics] for collecting metrics about the Arrow reader

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use crate::file::metrics::DecompressionMetrics;

/// This enum represents the state of Arrow reader metrics collection.
///
/// The inner metrics are stored in an `Arc<ArrowReaderMetricsInner>`
/// so cloning the `ArrowReaderMetrics` enum will not clone the inner metrics.
///
/// To access metrics, create an `ArrowReaderMetrics` via [`ArrowReaderMetrics::enabled()`]
/// and configure the `ArrowReaderBuilder` with a clone.
#[derive(Debug, Clone)]
pub enum ArrowReaderMetrics {
    /// Metrics are not collected (default)
    Disabled,
    /// Metrics are collected and stored in an `Arc`.
    ///
    /// Create this via [`ArrowReaderMetrics::enabled()`].
    Enabled(Arc<ArrowReaderMetricsInner>),
}

impl ArrowReaderMetrics {
    /// Creates a new instance of [`ArrowReaderMetrics::Disabled`]
    pub fn disabled() -> Self {
        Self::Disabled
    }

    /// Creates a new instance of [`ArrowReaderMetrics::Enabled`]
    pub fn enabled() -> Self {
        Self::Enabled(Arc::new(ArrowReaderMetricsInner::new()))
    }

    /// The decompression counters.
    ///
    /// Read the four cells of the page kind by phase matrix through this, for
    /// example the dictionary time paid while skipping, which is work done for
    /// rows that were never returned. The totals are also forwarded directly by
    /// [`Self::dictionary_decompress_time`] and its siblings.
    ///
    /// Also the handle to give to a page reader built by hand, via
    /// [`SerializedPageReader::with_decompression_metrics`]. Disabled when
    /// these metrics are, so nothing downstream reads a clock.
    ///
    /// [`SerializedPageReader::with_decompression_metrics`]:
    ///     crate::file::serialized_reader::SerializedPageReader::with_decompression_metrics
    pub fn decompression(&self) -> DecompressionMetrics {
        match self {
            Self::Disabled => DecompressionMetrics::disabled(),
            Self::Enabled(inner) => inner.decompression.clone(),
        }
    }

    /// Time spent decompressing dictionary pages, across every column read.
    ///
    /// A dictionary page is decompressed once per column chunk, however few of
    /// its values are later decoded, so this is the cost a selective read pays
    /// before it looks at anything. Decompression happens inside decoding and
    /// inside skipping, so this is a share of the scan's time rather than a
    /// stage to add beside it.
    ///
    /// Returns None if metrics are disabled.
    pub fn dictionary_decompress_time(&self) -> Option<Duration> {
        self.decompression_view()?.dictionary_decompress_time()
    }

    /// Time spent decompressing data pages, across every column read.
    ///
    /// Returns None if metrics are disabled.
    pub fn data_decompress_time(&self) -> Option<Duration> {
        self.decompression_view()?.data_decompress_time()
    }

    /// Uncompressed bytes produced from dictionary pages.
    ///
    /// Returns None if metrics are disabled.
    pub fn dictionary_decompressed_bytes(&self) -> Option<u64> {
        self.decompression_view()?.dictionary_decompressed_bytes()
    }

    /// Uncompressed bytes produced from data pages.
    ///
    /// Returns None if metrics are disabled.
    pub fn data_decompressed_bytes(&self) -> Option<u64> {
        self.decompression_view()?.data_decompressed_bytes()
    }

    fn decompression_view(&self) -> Option<&DecompressionMetrics> {
        match self {
            Self::Disabled => None,
            Self::Enabled(inner) => Some(&inner.decompression),
        }
    }

    /// Predicate Cache: number of records read directly from the inner reader
    ///
    /// This is the total number of records read from the inner reader (that is
    /// actually decoding). It measures the amount of work that could not be
    /// avoided with caching.
    ///
    /// It returns the number of records read across all columns, so if you read
    /// 2 columns each with 100 records, this will return 200.
    ///
    ///
    /// Returns None if metrics are disabled.
    pub fn records_read_from_inner(&self) -> Option<usize> {
        match self {
            Self::Disabled => None,
            Self::Enabled(inner) => Some(
                inner
                    .records_read_from_inner
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }

    /// Predicate Cache: number of records read from the cache
    ///
    /// This is the total number of records read from the cache actually
    /// decoding). It measures the amount of work that was avoided with caching.
    ///
    /// It returns the number of records read across all columns, so if you read
    /// 2 columns each with 100 records from the cache, this will return 200.
    ///
    /// Returns None if metrics are disabled.
    pub fn records_read_from_cache(&self) -> Option<usize> {
        match self {
            Self::Disabled => None,
            Self::Enabled(inner) => Some(
                inner
                    .records_read_from_cache
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }

    /// Increments the count of records read from the inner reader
    pub(crate) fn increment_inner_reads(&self, count: usize) {
        let Self::Enabled(inner) = self else {
            return;
        };
        inner
            .records_read_from_inner
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    }

    /// Increments the count of records read from the cache
    pub(crate) fn increment_cache_reads(&self, count: usize) {
        let Self::Enabled(inner) = self else {
            return;
        };

        inner
            .records_read_from_cache
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Holds the actual metrics for the Arrow reader.
///
/// Please see [`ArrowReaderMetrics`] for the public interface.
#[derive(Debug)]
pub struct ArrowReaderMetricsInner {
    // Metrics for Predicate Cache
    /// Total number of records read from the inner reader (uncached)
    records_read_from_inner: AtomicUsize,
    /// Total number of records read from previously cached pages
    records_read_from_cache: AtomicUsize,
    /// Decompression timings, shared with every page reader of this scan
    decompression: DecompressionMetrics,
}

impl ArrowReaderMetricsInner {
    /// Creates a new instance of `ArrowReaderMetricsInner`
    pub(crate) fn new() -> Self {
        Self {
            records_read_from_inner: AtomicUsize::new(0),
            records_read_from_cache: AtomicUsize::new(0),
            decompression: DecompressionMetrics::enabled(),
        }
    }
}
